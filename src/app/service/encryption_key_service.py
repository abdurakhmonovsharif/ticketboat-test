import base64
import os
from datetime import datetime, timedelta
from typing import Dict, Any
from uuid import uuid4

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from fastapi import HTTPException

from app.database import get_pg_database


class EncryptionKeyService:
    """Service for managing short-lived encryption keys for credit card data"""

    def __init__(self):
        self._master_key = self._get_master_key()

    @staticmethod
    def _get_master_key() -> bytes:
        """Get the master encryption key from environment"""
        try:
            master_key_base64 = os.environ.get("CC_MASTER_ENCRYPTION_KEY")
            if not master_key_base64:
                raise ValueError("CC_MASTER_ENCRYPTION_KEY environment variable not set.")

            master_key = base64.b64decode(master_key_base64)
            if len(master_key) != 32:
                raise ValueError("Master encryption key must be 32 bytes (256 bits) after decoding.")
            return master_key
        except Exception as e:
            raise Exception(f"Failed to load master encryption key: {e}")

    async def create_encryption_key(self, user_id: str) -> Dict[str, Any]:
        """Create a short-lived encryption key for a specific request"""
        try:
            # Generate a random encryption key (32 bytes for AES-256)
            encryption_key = os.urandom(32)

            # Generate a unique key ID
            key_id = str(uuid4())

            # Encrypt the encryption key with the master key using AES-256-GCM
            iv = os.urandom(12)
            cipher = Cipher(algorithms.AES(self._master_key), modes.GCM(iv), backend=default_backend())
            encryptor = cipher.encryptor()

            # Encrypt the encryption key
            encrypted_key = encryptor.update(encryption_key) + encryptor.finalize()
            tag = encryptor.tag

            # Store the encrypted key in the database with expiration
            expires_at = datetime.utcnow() + timedelta(minutes=7)

            query = """
                INSERT INTO ams.encryption_keys (
                    key_id,
                    encrypted_key,
                    encryption_iv,
                    encryption_tag,
                    user_id,
                    created_at,
                    expires_at,
                    used
                ) VALUES (
                    :key_id,
                    :encrypted_key,
                    :encryption_iv,
                    :encryption_tag,
                    :user_id,
                    :created_at,
                    :expires_at,
                    :used
                )
            """

            values = {
                "key_id": key_id,
                "encrypted_key": encrypted_key + tag,  # Store key + tag together
                "encryption_iv": iv,
                "encryption_tag": tag,
                "user_id": user_id,
                "created_at": datetime.utcnow(),
                "expires_at": expires_at,
                "used": False
            }

            await get_pg_database().execute(query=query, values=values)

            return {
                "key_id": key_id,
                "encryption_key": base64.b64encode(encryption_key).decode('utf-8'),
                "expires_at": expires_at.isoformat()
            }

        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to create encryption key: {e}")

    async def get_encryption_key(self, key_id: str, user_id: str) -> Dict[str, Any]:
        """Retrieve and decrypt an encryption key"""
        try:
            query = """
                SELECT 
                    encrypted_key,
                    encryption_iv,
                    encryption_tag,
                    user_id,
                    expires_at,
                    used
                FROM ams.encryption_keys 
                WHERE key_id = :key_id
            """

            row = await get_pg_database().fetch_one(query=query, values={"key_id": key_id})

            if not row:
                raise HTTPException(status_code=404, detail="Encryption key not found")

            # Check if the key has expired
            if datetime.utcnow() > row["expires_at"]:
                await self._delete_expired_key(key_id)
                raise HTTPException(status_code=410, detail="Encryption key has expired")

            # Check if key has already been used
            if row["used"]:
                await self._delete_expired_key(key_id)
                raise HTTPException(status_code=410, detail="Encryption key has already been used")

            # Check if user has permission to access this key
            if row["user_id"] != user_id:
                raise HTTPException(status_code=403, detail="Access denied to this encryption key")

            # Decrypt the encryption key
            encrypted_key_with_tag = row["encrypted_key"]
            iv = row["encryption_iv"]

            # Separate ciphertext and tag
            tag_length = 16
            ciphertext = encrypted_key_with_tag[:-tag_length]
            tag = encrypted_key_with_tag[-tag_length:]

            cipher = Cipher(algorithms.AES(self._master_key), modes.GCM(iv, tag), backend=default_backend())
            decryptor = cipher.decryptor()

            encryption_key = decryptor.update(ciphertext) + decryptor.finalize()

            # Mark key as used
            await self._mark_key_as_used(key_id)

            return {
                "key_id": key_id,
                "encryption_key": base64.b64encode(encryption_key).decode('utf-8'),
                "expires_at": row["expires_at"].isoformat()  # Convert to string
            }
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to retrieve encryption key: {e}")

    @staticmethod
    async def _mark_key_as_used(key_id: str):
        """Mark an encryption key as used"""
        query = "UPDATE ams.encryption_keys SET used = true WHERE key_id = :key_id"
        await get_pg_database().execute(query=query, values={"key_id": key_id})

    @staticmethod
    async def _delete_expired_key(key_id: str):
        """Delete an expired encryption key"""
        query = "DELETE FROM ams.encryption_keys WHERE key_id = :key_id"
        await get_pg_database().execute(query=query, values={"key_id": key_id})

    @staticmethod
    async def cleanup_expired_keys():
        """Clean up expired encryption keys"""
        try:
            query = "DELETE FROM ams.encryption_keys WHERE expires_at < :now"
            await get_pg_database().execute(query=query, values={"now": datetime.utcnow()})
        except Exception as e:
            print(f"Failed to cleanup expired keys: {e}")


# Global instance
encryption_key_service = EncryptionKeyService()
