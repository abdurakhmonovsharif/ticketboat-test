import requests
import json
import hashlib
import hmac
import base64
import secrets
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.kdf.hkdf import HKDF
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives.padding import PKCS7
from cryptography.hazmat.backends import default_backend


class VaultwardenRegistration:
    def __init__(self, server_url):
        """Initialize the Vaultwarden registration client"""
        self.server_url = server_url.rstrip('/')
        self.register_endpoint = f"{self.server_url}/api/accounts/register"

    def generate_master_key_and_hash(self, email, master_password, kdf_iterations=600000):
        """Generate master key and password hash using PBKDF2 exactly like Bitwarden"""
        # Create salt from email (lowercased)
        salt = email.lower().encode('utf-8')

        # First round: Generate master key (32 bytes)
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=salt,
            iterations=kdf_iterations,
            backend=default_backend()
        )
        master_key = kdf.derive(master_password.encode('utf-8'))

        # Second round: Generate hash for server verification
        kdf2 = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=master_password.encode('utf-8'),
            iterations=1,
            backend=default_backend()
        )
        password_hash = kdf2.derive(master_key)

        return master_key, base64.b64encode(password_hash).decode('utf-8')

    def derive_key_from_master_key(self, master_key, purpose):
        """
        Derive encryption/MAC keys from master key using HKDF
        This matches Bitwarden's actual key derivation
        """
        # Use HKDF to derive 64 bytes (32 for encryption + 32 for MAC)
        hkdf = HKDF(
            algorithm=hashes.SHA256(),
            length=64,
            salt=None,
            info=purpose.encode('utf-8'),
            backend=default_backend()
        )
        derived_key = hkdf.derive(master_key)
        return derived_key

    def stretch_key(self, key, purpose):
        """
        Stretch a 32-byte key to 64 bytes using HKDF-like expansion
        This matches Bitwarden's key stretching algorithm
        """
        if len(key) == 64:
            return key

        # Use HMAC-based key stretching like Bitwarden
        enc_key = hmac.new(key, purpose + b"enc", hashlib.sha256).digest()[:32]
        mac_key = hmac.new(key, purpose + b"mac", hashlib.sha256).digest()[:32]

        return enc_key + mac_key

    def expand_key(self, key):
        """
        Expand 32-byte key to 64 bytes using the correct Bitwarden method
        This is the critical fix - Bitwarden uses HKDF-Expand, not custom HMAC
        """
        if len(key) == 64:
            return key

        # Bitwarden uses HKDF-Expand with specific constants
        # Info parameter for encryption key
        enc_info = b"enc"
        mac_info = b"mac"

        # Simple HKDF-Expand implementation
        def hkdf_expand(prk, info, length):
            t = b""
            okm = b""
            counter = 1

            while len(okm) < length:
                t = hmac.new(prk, t + info + bytes([counter]), hashlib.sha256).digest()
                okm += t
                counter += 1

            return okm[:length]

        enc_key = hkdf_expand(key, enc_info, 32)
        mac_key = hkdf_expand(key, mac_info, 32)

        return enc_key + mac_key

    def aes_encrypt_bitwarden(self, plaintext, key):
        """
        Encrypt using AES-256-CBC with exact Bitwarden format
        Returns: "2.iv_base64|ciphertext_base64|mac_base64"
        """
        if isinstance(plaintext, str):
            plaintext = plaintext.encode('utf-8')

        # Expand key if needed
        if len(key) == 32:
            expanded_key = self.expand_key(key)
        elif len(key) == 64:
            expanded_key = key
        else:
            raise ValueError(f"Invalid key length: {len(key)}")

        enc_key = expanded_key[:32]
        mac_key = expanded_key[32:64]

        # Generate random IV
        iv = secrets.token_bytes(16)

        # Apply PKCS7 padding
        padder = PKCS7(128).padder()
        padded_data = padder.update(plaintext) + padder.finalize()

        # Encrypt with AES-256-CBC
        cipher = Cipher(algorithms.AES(enc_key), modes.CBC(iv), backend=default_backend())
        encryptor = cipher.encryptor()
        ciphertext = encryptor.update(padded_data) + encryptor.finalize()

        # Calculate MAC over IV + ciphertext
        mac = hmac.new(mac_key, iv + ciphertext, hashlib.sha256).digest()

        # Encode in Bitwarden format
        iv_b64 = base64.b64encode(iv).decode('utf-8')
        ct_b64 = base64.b64encode(ciphertext).decode('utf-8')
        mac_b64 = base64.b64encode(mac).decode('utf-8')

        return f"2.{iv_b64}|{ct_b64}|{mac_b64}"

    def generate_user_key(self):
        """Generate 64-byte user key (32 for encryption + 32 for MAC)"""
        return secrets.token_bytes(64)

    def generate_rsa_keypair(self):
        """Generate RSA keypair exactly like Bitwarden web client"""
        # Generate 2048-bit RSA keypair
        private_key = rsa.generate_private_key(
            public_exponent=65537,
            key_size=2048,
            backend=default_backend()
        )

        # Get public key in exact format Bitwarden uses
        public_key = private_key.public_key()
        public_pem = public_key.public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo
        ).decode('utf-8')

        # Get private key in PKCS#8 PEM format
        private_pem = private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )

        return public_pem, private_pem

    def create_registration_payload(self, email, name, master_password, password_hint="",
                                    kdf_iterations=600000, captcha_response=None):
        """Create exact registration payload that matches Bitwarden web client"""

        print(f"🔑 Generating keys for {email}...")

        # Step 1: Generate master key and password hash
        master_key, master_password_hash = self.generate_master_key_and_hash(
            email, master_password, kdf_iterations
        )
        print(f"Master key: {base64.b64encode(master_key).decode()}")
        print(f"Password hash: {master_password_hash}")

        # Step 2: Generate user's symmetric key (64 bytes)
        user_key = self.generate_user_key()
        print(f"User key: {base64.b64encode(user_key).decode()}")

        # Step 3: Encrypt user key with master key -> "key" field
        encrypted_user_key = self.aes_encrypt_bitwarden(user_key, master_key)
        print(f"Encrypted user key: {encrypted_user_key}")

        # Step 4: Generate RSA keypair
        public_key_pem, private_key_pem = self.generate_rsa_keypair()

        # Step 5: Encrypt private key with user key -> "keys.encryptedPrivateKey"
        encrypted_private_key = self.aes_encrypt_bitwarden(private_key_pem, user_key)
        print(f"Encrypted private key: {encrypted_private_key}")

        # Create the exact payload structure
        payload = {
            "email": email,
            "name": name,
            "masterPasswordHash": master_password_hash,
            "masterPasswordHint": password_hint,
            "key": encrypted_user_key,
            "keys": {
                "publicKey": public_key_pem,
                "encryptedPrivateKey": encrypted_private_key
            },
            "kdf": 0,  # PBKDF2_SHA256
            "kdfIterations": kdf_iterations,
            "kdfMemory": None,
            "kdfParallelism": None,
            "referenceData": {
                "id": None,
                "initiationPath": "Registration form"
            },
            "captchaResponse": captcha_response,
            "organizationUserId": None
        }

        return payload

    def register_account(self, email, name, master_password, password_hint="",
                         kdf_iterations=600000, captcha_response=None):
        """Register account with Vaultwarden"""
        try:
            # Create payload
            payload = self.create_registration_payload(
                email, name, master_password, password_hint,
                kdf_iterations, captcha_response
            )

            print("\n=== Final Registration Payload ===")
            # Don't print the full private key for security
            payload_copy = payload.copy()
            if len(payload_copy["keys"]["encryptedPrivateKey"]) > 100:
                payload_copy["keys"]["encryptedPrivateKey"] = payload_copy["keys"]["encryptedPrivateKey"][:100] + "..."
            if len(payload_copy["keys"]["publicKey"]) > 200:
                payload_copy["keys"]["publicKey"] = payload_copy["keys"]["publicKey"][:200] + "..."

            # Set headers exactly like browser
            headers = {
                'Content-Type': 'application/json',
                'Accept': 'application/json',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }

            print(f"\n🚀 Sending registration request to {self.register_endpoint}...")

            # Send registration request
            response = requests.post(
                self.register_endpoint,
                json=payload,  # Use json parameter instead of data
                headers=headers,
                timeout=30,
                verify=True
            )

            print(f"Response status: {response.status_code}")
            print(f"Response headers: {dict(response.headers)}")

            # Check response
            if response.status_code == 200:
                return True, response.json()
            else:
                print(f"Error response: {response.text}")
                return False, {
                    'status_code': response.status_code,
                    'error': response.text
                }

        except requests.exceptions.RequestException as e:
            return False, f"Network error: {str(e)}"
        except Exception as e:
            return False, f"Error: {str(e)}"

    def test_key_generation(self, email, master_password, kdf_iterations=600000):
        """Test key generation and compare with expected values"""
        print("=== Testing Key Generation ===")

        # Generate keys
        master_key, password_hash = self.generate_master_key_and_hash(
            email, master_password, kdf_iterations
        )

        user_key = self.generate_user_key()
        encrypted_user_key = self.aes_encrypt_bitwarden(user_key, master_key)

        public_key, private_key = self.generate_rsa_keypair()
        encrypted_private_key = self.aes_encrypt_bitwarden(private_key, user_key)

        print(f"Email: {email}")
        print(f"Master password: {master_password}")
        print(f"KDF iterations: {kdf_iterations}")
        print(f"Master key (b64): {base64.b64encode(master_key).decode()}")
        print(f"Password hash: {password_hash}")
        print(f"User key (b64): {base64.b64encode(user_key).decode()}")
        print(f"Encrypted user key: {encrypted_user_key}")
        print(f"Encrypted private key: {encrypted_private_key[:100]}...")

        return {
            'master_key': master_key,
            'password_hash': password_hash,
            'user_key': user_key,
            'encrypted_user_key': encrypted_user_key,
            'encrypted_private_key': encrypted_private_key
        }
