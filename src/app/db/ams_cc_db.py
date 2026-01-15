import base64
import json
import os
from datetime import datetime
import traceback
from typing import Dict, Any, List, Optional
from uuid import uuid4, UUID

from asyncpg import UniqueViolationError
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from fastapi import HTTPException

from app.database import get_pg_database, get_pg_readonly_database
from app.db.ams_db import get_accounts_data_by_ids
from app.model.ams_models import (
    CreateCreditCardIssuerRequest,
    CreditCardCreateRequest, 
    EncryptedCreditCardDataWithKeyId, 
    AccountData,
    CreditCardSingleUpdateRequest
)
from app.service.encryption_key_service import encryption_key_service


def get_encryption_key_for_storage() -> bytes | None:
    try:
        # Retrieve the Base64 encoded string from the environment variable
        ENCRYPTION_KEY_BASE64 = os.environ.get("CC_ENCRYPTION_KEY_FOR_STORAGE")
        if not ENCRYPTION_KEY_BASE64:
            raise ValueError("CC_ENCRYPTION_KEY_FOR_STORAGE environment variable not set.")

        # Decode the Base64 string back to bytes
        ENCRYPTION_KEY_FOR_STORAGE = base64.b64decode(ENCRYPTION_KEY_BASE64)

        # Basic validation: ensure it's the correct length for AES-256
        if len(ENCRYPTION_KEY_FOR_STORAGE) != 32:
            raise ValueError("Encryption key must be 32 bytes (256 bits) after decoding.")
        return ENCRYPTION_KEY_FOR_STORAGE
    except Exception as e:
        raise Exception(f"ERROR: Failed to load encryption key securely: {e}")


def encrypt_card_data(data: str, encryption_key: bytes) -> tuple[bytes, bytes]:
    """Encrypts data using AES-256 GCM with a provided key."""
    if not encryption_key:
        raise ValueError("Encryption key is required.")

    # AES GCM recommends a 12-byte (96-bit) IV/nonce
    iv = os.urandom(12)

    cipher = Cipher(algorithms.AES(encryption_key), modes.GCM(iv), backend=default_backend())
    encryptor = cipher.encryptor()

    # Data must be bytes
    padded_data = data.encode('utf-8')

    # Encrypt the data
    ciphertext = encryptor.update(padded_data) + encryptor.finalize()

    # The GCM mode generates an authentication tag during finalization
    tag = encryptor.tag

    # Store IV and tag along with ciphertext
    return ciphertext + tag, iv  # Concatenate ciphertext and tag for storage


def decrypt_card_data(encrypted_data_with_tag: bytes, iv: bytes, encryption_key: bytes) -> str:
    """Decrypts data using AES-256 GCM with a provided key."""
    if not encryption_key:
        raise ValueError("Encryption key is required.")

    # Separate ciphertext and tag (default tag is 16 bytes for AES GCM)
    tag_length = 16
    ciphertext = encrypted_data_with_tag[:-tag_length]
    tag = encrypted_data_with_tag[-tag_length:]

    cipher = Cipher(algorithms.AES(encryption_key), modes.GCM(iv, tag), backend=default_backend())
    decryptor = cipher.decryptor()

    # Decrypt the data
    plaintext = decryptor.update(ciphertext) + decryptor.finalize()
    return plaintext.decode('utf-8')


def validate_credit_card_number(card_number: str) -> bool:
    """Validate credit card number"""
    # Check if it's all digits
    if not card_number.isdigit():
        return False
    # Check length (most cards are 13-19 digits)
    if len(card_number) < 13 or len(card_number) > 19:
        return False
    return True


async def create_credit_card_encrypted(encrypted_request: EncryptedCreditCardDataWithKeyId, user_id: str):
    """Create a credit card from encrypted data"""
    try:
        print("CC_ENCRYPTION_KEY_FOR_STORAGE", os.environ.get("CC_ENCRYPTION_KEY_FOR_STORAGE"))
        print("CC_MASTER_ENCRYPTION_KEY", os.environ.get("CC_MASTER_ENCRYPTION_KEY"))
        # Get the encryption key
        encryption_key_data = await encryption_key_service.get_encryption_key(
            encrypted_request.encrypted_key_id,
            user_id
        )
        encryption_key = base64.b64decode(encryption_key_data['encryption_key'])

        # Decrypt the card data
        encrypted_data = base64.b64decode(encrypted_request.encrypted_data)

        iv = encrypted_data[:12]  # First 12 bytes are IV
        ciphertext_with_tag = encrypted_data[12:]  # Rest is ciphertext + tag

        decrypted_data = decrypt_card_data(ciphertext_with_tag, iv, encryption_key)
        card_data_dict = json.loads(decrypted_data)

        # Validate the card number
        if not validate_credit_card_number(card_data_dict["card_number"]):
            raise HTTPException(status_code=400, detail="Invalid credit card number")

        # Check if card number already exists
        exists, existing_card_id = await check_card_number_exists(card_data_dict["card_number"])
        if exists:
            raise HTTPException(
                status_code=409,
                detail=f"Credit card number already exists in the system. Card ID: {existing_card_id}"
            )

        # Create CreditCardCreateRequest object
        card_data = CreditCardCreateRequest(**card_data_dict)

        # Create the credit card using the existing method
        return await create_credit_card(card_data)

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create credit card from encrypted data: {e}")


async def find_account(account_id: str):
    accounts = await get_accounts_data_by_ids([account_id])
    if not accounts:
        raise HTTPException(status_code=500, detail="The chosen account wasn't found.")
    return accounts[0]


async def check_credit_card_nickname_unique(account_id: str, nickname: str) -> bool:
    """
    Check if a credit card nickname is unique for a given account.
    
    Args:
        account_id: The AMS account ID
        nickname: The credit card nickname to check
        
    Returns:
        True if nickname is unique (or empty), False if duplicate exists
    """
    if not account_id and not nickname or not nickname.strip():
        return True
    
    try:
        query = """
            SELECT COUNT(*) as count
            FROM ams.ams_credit_card
            WHERE ams_account_id = :account_id
            AND nickname = :nickname
        """
        
        result = await get_pg_readonly_database().fetch_one(
            query=query,
            values={"account_id": account_id, "nickname": nickname.strip()}
        )
        
        return result["count"] == 0
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error checking credit card nickname uniqueness: {e}")


async def create_credit_card(card_data: CreditCardCreateRequest):
    try:
        if not card_data.ams_account_id:
            if not card_data.account_address_id:
                raise HTTPException(
                    status_code=400, 
                    detail="Account address is required"
                )
                
            account_address_id = card_data.account_address_id
            if card_data.avs_same_as_account:
                avs_address_id = card_data.account_address_id
            else:
                if not card_data.avs_address_id:
                    raise HTTPException(
                        status_code=400, 
                        detail="AVS address is required when 'Same as Account' is not selected"
                    )
                avs_address_id = card_data.avs_address_id
        else:
            account = await find_account(card_data.ams_account_id)
            
            if card_data.nickname and card_data.nickname.strip():
                is_unique = await check_credit_card_nickname_unique(
                    card_data.ams_account_id,
                    card_data.nickname
                )
                if not is_unique:
                    raise HTTPException(
                        status_code=409,
                        detail=f"Credit card nickname '{card_data.nickname}' already exists for this account. Each card must have a unique nickname."
                    )
            
            if card_data.account_address_id:
                account_address_id = card_data.account_address_id
            else:
                account_address_id = json.loads(account["address"])["id"]
            
            if card_data.avs_same_as_account:
                avs_address_id = account_address_id
            else:
                if not card_data.avs_address_id:
                    raise HTTPException(
                        status_code=400, 
                        detail="AVS address is required when 'Same as Account' is not selected"
                    )
                avs_address_id = card_data.avs_address_id

        masked_card_number = f"************{card_data.card_number[-4:]}"
        card_id = uuid4()
        encryption_key_for_storage = get_encryption_key_for_storage()
        encrypted_card_number, card_number_iv = encrypt_card_data(card_data.card_number, encryption_key_for_storage)
        encrypted_cvv, cvv_iv = encrypt_card_data(card_data.cvv, encryption_key_for_storage)

        query = """
            INSERT INTO ams.ams_credit_card (
                id,
                ams_account_id,
                card_type,
                issuer_id,
                masked_card_number,
                encrypted_card_number,
                encryption_card_number_iv,
                encrypted_cvv,
                encryption_cvv_iv,
                expiration_month,
                expiration_year,
                ams_person_id,
                account_address_id,
                avs_address_id,
                avs_same_as_account,
                tm_card,
                status,
                secondary_card,
                type,
                nickname,
                cc_created,
                cc_created_by
            )
            VALUES (
                :id,
                :ams_account_id,
                :card_type,
                :issuer_id,
                :masked_card_number,
                :encrypted_card_number,
                :encryption_card_number_iv,
                :encrypted_cvv,
                :encryption_cvv_iv,
                :expiration_month,
                :expiration_year,
                :ams_person_id,
                :account_address_id,
                :avs_address_id,
                :avs_same_as_account,
                :tm_card,
                :status,
                :secondary_card,
                :type,
                :nickname,
                :cc_created,
                :cc_created_by
            )
        """
        values = {
            "id": card_id,
            "ams_account_id": None if not card_data.ams_account_id else card_data.ams_account_id,
            "card_type": card_data.card_type,
            "issuer_id": card_data.issuer_id,
            "masked_card_number": masked_card_number,
            "encrypted_card_number": encrypted_card_number,
            "encryption_card_number_iv": card_number_iv,
            "encrypted_cvv": encrypted_cvv,
            "encryption_cvv_iv": cvv_iv,
            "expiration_month": card_data.expiration_month,
            "expiration_year": card_data.expiration_year,
            "ams_person_id": card_data.ams_person_id,
            "account_address_id": account_address_id,
            "avs_address_id": avs_address_id,
            "avs_same_as_account": card_data.avs_same_as_account,
            "tm_card": card_data.tm_card,
            "status": card_data.status,
            "secondary_card": card_data.secondary_card,
            "type": card_data.type,
            "nickname": card_data.nickname,
            "cc_created": datetime.fromisoformat(card_data.created.replace("Z", "")),
            "cc_created_by": card_data.created_by,
        }

        await get_pg_database().execute(query=query, values=values)
        return card_id
    except UniqueViolationError:
        raise HTTPException(status_code=409, detail="Credit card already exists.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred while creating the credit card: {e}")


FIELD_TO_COLUMN = {
    "company": "company",
    "application": "issuer_id",
    "tm": "tm_card",
    "status": "status",
    "nickname": "nickname",
}


async def update_credit_cards(update_data: dict[str, list[str] | str]):
    try:
        # Validate input data
        if 'card_ids' not in update_data:
            raise HTTPException(status_code=400, detail="card_ids is required")
        if 'status' not in update_data:
            raise HTTPException(status_code=400, detail="status is required")
        
        card_ids = update_data['card_ids']
        status = update_data['status']
        
        # Validate types
        if not isinstance(card_ids, list):
            raise HTTPException(status_code=400, detail="card_ids must be a list")
        if not isinstance(status, str):
            raise HTTPException(status_code=400, detail="status must be a string")
        
        # Validate that card_ids is not empty
        if len(card_ids) == 0:
            raise HTTPException(status_code=400, detail="card_ids cannot be empty")
        
        # Create a placeholder for each ID
        placeholders = ', '.join([f':id{i}' for i in range(len(card_ids))])
        
        query = f"""
        UPDATE ams.ams_credit_card
        SET
            status = :status,
            last_modified = CURRENT_TIMESTAMP
        WHERE id IN ({placeholders})
        RETURNING id;
        """

        # Create values dict with individual ID parameters
        values = {"status": status}
        for i, cid in enumerate(card_ids):
            values[f"id{i}"] = cid
        
        result = await get_pg_database().fetch_all(query=query, values=values)
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")
    

SINGLE_CARD_UPDATE_FIELD_TO_COLUMN = {
    "ams_account_id": "ams_account_id",
    "ams_person_id": "ams_person_id",
    "card_type": "card_type",
    "issuer_id": "issuer_id",
    "expiration_month": "expiration_month",
    "expiration_year": "expiration_year",
    "tm_card": "tm_card",
    "status": "status",
    "type": "type",
    "secondary_card": "secondary_card",
    "nickname": "nickname",
    "account_address_id": "account_address_id",
    "avs_address_id": "avs_address_id",
    "avs_same_as_account": "avs_same_as_account",
}


async def update_credit_card(card_id: str, update_data: CreditCardSingleUpdateRequest):
    """Update a single credit card"""
    try:
        set_clauses = []
        values = {}

        # Map the request fields to database columns
        for field, column in SINGLE_CARD_UPDATE_FIELD_TO_COLUMN.items():
            value = getattr(update_data, field, None)
            if value is not None:
                set_clauses.append(f"{column} = :{field}")
                values[field] = value
                
        if update_data.card_number:
            encryption_key_for_storage = get_encryption_key_for_storage()
            encrypted_card_number, iv = encrypt_card_data(update_data.card_number, encryption_key_for_storage)
            masked_card_number = f"************{update_data.card_number[-4:]}"
            set_clauses.append("masked_card_number = :masked_card_number")
            set_clauses.append("encrypted_card_number = :encrypted_card_number")
            set_clauses.append("encryption_card_number_iv = :encryption_card_number_iv")
            values["masked_card_number"] = masked_card_number
            values["encrypted_card_number"] = encrypted_card_number
            values["encryption_card_number_iv"] = iv

        if update_data.cvv:
            encryption_key_for_storage = get_encryption_key_for_storage()
            encrypted_cvv, cvv_iv = encrypt_card_data(update_data.cvv, encryption_key_for_storage)

            set_clauses.append("encrypted_cvv = :encrypted_cvv")
            set_clauses.append("encryption_cvv_iv = :encryption_cvv_iv")

            values["encrypted_cvv"] = encrypted_cvv
            values["encryption_cvv_iv"] = cvv_iv

        set_clauses.append("last_modified = CURRENT_TIMESTAMP")

        if len(set_clauses) == 1:
            raise HTTPException(status_code=400, detail="No fields to update.")

        set_clause = ', '.join(set_clauses)
        values["card_id"] = card_id

        query = f"""
            UPDATE ams.ams_credit_card
            SET {set_clause}
            WHERE id = :card_id
            RETURNING 
                ams_account_id,
                ams_person_id,
                card_type,
                issuer_id,
                expiration_month,
                expiration_year,
                tm_card,
                status,
                type,
                secondary_card,
                nickname,
                account_address_id,
                avs_address_id,
                avs_same_as_account,
                last_modified;
        """

        result = await get_pg_database().fetch_one(query=query, values=values)

        if not result:
            raise HTTPException(status_code=404, detail=f"Credit card with id {card_id} not found.")

        return dict(result)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred while updating the credit card: {e}")


async def update_credit_card_encrypted(
    card_id: str,
    user_id: str,
    encrypted_request: EncryptedCreditCardDataWithKeyId
):
    try:
        encryption_key_data = await encryption_key_service.get_encryption_key(
            encrypted_request.encrypted_key_id,
            user_id
        )
        encryption_key = base64.b64decode(encryption_key_data['encryption_key'])
        encrypted_data = base64.b64decode(encrypted_request.encrypted_data)

        iv = encrypted_data[:12]
        ciphertext_with_tag = encrypted_data[12:]

        decrypted_data = decrypt_card_data(ciphertext_with_tag, iv, encryption_key)
        card_data_dict = json.loads(decrypted_data)

        if not validate_credit_card_number(card_data_dict["card_number"]):
            raise HTTPException(status_code=400, detail="Invalid credit card number")

        exists, existing_card_id = await check_card_number_exists(card_data_dict["card_number"])
        if exists and existing_card_id != card_id:
            raise HTTPException(
                status_code=409,
                detail=f"Credit card number already exists in the system. Card ID: {existing_card_id}"
            )

        card_data = CreditCardSingleUpdateRequest(**card_data_dict)
        return await update_credit_card(card_id, card_data)

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to update credit card from encrypted data: {e}")


async def get_credit_cards_by_filters(
        page: int = 1,
        page_size: int = 10,
        sort_field: str = None,
        sort_order: str = 'desc',
        search_query: str = "",
        metro_area_ids: list[str] = None,
        company_ids: list[str] = None,
):
    try:
        filters = []
        values: Dict[str, Any] = {}

        # Build the metro area filter condition
        if metro_area_ids and len(metro_area_ids) > 0:
            # Create placeholders for each metro area ID
            placeholders = ",".join([f"'{id}'" for id in metro_area_ids])
            filters.append(f"ma.id IN ({placeholders})")

        if company_ids and len(company_ids) > 0:
            placeholders = ",".join([f"'{id}'" for id in company_ids])
            # Filter by either the account's company or the card's company
            filters.append(f"(a.company_id IN ({placeholders}))")

        if search_query:
            filters.append("""
                (
                    a.id::TEXT ILIKE :search OR
                    CONCAT(p.first_name, ' ', p.last_name) ILIKE :search OR
                    cc.nickname ILIKE :search OR
                    cc.masked_card_number ILIKE :search OR
                    cc.card_type ILIKE :search OR
                    ic.label ILIKE :search OR
                    (LPAD(CAST(cc.expiration_month AS TEXT), 2, '0') || '/' || RIGHT(CAST(cc.expiration_year AS TEXT), 2)) ILIKE :search OR
                    account_company.name ILIKE :search OR
                    a.nickname ILIKE :search OR
                    addr.street_one ILIKE :search OR
                    addr.city ILIKE :search OR
                    addr.postal_code ILIKE :search OR
                    s.name ILIKE :search OR
                    s.abbreviation ILIKE :search
                )
            """)
            values["search"] = f"%{search_query}%"

        where_clause = "WHERE " + " AND ".join(filters) if filters else ""

        offset = (page - 1) * page_size
        values["limit"] = page_size
        values["offset"] = offset

        query = f"""
            SELECT
                cc.id as "id",
                cc.masked_card_number as "card_number",
                cc.card_type as "card_type",
                ic.label as "issuer",
                (LPAD(CAST(cc.expiration_month AS TEXT), 2, '0') || '/' || RIGHT(CAST(cc.expiration_year AS TEXT), 2)) as "expires",
                '***' as "cvv",
                TO_CHAR(cc.cc_created, 'YYYY-MM-DD') as "created",
                CASE WHEN a.id IS NOT NULL THEN 
                    jsonb_build_object(
                    'id', a.id, 
                    'account_nickname', a.nickname
                    )
                    ELSE NULL END AS "account",
                cc.nickname,
                CONCAT(p.first_name, ' ', p.last_name) as "cardholder_name",
                jsonb_build_object(
                    'id', addr.id,
                    'street_one', addr.street_one,
                    'street_two', addr.street_two,
                    'postal_code', addr.postal_code,
                    'city', addr.city,
                    'state', jsonb_build_object(
                        'id', s.id,
                        'name', s.name,
                        'abbreviation', s.abbreviation
                    )
                )::json AS "address",
                cc.tm_card as "tm_card",
                account_company.name as "company",
                cc.status as "status",
                cc.is_starred as "starred"
            FROM ams.ams_credit_card cc 
            LEFT JOIN ams.ams_account a ON cc.ams_account_id = a.id
            LEFT JOIN ams.company account_company ON a.company_id = account_company.id::TEXT
            LEFT JOIN ams.ams_address AS addr ON cc.account_address_id::TEXT = addr.id::TEXT
            LEFT JOIN ams.state AS s ON addr.state_id::TEXT = s.id::TEXT
            LEFT JOIN ams.metro_area AS ma ON addr.metro_area_id::TEXT = ma.id::TEXT
            LEFT JOIN ams.ams_person p ON cc.ams_person_id::TEXT = p.id::TEXT
            LEFT JOIN ams.timezone AS t ON ma.timezone = t.id::TEXT
            LEFT JOIN ams.ams_credit_card_issuer ic ON cc.issuer_id = ic.id
            {where_clause}
            ORDER BY cc.created_at DESC
            LIMIT :limit OFFSET :offset
        """

        rows = await get_pg_database().fetch_all(query=query, values=values)
        cards = []
        for row in rows:
            card = {**dict(row), "address": json.loads(row["address"])}
            cards.append(card)
        return cards
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}")


async def get_single_credit_card(card_id: str = "", user_id: str = "") -> EncryptedCreditCardDataWithKeyId:
    try:
        if not card_id or not user_id:
            raise HTTPException(status_code=400, detail="No card id or user id provided")

        query = """
            SELECT
                cc.id,
                cc.card_type as type,
                cc.encrypted_card_number,
                cc.encryption_card_number_iv,
                cc.encrypted_cvv,
                cc.encryption_cvv_iv,
                ic.label as issuer,
                (LPAD(CAST(cc.expiration_month AS TEXT), 2, '0') || '/' || RIGHT(CAST(cc.expiration_year AS TEXT), 2)) as expires,
                p.first_name || ' ' || p.last_name as cardholder_name,
                cc.status,
                aa.street_one,
                aa.street_two,
                aa.city,
                s.name as state_name,
                s.abbreviation as state_abbreviation,
                aa.postal_code
            FROM ams.ams_credit_card cc
            LEFT JOIN ams.ams_person p ON cc.ams_person_id = p.id
            LEFT JOIN ams.ams_credit_card_issuer ic ON cc.issuer_id = ic.id
            LEFT JOIN ams.ams_address aa ON cc.avs_address_id = aa.id
            LEFT JOIN ams.state s ON aa.state_id::TEXT = s.id::TEXT
            WHERE cc.id = :card_id;
            """
        row = await get_pg_database().fetch_one(query=query, values={"card_id": card_id})
        result = dict(row)
        if result is None:
            raise HTTPException(status_code=404, detail="Credit card not found")

        encryption_key_for_storage = get_encryption_key_for_storage()
        card_number = decrypt_card_data(result['encrypted_card_number'],
                                        result['encryption_card_number_iv'], encryption_key_for_storage)
        cvv_number = decrypt_card_data(result['encrypted_cvv'], result['encryption_cvv_iv'], encryption_key_for_storage)
        card_data = {
            "id": str(result['id']),  # Convert UUID to string,
            "type": result['type'],
            "card_number": card_number,
            "cvv": cvv_number,
            "issuer": result['issuer'],
            "expires": result['expires'],
            "cardholder_name": result['cardholder_name'],
            "status": result['status'],
            "street_one": result['street_one'],
            "street_two": result['street_two'],
            "city": result['city'],
            "state_name": result['state_name'],
            "state_abbreviation": result['state_abbreviation'],
            "postal_code": result['postal_code'],
        }

        # Encrypt the card data with a short-lived key
        key_data = await encryption_key_service.create_encryption_key(
            user_id=user_id,
        )
        key = base64.b64decode(key_data['encryption_key'])
        encrypted_card_data, iv = encrypt_card_data(json.dumps(card_data), key)
        # Combine IV and encrypted data
        encrypted_data = iv + encrypted_card_data  # Prepend IV to the encrypted data

        return EncryptedCreditCardDataWithKeyId(
            encrypted_data=base64.b64encode(encrypted_data).decode('utf-8'),
            encrypted_key_id=key_data['key_id']
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}")


async def get_credit_card_for_edit(card_id: str) -> Dict[str, Any]:
    """
    Get full credit card data for editing in the edit modal.
    Returns all necessary information including addresses, account, person, and company.
    """
    try:
        if not card_id:
            raise HTTPException(status_code=400, detail="No card id provided")

        query = """
            SELECT
                cc.id,
                cc.ams_account_id,
                cc.card_type,
                cc.issuer_id,
                cc.type,
                cc.expiration_month,
                cc.expiration_year,
                cc.ams_person_id,
                cc.account_address_id,
                cc.avs_address_id,
                cc.avs_same_as_account,
                cc.tm_card,
                cc.secondary_card,
                cc.status,
                cc.nickname,
                TO_CHAR(cc.cc_created, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') as created,
                cc.cc_created_by as created_by,
                CASE WHEN a.id IS NOT NULL THEN 
                    jsonb_build_object(
                        'id', a.id, 
                        'nickname', a.nickname,
                        'company_id', a.company_id
                    )
                ELSE NULL END AS account,
                jsonb_build_object(
                    'id', p.id,
                    'first_name', p.first_name,
                    'last_name', p.last_name
                ) as person,
                jsonb_build_object(
                    'id', account_addr.id,
                    'street_one', account_addr.street_one,
                    'street_two', account_addr.street_two,
                    'postal_code', account_addr.postal_code,
                    'city', account_addr.city,
                    'state', jsonb_build_object(
                        'id', account_state.id,
                        'name', account_state.name,
                        'abbreviation', account_state.abbreviation
                    ),
                    'address_name', account_addr.address_name,
                    'address_type', account_addr.address_type
                ) as account_address,
                CASE WHEN avs_addr.id IS NOT NULL THEN
                    jsonb_build_object(
                        'id', avs_addr.id,
                        'street_one', avs_addr.street_one,
                        'street_two', avs_addr.street_two,
                        'postal_code', avs_addr.postal_code,
                        'city', avs_addr.city,
                        'state', jsonb_build_object(
                            'id', avs_state.id,
                            'name', avs_state.name,
                            'abbreviation', avs_state.abbreviation
                        ),
                        'address_name', avs_addr.address_name,
                        'address_type', avs_addr.address_type
                    )
                ELSE NULL END as avs_address,
                    jsonb_build_object(
                        'id', account_company.id,
                        'name', account_company.name
                    )
                as company
            FROM ams.ams_credit_card cc
            LEFT JOIN ams.ams_account a ON cc.ams_account_id = a.id
            LEFT JOIN ams.ams_person p ON cc.ams_person_id::TEXT  = p.id::TEXT 
            LEFT JOIN ams.ams_address account_addr ON cc.account_address_id::TEXT  = account_addr.id::TEXT 
            LEFT JOIN ams.state account_state ON account_addr.state_id::TEXT  = account_state.id::TEXT 
            LEFT JOIN ams.ams_address avs_addr ON cc.avs_address_id = avs_addr.id
            LEFT JOIN ams.state avs_state ON avs_addr.state_id::TEXT  = avs_state.id::TEXT 
            LEFT JOIN ams.company account_company ON a.company_id = account_company.id::TEXT
            WHERE cc.id = :card_id
        """

        row = await get_pg_readonly_database().fetch_one(query=query, values={"card_id": card_id})

        if not row:
            raise HTTPException(status_code=404, detail="Credit card not found")

        result = dict(row)

        # Parse JSON fields
        result['account'] = json.loads(result['account']) if result['account'] else None
        result['person'] = json.loads(result['person']) if result['person'] else None
        result['account_address'] = json.loads(result['account_address']) if result['account_address'] else None
        result['avs_address'] = json.loads(result['avs_address']) if result['avs_address'] else None

        # Convert status to match frontend expectations
        result['active'] = result['status'] == 'active'

        return result

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred while fetching card for edit: {e}")


async def check_card_number_exists(card_number: str) -> tuple[bool, Optional[str]]:
    """
    Check if a credit card number already exists in the system.
    
    Args:
        card_number: The card number to check (will be encrypted for comparison)
    
    Returns:
        tuple: (exists: bool, card_id: Optional[str])
    """
    try:
        encryption_key_for_storage = get_encryption_key_for_storage()

        # Encrypt the card number to compare with stored encrypted values
        encrypted_card_number, iv = encrypt_card_data(card_number, encryption_key_for_storage)

        # Get all credit cards and decrypt to check for match
        query = """
            SELECT
                cc.id,
                cc.encrypted_card_number,
                cc.encryption_card_number_iv
            FROM ams.ams_credit_card cc
            WHERE cc.status != 'deleted'
        """

        rows = await get_pg_readonly_database().fetch_all(query=query)

        for row in rows:
            try:
                # Decrypt the stored card number
                stored_card_number = decrypt_card_data(
                    row['encrypted_card_number'],
                    row['encryption_card_number_iv'],
                    encryption_key_for_storage
                )

                # Compare decrypted values
                if stored_card_number == card_number:
                    return True, str(row['id'])
            except Exception as decrypt_error:
                # Skip cards that fail to decrypt
                print(f"Failed to decrypt card {row['id']}: {decrypt_error}")
                continue

        return False, None

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred while checking card number: {e}")


async def get_account_data_for_credit_card(account_id: UUID) -> Optional[AccountData]:
    """
    Retrieve account data needed for credit card creation from AMS database
    
    Args:
        account_id: UUID of the AMS account
        
    Returns:
        AccountData object with all required fields, or None if not found
    """
    try:
        query = """
            SELECT 
                a.id,
                a.nickname,
                a.company_id,
                comp.name as company_name,
                p.id as person_id,
                p.first_name as person_first_name,
                p.last_name as person_last_name,
                p.full_name as person_full_name,
                addr.street_one as address_street_one,
                addr.street_two as address_street_two,
                addr.city as address_city,
                s.abbreviation as address_state,
                addr.postal_code as address_postal_code,
                c.name as address_country,
                e.email_address,
                ph.number as phone_number
            FROM ams.ams_account a
            JOIN ams.ams_person p ON a.ams_person_id = p.id
            JOIN ams.ams_address addr ON a.ams_address_id = addr.id
            LEFT JOIN ams.company comp ON a.company_id = comp.id::text
            LEFT JOIN ams.metro_area ma ON addr.metro_area_id = ma.id
            LEFT JOIN ams.state s ON addr.state_id = s.id
            LEFT JOIN ams.country c ON ma.country_id = c.id
            LEFT JOIN ams.ams_email e ON a.ams_email_id = e.id
            LEFT JOIN ams.phone_number ph ON a.phone_number_id = ph.id
            WHERE a.id = :account_id
        """

        result = await get_pg_readonly_database().fetch_one(
            query=query,
            values={"account_id": str(account_id)}
        )

        if not result:
            return None

        return AccountData(
            id=result["id"],
            nickname=result["nickname"],
            company_id=result["company_id"],
            company_name=result["company_name"],
            person_id=result["person_id"],
            person_first_name=result["person_first_name"],
            person_last_name=result["person_last_name"],
            person_full_name=result["person_full_name"],
            address_street_one=result["address_street_one"],
            address_street_two=result["address_street_two"],
            address_city=result["address_city"],
            address_state=result["address_state"],
            address_postal_code=result["address_postal_code"],
            address_country=result["address_country"],
            email_address=result["email_address"],
            phone_number=result["phone_number"]
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving account data: {str(e)}")


async def get_existing_credit_cards_by_accounts(account_ids: List[str]) -> Dict[str, List[Dict[str, Any]]]:
    """
    Get existing credit cards for multiple accounts
    
    Args:
        account_ids: List of account UUIDs
        
    Returns:
        Dictionary mapping account_id to list of existing credit cards
        Format: {
            "account_id_1": [
                {
                    "provider": "wex",
                    "card_type": "virtual",
                    "status": "active",
                    "masked_number": "****1234",
                    "created_at": "2024-01-15",
                    "nickname": "My Card",
                    "expiration_month": 12,
                    "expiration_year": 2025
                }
            ]
        }
    """
    try:
        if not account_ids:
            return {}

        # Create placeholders for the IN clause
        placeholders = ','.join([f':account_id_{i}' for i in range(len(account_ids))])

        # Build values dict for parameterized query
        values = {f'account_id_{i}': account_id for i, account_id in enumerate(account_ids)}

        query = f"""
            SELECT
                cc.ams_account_id,
                ic.label as provider,
                cc.type as card_type,
                cc.status,
                cc.masked_card_number as masked_number,
                TO_CHAR(cc.cc_created, 'YYYY-MM-DD') as created_at,
                cc.nickname,
                cc.expiration_month,
                cc.expiration_year
            FROM ams.ams_credit_card cc
            LEFT JOIN ams.ams_credit_card_issuer ic ON cc.issuer_id = ic.id
            WHERE cc.ams_account_id IN ({placeholders})
            ORDER BY cc.ams_account_id, cc.cc_created DESC
        """

        rows = await get_pg_readonly_database().fetch_all(query=query, values=values)

        result = {}
        for row in rows:
            account_id = str(row['ams_account_id'])
            if account_id not in result:
                result[account_id] = []

            # Map issuer to provider (normalize provider names)
            provider = row['provider']
            if provider and provider.lower() in ['wex', 'divvy', 'amex']:
                provider = provider.lower()

            card_info = {
                'provider': provider,
                'card_type': row['card_type'],
                'status': row['status'],
                'masked_number': row['masked_number'],
                'created_at': row['created_at'],
                'nickname': row['nickname'],
                'expiration_month': row['expiration_month'],
                'expiration_year': row['expiration_year']
            }
            result[account_id].append(card_info)

        for account_id in account_ids:
            if account_id not in result:
                result[account_id] = []

        return result

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching existing credit cards: {str(e)}")


async def log_credit_card_operation(
        account_id: UUID,
        operation: str,
        provider: str,
        success: bool,
        details: Optional[dict] = None
) -> None:
    """
    Log credit card operations for audit purposes
    
    Args:
        account_id: UUID of the AMS account
        operation: Type of operation
        provider: Credit card provider
        success: Whether the operation was successful
        details: Additional operation details
    """
    try:
        query = """
            INSERT INTO ams.credit_card_request_log (
                ams_account_id,
                operation,
                provider,
                success,
                details,
                created_at
            ) VALUES (
                :account_id,
                :operation,
                :provider,
                :success,
                :details,
                CURRENT_TIMESTAMP
            )
        """

        await get_pg_database().execute(
            query=query,
            values={
                "account_id": str(account_id),
                "operation": operation,
                "provider": provider,
                "success": success,
                "details": json.dumps(details) if details is not None else None
            }
        )

    except Exception as e:
        import logging
        logger = logging.getLogger(__name__)
        logger.error(f"Failed to log credit card operation: {str(e)}")
        

async def get_all_credit_card_issuers():
    try:
        query = """
            SELECT id, label, has_avs 
            FROM ams.ams_credit_card_issuer
            ORDER BY label ASC
            """
        return await get_pg_readonly_database().fetch_all(query=query)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")
    

async def create_credit_card_issuer(issuer_data: CreateCreditCardIssuerRequest) -> str:
    try:
        query = """
            INSERT INTO ams.ams_credit_card_issuer (label, has_avs)
            VALUES (:label, :has_avs)
            RETURNING id
            """
        values = {
            "label": issuer_data.label,
            "has_avs": issuer_data.has_avs
        }
        result = await get_pg_database().fetch_one(query=query, values=values)
        if not result:
            raise HTTPException(status_code=500, detail="Failed to create credit card issuer.")
        return str(result['id'])
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")
