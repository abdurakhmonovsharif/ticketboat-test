import os
import json
import logging
from typing import Optional
from uuid import UUID
import boto3
from botocore.exceptions import ClientError

from app.model.wex_config_models import WEXCredentials

logger = logging.getLogger(__name__)


class WEXCredentialManager:
    """
    Manages WEX credentials from AWS Secrets Manager.
    
    Credentials are stored in AWS Secrets Manager:
    - Secret name: {environment}/wex/{company_id}
    - Example: production/wex/123e4567-e89b-12d3-a456-426614174000
    
    Secret data example:
    {
        "org_group_login_id": "org_id",
        "username": "username",
        "password": "SecurePassword!",
        "bank_number": "4567",
        "company_number": "4564565"
    }
    """
    
    def __init__(self):
        self._secrets_client = None
        self._region = os.getenv("AWS_REGION", "us-east-1")
        self._environment = os.getenv("ENVIRONMENT", "staging")
        self._cache = {}
        self._cache_ttl = 120  # seconds
        
    
    def _get_secrets_client(self):
        """Lazy initialization of AWS Secrets Manager client"""
        if self._secrets_client is None:
            try:
                self._secrets_client = boto3.client('secretsmanager', region_name=self._region)
                logger.info(f"Initialized AWS Secrets Manager client in region: {self._region}")
            except Exception as e:
                logger.error(f"Failed to initialize AWS Secrets Manager client: {str(e)}")
        return self._secrets_client
    
    
    def _get_secret_name(self, company_id: UUID) -> str:
        """Generate secret name from company ID"""
        return f"{self._environment}/wex/{company_id}"
    
    
    def _load_from_secrets_manager(self, company_id: UUID) -> Optional[WEXCredentials]:
        """Load credentials from AWS Secrets Manager"""
        client = self._get_secrets_client()
        if not client:
            logger.error("Secrets Manager client not available")
            return None
        
        secret_name = self._get_secret_name(company_id)
        try:
            logger.info(f"Fetching WEX credentials for company {company_id} from secret: {secret_name}")
            response = client.get_secret_value(SecretId=secret_name)
            
            if 'SecretString' not in response:
                logger.error(f"Secret {secret_name} has no SecretString")
                return None
            
            secret_data = json.loads(response['SecretString'])
            required_fields = ['org_group_login_id', 'username', 'password', 'company_number']
            missing = [f for f in required_fields if f not in secret_data]
            
            if missing:
                logger.error(f"Secret {secret_name} missing required fields: {', '.join(missing)}")
                return None
            
            credentials = WEXCredentials(
                org_group_login_id=secret_data['org_group_login_id'],
                username=secret_data['username'],
                password=secret_data['password'],
                bank_number=secret_data.get('bank_number', '0010'),
                company_number=secret_data['company_number']
            )
            
            logger.info(f"Successfully loaded WEX credentials for company {company_id}")
            return credentials
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'ResourceNotFoundException':
                logger.warning(f"No WEX credentials found for company {company_id} (secret: {secret_name})")
            else:
                logger.error(f"Error loading secret {secret_name}: {str(e)}")
            return None
            
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in secret {secret_name}: {str(e)}")
            return None
            
        except Exception as e:
            logger.error(f"Unexpected error loading secret {secret_name}: {str(e)}")
            return None
        
    
    def get_credentials_for_company(self, company_id: UUID) -> Optional[WEXCredentials]:
        """
        Get WEX credentials for a specific company.
        
        Args:
            company_id: UUID of the company
            
        Returns:
            WEXCredentials object, or None if not found
        """
        
        cache_key = str(company_id)
        if cache_key in self._cache:
            cached_creds, cached_time = self._cache[cache_key]
            import time
            if time.time() - cached_time < self._cache_ttl:
                logger.debug(f"Using cached credentials for company {company_id}")
                return cached_creds
        
        credentials = self._load_from_secrets_manager(company_id)
        if credentials:
            import time
            self._cache[cache_key] = (credentials, time.time())
        
        return credentials

    
    def clear_cache(self):
        """Clear the credentials cache"""
        self._cache.clear()
        logger.info("Cleared WEX credentials cache")


# Global singleton instance
wex_credential_manager = WEXCredentialManager()
