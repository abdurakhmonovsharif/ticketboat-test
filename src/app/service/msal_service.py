import os
import ast
from fastapi import HTTPException
import msal


class MsalService:
    def get_access_token(self):
        """Generates and returns Access token

        Returns:
            string: Access token
        """

        response = None

        authority = os.getenv("AZURE_AUTHORITY_URL").replace(
            "organizations", os.getenv("AZURE_TENANT_ID")
        )
        clientapp = msal.ConfidentialClientApplication(
            os.getenv("AZURE_AD_APP_ID"),
            client_credential=os.getenv("AZURE_AD_APP_SECRET"),
            authority=authority,
        )

        scopes: list = ast.literal_eval(os.getenv("AZURE_SCOPE_BASE"))
        # Make a client call if Access token is not available in cache
        response = clientapp.acquire_token_for_client(scopes=scopes)

        try:
            return response["access_token"]
        except KeyError:
            raise HTTPException(detail=response["error_description"])

        except HTTPException as ex:
            raise HTTPException(detail="Error retrieving Access token\n" + str(ex))
