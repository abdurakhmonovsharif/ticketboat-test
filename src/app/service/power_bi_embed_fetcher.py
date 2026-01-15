import json
import requests
from fastapi import HTTPException, status
from pydantic import BaseModel

from app.model.user import User
from app.service.msal_service import MsalService


class EmbedTokenRequestBody(BaseModel):
    datasets: list[dict] = []
    reports: list[dict] = []
    targetWorkspaces: list[dict] = []
    identities: list[dict] = []


class PowerBiReport(BaseModel):
    report_id: str
    workspace_id: str


class PowerBiEmbedFetcher:
    def __init__(self):
        self._msal_service = MsalService()

    def get_embed_token(self, user: User, powerbi_report: PowerBiReport):
        report_url = f"https://api.powerbi.com/v1.0/myorg/groups/{powerbi_report.workspace_id}/reports/{powerbi_report.report_id}"
        api_response = requests.get(report_url, headers=self._get_request_header())
        if api_response.status_code != 200:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f'Error while retrieving Embed URL\n{api_response.reason}:\t{api_response.text}\nRequestId:\t{api_response.headers.get("RequestId")}',
            )
        api_response = json.loads(api_response.text)
        powerbi_dataset_ids = [api_response["datasetId"]]
        return self._generate_embed_token(user, powerbi_report, powerbi_dataset_ids)

    def _generate_embed_token(
        self, user: User, powerbi_report: PowerBiReport, powerbi_dataset_ids: list
    ):
        try:
            return self._generate_embed_token_with_identities(
                user, powerbi_report, powerbi_dataset_ids
            )
        except requests.exceptions.HTTPError as exc:
            if exc.response.status_code == 400:
                error_response = exc.response.json()
                if (
                    "error" in error_response
                    and error_response["error"]["code"] == "InvalidRequest"
                    and "shouldn't have effective identity"
                    in error_response["error"]["message"]
                ):
                    return self._generate_embed_token_without_identities(
                        powerbi_report, powerbi_dataset_ids
                    )
            raise

    def _generate_embed_token_with_identities(
        self, user: User, powerbi_report: PowerBiReport, powerbi_dataset_ids: list
    ):
        body = self._prepare_embed_token_request_body(
            user, powerbi_report, powerbi_dataset_ids, include_identities=True
        )
        response_json = self._make_embed_token_request(body)
        return response_json

    def _generate_embed_token_without_identities(
        self, powerbi_report: PowerBiReport, powerbi_dataset_ids: list
    ):
        body = self._prepare_embed_token_request_body(
            None, powerbi_report, powerbi_dataset_ids, include_identities=False
        )
        response_json = self._make_embed_token_request(body)
        return response_json

    def _prepare_embed_token_request_body(
        self,
        user: User,
        powerbi_report: PowerBiReport,
        powerbi_dataset_ids: list,
        include_identities: bool,
    ):
        body = EmbedTokenRequestBody()
        body.reports = [{"id": powerbi_report.report_id}]
        body.datasets = [
            {"id": powerbi_dataset_id} for powerbi_dataset_id in powerbi_dataset_ids
        ]
        body.targetWorkspaces = [{"id": powerbi_report.workspace_id}]
        if include_identities:
            group_names = user.roles
            usernames_and_group_names_str = "|".join([user.email, *group_names])
            body.identities = [
                {
                    "username": usernames_and_group_names_str,
                    "roles": ["ClientRole"],
                    "datasets": powerbi_dataset_ids,
                }
            ]
        return body

    def _make_embed_token_request(self, body):
        embed_token_url = "https://api.powerbi.com/v1.0/myorg/GenerateToken"
        headers = self._get_request_header()

        response = requests.post(url=embed_token_url, headers=headers, json=body.dict())
        response.raise_for_status()  # raises requests.exceptions.HTTPError for 4xx and 5xx responses

        response_json = response.json()
        response_json["associatedReports"] = [report["id"] for report in body.reports]
        return response_json

    def _get_request_header(self):
        return {
            "Content-Type": "application/json",
            "Authorization": "Bearer " + self._msal_service.get_access_token(),
        }
