import asyncio
import os
import random
import re
from typing import Any, Awaitable, Callable, Optional

import aiohttp

from app.db.ams_db import (
    get_mlx_account_creation_data,
    update_accounts_with_mlx_ids,
)
from app.utils import get_ses_client


async def rate_limiter(
    request_func: Callable[[], Awaitable[aiohttp.ClientResponse]],
    max_retries: int = 6,
    retry_delay: int = 30,
    success_codes: set[int] | None = None,
    retry_on_statuses: set[int] | None = None,
    consume_body: bool = False,
) -> aiohttp.ClientResponse:
    """
    Retries a request function up to max_retries times with a delay, surfacing the
    underlying failure instead of masking it behind a generic retry error.
    """
    if success_codes is None:
        success_codes = {200, 201}
    retry_on_statuses = retry_on_statuses or {429, 500, 502, 503, 504}

    last_error: Exception | None = None

    for attempt in range(max_retries):
        try:
            response = await request_func()
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            last_error = exc
            if attempt < max_retries - 1:
                print(f"Request failed (attempt {attempt + 1}/{max_retries}): {exc}")
                print(f"Retrying in {retry_delay} seconds...")
                await asyncio.sleep(retry_delay)
                continue
            break

        if response.status in success_codes:
            if consume_body:
                await response.read()
            return response

        body_text = await response.text()
        if response.status not in retry_on_statuses:
            raise aiohttp.ClientResponseError(
                response.request_info,
                response.history,
                status=response.status,
                message=f"Unexpected status {response.status}: {body_text}",
                headers=response.headers,
            )

        last_error = aiohttp.ClientResponseError(
            response.request_info,
            response.history,
            status=response.status,
            message=f"Retryable status {response.status}: {body_text}",
            headers=response.headers,
        )
        if attempt < max_retries - 1:
            print(f"Retrying in {retry_delay} seconds...")
            await asyncio.sleep(retry_delay)

    if last_error:
        raise last_error
    raise RuntimeError("rate_limiter exited without a response or exception.")


def _require_mlx_token() -> str:
    token = os.getenv("MLX_IT_KEY")
    if not token:
        raise EnvironmentError("MLX_IT_KEY not set in environment variables.")
    return token


def _normalize_tag_name(tag_name: str) -> str:
    return re.sub(r"\s+", "", tag_name.strip().lower())


def _environment_tag(tag_name: str, environment: str) -> str:
    return f"{environment}-{tag_name}" if environment != "prod" else tag_name


class MlxProfileService:
    """
    Singleton-ish service to orchestrate MLX profile creation with
    per-step error capture. Call `run` to execute the full pipeline.
    """

    def __init__(self) -> None:
        self.token: Optional[str] = None
        self.headers: dict[str, str] = {}
        self.environment = os.getenv("ENVIRONMENT", "staging")

    def _account_label(self, account: dict) -> str:
        """Prefer human-friendly account identifiers in logs and errors."""
        return (
            account.get("nickname")
            or account.get("name")
            or account.get("id")
            or account.get("multilogin_id")
            or "unknown account"
        )

    def _refresh_auth(self) -> None:
        self.token = _require_mlx_token()
        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "accept": "application/json",
        }

    async def create_payload(self, account: dict) -> dict:
        name = (
            f"{account['nickname']}-{self.environment}"
            if self.environment != "prod"
            else account["nickname"]
        )
        # width, height
        resolution_options = [
            (1280, 1024),
            (1440, 900),
            (1600, 900),
            (1680, 1050),
            (1920, 1080),
            (1920, 1200),
            (2560, 1440),
            (2560, 1600),
            (3840, 2160),
        ]

        resolution = random.choice(resolution_options)

        return {
            "name": name,
            "browser_type": "mimic",
            "folder_id": "9d2eefe3-00b2-4127-aced-b8d3524ef15c",  # AMS Built Folder
            "os_type": "windows",
            "notes": account["email_address"],
            "parameters": {
                "custom_start_urls": [
                    "https://ip-api.com/",
                    "https://mail.google.com/",
                ],
                "flags": {
                    "audio_masking": "natural",
                    "canvas_noise": "mask",
                    "fonts_masking": "mask",
                    "geolocation_masking": "mask",
                    "geolocation_popup": "prompt",
                    "graphics_masking": "mask",
                    "graphics_noise": "mask",
                    "localization_masking": "mask",
                    "media_devices_masking": "mask",
                    "navigator_masking": "mask",
                    "ports_masking": "mask",
                    "proxy_masking": "custom",
                    "quic_mode": "disabled",
                    "screen_masking": "custom",
                    "startup_behavior": "recover",
                    "timezone_masking": "mask",
                    "webrtc_masking": "mask",
                },
                "notes": account["email_address"],
                "proxy": {
                    "host": account["proxy_host"],
                    "password": account["proxy_pass"],
                    "port": int(account["proxy_port"]),
                    "save_traffic": False,
                    "type": "http",
                    "username": account["proxy_user"],
                },
                "storage": {
                    "is_local": False,
                    "save_service_worker": True,
                },
                "fingerprint": {
                    "screen": {"height": resolution[1], "pixel_ratio": 1.0, "width": resolution[0]},
                },
            },
        }

    async def create_profile(self, account: dict, session: aiohttp.ClientSession) -> dict:
        url = "https://api.multilogin.com/profile/create"
        payload = await self.create_payload(account)
        res = await rate_limiter(lambda: session.post(url, json=payload, headers=self.headers))
        result = await res.json()
        return {
            "id": account["id"],
            "nickname": account.get("nickname"),
            "multilogin_id": result["data"]["ids"][0],
            "tags": account.get("tags", []),
        }

    async def get_tags(self, session: aiohttp.ClientSession) -> list[dict]:
        url = "https://api.multilogin.com/tag/search"
        limit = 100
        offset = 0
        all_tags = []

        while True:
            payload = {"limit": limit, "offset": offset, "search_text": ""}
            res = await rate_limiter(
                lambda: session.post(url, headers=self.headers, json=payload),
                retry_delay=5,
            )
            data = await res.json()

            body = data.get("data") if isinstance(data, dict) else None
            if not isinstance(body, dict):
                raise ValueError("Unexpected MLX tags response format: missing data field.")

            tags = body.get("tags", [])
            total_count = body.get("total_count")

            if not isinstance(tags, list):
                raise ValueError("Unexpected MLX tags response format: tags not a list.")

            all_tags.extend(tags)

            if total_count is not None and len(all_tags) >= total_count:
                break

            if len(tags) < limit:
                break

            offset += limit

        return all_tags

    async def reconcile_tags(self, profiles: list[dict], session: aiohttp.ClientSession) -> dict:
        existing_tags = await self.get_tags(session)
        normalized_existing_tags_map = {
            _normalize_tag_name(tag["name"]): tag["id"] for tag in existing_tags
        }

        tags_to_create = set()
        for profile in profiles:
            for tag_name in profile.get("tags", []):
                target_name = _environment_tag(tag_name, self.environment)
                normalized_tag = _normalize_tag_name(target_name)
                if normalized_tag not in normalized_existing_tags_map:
                    tags_to_create.add(target_name)

        url = "https://api.multilogin.com/tag/create"
        colors = ["blue", "green", "red", "orange", "purple", "teal", "yellow", "gray"]
        payload = {
            "tags": [
                {
                    "name": tag_name,
                    "color": random.choice(colors),
                }
                for tag_name in tags_to_create
            ]
        }
        if payload["tags"]:
            await rate_limiter(
                lambda: session.post(url, json=payload, headers=self.headers),
                success_codes={200, 201, 409},
                consume_body=True,
            )

        refreshed_tags = await self.get_tags(session)
        normalized_existing_tags_map = {
            _normalize_tag_name(tag["name"]): tag["id"] for tag in refreshed_tags
        }
        return normalized_existing_tags_map

    async def enable_resource_for_profile(
        self, profile: dict, resource_id: str, session: aiohttp.ClientSession
    ) -> None:
        url = f"https://api.multilogin.com/api/v1/resources/{resource_id}/enable_for_profiles"
        payload = {"profile_ids": [profile["multilogin_id"]]}
        await rate_limiter(
            lambda: session.post(url, json=payload, headers=self.headers),
            consume_body=True,
        )

    async def enable_purchase_tracker(self, profile: dict, session: aiohttp.ClientSession) -> None:
        await self.enable_resource_for_profile(
            profile, "71f5ede6-413b-4cd1-a926-44972150022a", session
        )

    async def enable_bitwarden(self, profile: dict, session: aiohttp.ClientSession) -> None:
        await self.enable_resource_for_profile(profile, "a2bb4af7-b2d8-4c81-b604-4d5e07a08e6d", session)

    async def add_cookies_to_profile(self, profile: dict, session: aiohttp.ClientSession) -> None:
        url = "https://cookies.multilogin.com/api/v1/cookies/metadata"
        payload = {"profile_id": profile["multilogin_id"], "target_website": "amazon"}
        await rate_limiter(
            lambda: session.post(url, json=payload, headers=self.headers),
            consume_body=True,
        )

    async def tag_profile(
        self, profile: dict, tag_map: dict, session: aiohttp.ClientSession
    ) -> Optional[str]:
        url = "https://api.multilogin.com/profile/tag/assign"
        tag_ids: list[str] = []
        tag_labels: list[str] = []
        seen: set[str] = set()
        for tag_name in profile.get("tags", []):
            normalized_tag = _normalize_tag_name(_environment_tag(tag_name, self.environment))
            tag_id = tag_map.get(normalized_tag)
            if tag_id and tag_id not in seen:
                tag_ids.append(tag_id)
                tag_labels.append(normalized_tag)
                seen.add(tag_id)

        if not tag_ids:
            return None

        warning: Optional[str] = None
        if len(tag_ids) > 10:
            warning = (
                f"tag limit exceeded for profile {self._account_label(profile)}: "
                f"applied first 10 tags; skipped {len(tag_ids) - 10} extra "
                f"({', '.join(tag_labels[10:])})"
            )
            tag_ids = tag_ids[:10]

        payload = {"profile_id": profile["multilogin_id"], "tags": tag_ids}
        print(f"Tagging profile {profile['multilogin_id']} with tags {tag_ids}")
        await rate_limiter(
            lambda: session.post(url, json=payload, headers=self.headers),
            retry_delay=3,
            consume_body=True,
        )
        return warning

    def _send_error_email(
        self,
        task_name: str,
        accounts_requested: int,
        profiles_created: int,
        errors: list[str],
        user_email: str | None = None,
    ) -> Optional[str]:
        """
        Send a simple SES email summarizing errors. Returns an error string if send fails.
        """
        try:
            ses_client = get_ses_client()
            if not ses_client:
                return "SES client unavailable; email not sent."

            source = os.getenv("FORWARDER_FROM_EMAIL", "forwarder@tb-portal.com")
            subject = f"[AMS] MLX profile creation errors ({task_name})"
            destination = (
                {"ToAddresses": [user_email]}
                if user_email
                else {"ToAddresses": ["support@ticketboat.com"]}
            )
            body_lines = [
                f"Task: {task_name}",
                f"Accounts requested: {accounts_requested}",
                f"Profiles created: {profiles_created}",
                "",
                "Errors:",
                *[f"- {err}" for err in errors],
            ]
            body_text = "\n".join(body_lines)

            ses_client.send_email(
                Source=source,
                Destination=destination,
                Message={
                    "Subject": {"Data": subject},
                    "Body": {"Text": {"Data": body_text}},
                },
            )
            return None
        except Exception as exc:
            return f"Failed to send error email: {exc}"

    async def update_db(self, profiles: list[dict]) -> Any:
        return await update_accounts_with_mlx_ids(
            [profile for profile in profiles if profile.get("multilogin_id")]
        )

    async def run(
        self,
        account_id_list: list[str],
        task_name: str | None = None,
        user_email: str | None = None,
    ) -> None:
        name = task_name or f"mlx_profiles:{len(account_id_list)}"
        self.environment = os.getenv("ENVIRONMENT", "staging")
        self._refresh_auth()
        print(f"[{name}] Starting MLX profile pipeline")

        errors: list[str] = []
        timeout = aiohttp.ClientTimeout(total=60)
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                try:
                    accounts_data = await get_mlx_account_creation_data(account_id_list)
                except Exception as exc:
                    print(f"[{name}] Error fetching account data: {exc}")
                    return

                if not accounts_data:
                    print(f"[{name}] No accounts to process.")
                    return

                created_profiles: list[dict] = []
                for account in accounts_data:
                    try:
                        profile = await self.create_profile(account, session)
                        created_profiles.append(profile)
                    except Exception as exc:
                        errors.append(f"create profile for {self._account_label(account)}: {exc}")

                for profile in created_profiles:
                    for step_name, fn in (
                        ("enable purchase tracker", self.enable_purchase_tracker),
                        ("enable bitwarden", self.enable_bitwarden),
                        ("add cookies", self.add_cookies_to_profile),
                    ):
                        try:
                            await fn(profile, session)
                        except Exception as exc:
                            errors.append(f"{step_name} for {self._account_label(profile)}: {exc}")

                tag_map = None
                try:
                    tag_map = await self.reconcile_tags(created_profiles, session)
                except Exception as exc:
                    errors.append(f"reconcile tags: {exc}")

                if tag_map:
                    for profile in created_profiles:
                        try:
                            warning = await self.tag_profile(profile, tag_map, session)
                            if warning:
                                errors.append(warning)
                        except Exception as exc:
                            errors.append(f"tag profile for {self._account_label(profile)}: {exc}")
                else:
                    errors.append("tagging skipped: tag map unavailable")

                try:
                    await self.update_db(created_profiles)
                except Exception as exc:
                    errors.append(f"db update: {exc}")

                print(f"[{name}] Finished MLX profile pipeline.")
                print(f"[{name}] Accounts requested: {len(accounts_data)}")
                print(f"[{name}] Profiles created: {len(created_profiles)}")

                if errors:
                    for err in errors:
                        print(f"[{name}] ERROR: {err}")
                    email_err = self._send_error_email(
                        task_name=name,
                        accounts_requested=len(accounts_data),
                        profiles_created=len(created_profiles),
                        errors=errors,
                        user_email=user_email,
                    )
                    if email_err:
                        print(f"[{name}] ERROR: {email_err}")
                else:
                    print(f"[{name}] No errors encountered.")
        except Exception as exc:
            print(f"[{name}] MLX profile pipeline failed: {exc}")


def get_mlx_profile_service() -> MlxProfileService:
    # Return a fresh instance per call to avoid shared mutable state between concurrent runs.
    return MlxProfileService()
