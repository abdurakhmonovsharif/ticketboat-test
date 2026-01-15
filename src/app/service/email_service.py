import base64
import binascii
import json
import logging
import quopri
import re
import time
import traceback
import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional, List
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formatdate, formataddr

import pytz
from fastapi import HTTPException
from opensearchpy import AsyncOpenSearch
from pydantic import BaseModel

from app.cache import invalidate_cache
from app.database import get_pg_database, get_async_opensearch_client
from app.utils import get_ses_client

logger = logging.getLogger(__name__)

EMAIL_INDEX = "email"
EMAIL_BODY_INDEX = "email_body"
CACHE_KEY_PATTERN = f"email_list_v3/*"


class CommentResponse(BaseModel):
    id: str
    text: Optional[str]
    author: Optional[str]
    created_at: Optional[str]
    scope: Optional[str]


def normalize_date_to_utc(date_str: str) -> datetime | None:
    try:
        if date_str.endswith('Z'):
            # Handle UTC format ending with Z
            return datetime.fromisoformat(date_str.replace('Z', '+00:00'))
        else:
            # Handle format with timezone offset
            dt = datetime.fromisoformat(date_str)
            return dt.astimezone(pytz.UTC)
    except ValueError as e:
        print(f"Error parsing date {date_str}: {e}")
        return None


def convert_to_timezone(dt: datetime, timezone_str: str) -> datetime | None:
    try:
        target_tz = pytz.timezone(timezone_str)
        return dt.astimezone(target_tz)
    except Exception as e:
        print(f"Error converting timezone: {e}")
        return dt


class EmailService:

    def __init__(self, opensearch_client: Optional[AsyncOpenSearch] = None):
        self.client = opensearch_client

    async def get_email_comments(self, email_id: str) -> List[dict]:
        all_comments = []
        async with get_async_opensearch_client() as client:
            query = {
                "query": {
                    "term": {
                        "id.keyword": email_id
                    }
                },
                "_source": ["comments", "duplication_id"],
                "size": 1
            }

            response = await client.search(
                index="emails",
                body=query
            )

            if not response["hits"]["hits"]:
                raise HTTPException(
                    status_code=404,
                    detail=f"Email not found with id {email_id}"
                )

            email_data = response["hits"]["hits"][0]["_source"]
            email_comments = email_data.get("comments", [])
            if email_comments and isinstance(email_comments, list):
                for ec in email_comments:
                    all_comments.append(
                        CommentResponse(
                            **ec,
                            scope="individual"
                        ).model_dump()
                    )

            duplication_id = email_data.get("duplication_id")
            if duplication_id:
                group_comments: List[dict] = await self.get_group_comments(duplication_id)
                all_comments.extend(group_comments)

            all_comments.sort(key=lambda x: x.get("created_at", ""))
            return all_comments

    async def get_group_comments(self, duplication_id: str) -> List[dict]:
        comments = []
        async with get_async_opensearch_client() as client:
            query = {
                "query": {
                    "term": {"duplication_id": duplication_id}
                },
                "sort": [
                    {"date": {"order": "asc"}},
                    {"id.keyword": {"order": "asc"}}
                ],
                "size": 1,
                "_source": ["id", "group_comments"]
            }

            search_response = await client.search(
                index="emails",
                body=query
            )

            if search_response["hits"]["total"]["value"] == 0:
                raise HTTPException(
                    status_code=404,
                    detail=f"No emails found with duplication_id {duplication_id}"
                )

            source = search_response["hits"]["hits"][0]["_source"]
            group_comments = source.get("group_comments", [])
            if group_comments and isinstance(group_comments, list):
                for gc in group_comments:
                    comments.append(
                        CommentResponse(
                            **gc,
                            scope="group"
                        ).model_dump()
                    )
            return comments

    async def get_email_list(
            self,
            timezone: str = "America/Chicago",
            page: int = 1,
            page_size: int = 2,
            search_term: Optional[str] = None,
            filter_flags: Optional[List[str]] = None,
            filter_users: Optional[List[str]] = None,
            start_date: Optional[str] = None,
            end_date: Optional[str] = None,
            from_email: Optional[str] = None,
            to_email: Optional[str] = None,
            subject: Optional[str] = None,
            search_in: Optional[str] = 'inbox',
            task_complete: Optional[bool] = None
    ):
        try:
            client_init_start = time.time()
            self.client = get_async_opensearch_client()
            client_init_duration = time.time() - client_init_start
            print(f"Time spent initializing OpenSearch client: {client_init_duration:.3f} seconds")

            overall_start_time = time.time()

            # Build base query for filtering emails
            base_query = {"bool": {"must": []}}

            # Apply inbox/archived/starred filter
            if search_in != 'all':
                if search_in == 'inbox' or not search_in:
                    # For inbox: archived should be false or not exist
                    base_query["bool"]["must_not"] = base_query["bool"].get("must_not", [])
                    base_query["bool"]["must_not"].extend([
                        {"term": {"archived": "true"}},
                        {"term": {"archived": True}}
                    ])
                elif search_in == 'starred':
                    # For starred: starred should be true
                    base_query["bool"]["must"].append({
                        "bool": {
                            "should": [
                                {"term": {"starred": "true"}},
                                {"term": {"starred": True}}
                            ],
                            "minimum_should_match": 1
                        }
                    })
                elif search_in == 'archived':
                    # For archived: archived should be true
                    base_query["bool"]["must"].append({
                        "bool": {
                            "should": [
                                {"term": {"archived": "true"}},
                                {"term": {"archived": True}}
                            ],
                            "minimum_should_match": 1
                        }
                    })

            # Apply from_email filter
            if from_email:
                base_query["bool"]["must"].append({"match_phrase": {"from": from_email}})

            # Apply subject filter
            if subject:
                base_query["bool"]["must"].append({"match_phrase": {"subject": subject}})

            # Apply to_email filter
            if to_email:
                base_query["bool"]["must"].append({"match_phrase": {"to": to_email}})

            # Apply date range filter
            if start_date and end_date:
                start_dt = datetime.strptime(start_date, "%Y-%m-%d")
                end_dt = datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)
                base_query["bool"]["must"].append({
                    "range": {
                        "date": {
                            "gte": start_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
                            "lt": end_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
                        }
                    }
                })

            # Apply flag filters
            if filter_flags:
                # Separate no_flags from other flags
                other_flags = [flag for flag in filter_flags if flag != "no_flags"]
                has_no_flags = "no_flags" in filter_flags
                
                # Build flag conditions
                flag_conditions = []
                
                # Add condition for emails with no flags if no_flags is specified
                if has_no_flags:
                    flag_conditions.append({"bool": {"must_not": {"exists": {"field": "flags"}}}})
                
                # Add conditions for emails with specific flags
                if other_flags:
                    flag_terms = []
                    for flag in other_flags:
                        flag_terms.append({"term": {"flags": flag.lower()}})
                    flag_conditions.append({"bool": {"should": flag_terms, "minimum_should_match": 1}})
                
                # Combine all flag conditions with OR logic
                if flag_conditions:
                    base_query["bool"]["must"].append({"bool": {"should": flag_conditions, "minimum_should_match": 1}})

            # Apply user filters
            if filter_users:
                if "no_users" in filter_users:
                    base_query["bool"]["must_not"] = base_query["bool"].get("must_not", []) + [
                        {"exists": {"field": "assigned_user_ids"}}]
                else:
                    user_terms = []
                    for user in filter_users:
                        user_terms.append({"term": {"assigned_user_ids.keyword": user.lower()}})
                    base_query["bool"]["must"].append({"bool": {"should": user_terms, "minimum_should_match": 1}})

            # Apply task_complete filter
            if task_complete is not None:
                if task_complete:
                    # Find emails where is_task_complete = true
                    base_query["bool"]["must"].append({
                        "bool": {
                            "should": [
                                {"term": {"is_task_complete": "true"}},
                                {"term": {"is_task_complete": True}}
                            ],
                            "minimum_should_match": 1
                        }
                    })
                else:
                    # Find emails where is_task_complete = false OR is not set at all
                    base_query["bool"]["must"].append({
                        "bool": {
                            "should": [
                                {"term": {"is_task_complete": "false"}},
                                {"term": {"is_task_complete": False}},
                                {"bool": {"must_not": {"exists": {"field": "is_task_complete"}}}}
                            ],
                            "minimum_should_match": 1
                        }
                    })
            # If task_complete is None, no filter is applied (find emails regardless of is_task_complete value)

            # Exclude deleted emails unless searching in archived
            if search_in != 'archived':
                base_query["bool"]["must_not"] = base_query["bool"].get("must_not", []) + [
                    {"term": {"is_deleted": True}}
                ]

            # Handle search term in the unified index
            if search_term:
                # Efficient match query: finds if search_term appears in any of the fields
                # Uses fuzzy matching to handle "mud" -> "mudd"
                # Much faster than regexp queries on analyzed text fields
                
                # Simple bool query: match if ANY field contains the search_term (with fuzzy tolerance)
                search_conditions = [
                    {"match": {"subject": {"query": search_term, "operator": "and", "fuzziness": "AUTO"}}},
                    {"match": {"body_plain": {"query": search_term, "operator": "and", "fuzziness": "AUTO"}}}
                ]
                
                search_query = {
                    "bool": {
                        "should": search_conditions,
                        "minimum_should_match": 1
                    }
                }
                base_query["bool"]["must"].append(search_query)

            # Get total count of unique duplication_ids that match our query
            count_query = {
                "size": 0,
                "query": base_query,
                "aggs": {
                    "unique_duplication_ids": {
                        "cardinality": {
                            "field": "duplication_id.keyword"
                        }
                    }
                }
            }

            try:
                print("Starting count query...")
                count_start = time.time()

                count_response = await self.client.search(
                    index="emails",
                    body=count_query
                )

                count_duration = time.time() - count_start
                print(f"Count query time: {count_duration:.3f} seconds")

                total_count = count_response["aggregations"]["unique_duplication_ids"]["value"]
                print(f"Count: {total_count}")
            except Exception as e:
                print(f"Error getting count: {str(e)}")
                total_count = 0

            # STEP 1: Get top duplication_ids ordered by their latest date
            collapse_query = {
                "size": page_size,
                "from": (page - 1) * page_size,
                "query": base_query,
                "sort": [
                    {"date": {"order": "desc"}},
                    {"id.keyword": {"order": "asc"}}
                ],
                "collapse": {
                    "field": "duplication_id.keyword",
                    "inner_hits": {
                        "name": "latest_email",
                        "size": 1,
                        "sort": [
                            {"date": {"order": "desc"}},
                            {"id.keyword": {"order": "asc"}}
                        ]
                    }
                },
                "_source": ["duplication_id", "date"]
            }

            try:
                print("Starting collapse query for duplication_ids...")
                collapse_start = time.time()

                collapse_response = await self.client.search(
                    index="emails",
                    body=collapse_query
                )

                collapse_duration = time.time() - collapse_start
                print(f"Collapse query time: {collapse_duration:.3f} seconds")

                # Extract duplication_ids
                duplication_ids = []
                for hit in collapse_response["hits"]["hits"]:
                    duplication_ids.append(hit["_source"]["duplication_id"])

                if not duplication_ids:
                    return {"total": total_count, "items": []}

            except Exception as e:
                print(f"Error executing collapse query: {str(e)}")
                return {"total": total_count, "items": []}

            # STEP 2: Get detailed information for these duplication_ids
            detail_query = {
                "size": 1000,  # Adjust based on maximum emails per group you want to fetch
                "query": {
                    "bool": {
                        "must": [
                            base_query,
                            {"terms": {"duplication_id.keyword": duplication_ids}}
                        ]
                    }
                },
                "sort": [
                    {"date": {"order": "desc"}},
                    {"id.keyword": {"order": "asc"}}
                ],
                "_source": [
                    "id", "from", "to", "date", "subject",
                    "summary", "read", "starred", "archived",
                    "comments", "group_comments", "flags", "flag_ids",
                    "flag_user_mapping", "assigned_user_ids", "nickname",
                    "duplication_id", "is_task_complete", "is_starred", "is_deleted"
                ]
            }

            try:
                print("Starting detail query...")
                detail_start = time.time()

                detail_response = await self.client.search(
                    index="emails",
                    body=detail_query
                )

                detail_duration = time.time() - detail_start
                print(f"Detail query time: {detail_duration:.3f} seconds")

            except Exception as e:
                print(f"Error executing detail query: {str(e)}")
                return {"total": total_count, "items": []}

            # Transform results
            print("Starting transformation of results...")
            start_transform_time = time.time()

            # Group emails by duplication_id
            email_groups = {}
            for hit in detail_response["hits"]["hits"]:
                source = hit["_source"]
                dup_id = source["duplication_id"]

                if dup_id not in email_groups:
                    email_groups[dup_id] = []

                email_groups[dup_id].append(source)

            # Process each group
            items = []
            for dup_id in duplication_ids:  # Process in the original order from collapse query
                if dup_id not in email_groups:
                    continue

                emails_list = email_groups[dup_id]

                # Skip empty groups
                if not emails_list:
                    continue

                # Sort emails within the group by date (latest first)
                emails_list.sort(key=lambda x: x["date"], reverse=True)

                first_email = emails_list[0]  # Latest email for last_received

                # Find oldest email for group-level info
                oldest_email = min(emails_list, key=lambda x: x["date"])
                group_comments = oldest_email.get("group_comments", [])
                group_comment_count = len(group_comments) if group_comments else 0

                # Format emails array
                emails = []
                for email in emails_list:
                    individual_comments = email.get("comments", [])
                    comment_count = len(
                        individual_comments) + group_comment_count if individual_comments else group_comment_count

                    emails.append({
                        "id": email["id"],
                        "to": email["to"],
                        "created": self.format_datetime(email["date"], timezone),
                        "is_read": email.get("read", "false") == "true",
                        "summary": email.get("summary", "null"),
                        "to_nickname": email.get("nickname", ""),
                        "comment_count": comment_count,
                        "is_starred": email.get("is_starred", False),
                        "is_task_complete": email.get("is_task_complete", False)
                    })

                # Format flags from the oldest email
                flags = []
                flag_list = oldest_email.get("flags", []) if isinstance(oldest_email.get("flags", []), list) else []

                for flag in flag_list:
                    assigned_by_user = False
                    flag_user_mappings = oldest_email.get("flag_user_mapping", []) if isinstance(
                        oldest_email.get("flag_user_mapping", []), list) else []

                    for flag_mapping in flag_user_mappings:
                        if flag_mapping.get("flag_name") == flag:
                            flags.append({
                                "flag_id": flag_mapping.get("flag_id"),
                                "flag_name": flag,
                                "assigned_by": flag_mapping.get("assigned_by")
                            })
                            assigned_by_user = True
                            break

                    if not assigned_by_user:
                        flags.append({
                            "flag_id": None,
                            "flag_name": flag,
                            "assigned_by": None
                        })

                # Get starred status from oldest email
                is_starred = oldest_email.get("starred", "false") == "true"

                # Get task complete status from oldest email
                is_task_complete = oldest_email.get("is_task_complete", False)

                # Get deleted status from oldest email
                is_deleted = oldest_email.get("is_deleted", False)

                # Format assigned users from oldest email
                assigned_users = oldest_email.get("assigned_user_ids") if oldest_email.get("assigned_user_ids") else []

                # Create item in required format
                item = {
                    "from": oldest_email.get("from") if oldest_email.get("from") else "",
                    # Use from field from oldest email
                    "id": dup_id,
                    "subject": oldest_email.get("subject") if oldest_email.get("subject") else "",
                    # Use subject from oldest email
                    "is_starred": is_starred,  # Use starred status from oldest email
                    "is_task_complete": is_task_complete,  # Use task complete status from oldest email
                    "is_deleted": is_deleted,  # Use deleted status from oldest email
                    "last_received": self.format_datetime(first_email["date"], timezone),
                    # Still use latest date for sorting
                    "emails": emails,
                    "flags": flags,
                    "assigned_users": assigned_users,
                    "total_comment_count": group_comment_count
                }

                items.append(item)

            transform_duration = time.time() - start_transform_time
            print(f"Transform time: {transform_duration:.3f} seconds")

            total_duration = time.time() - overall_start_time
            print(f"Total execution time: {total_duration:.3f} seconds")
            return {"total": total_count, "items": items}
        finally:
            if self.client:
                await self.client.close()

    # Helper function to format datetime with timezone
    def format_datetime(self, date_str, timezone):
        dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
        tz = pytz.timezone(timezone)
        dt = dt.astimezone(tz)
        return dt.strftime("%Y-%m-%dT%H:%M:%S")

    async def update_email_user_assignment(
            self,
            email_group_id: str,
            assigned_user_emails: List[str]
    ):
        async with get_async_opensearch_client() as client:
            update_body = {
                "query": {
                    "term": {"duplication_id": email_group_id}
                },
                "script": {
                    "source": """
                        ctx._source.assigned_user_ids = params.assigned_user_emails;
                    """,
                    "lang": "painless",
                    "params": {
                        "assigned_user_emails": assigned_user_emails
                    }
                }
            }

            response = await client.update_by_query(
                index="emails",
                body=update_body,
                refresh=True
            )

            invalidate_cache(CACHE_KEY_PATTERN)
            return response

    async def update_email_starred(
            self,
            duplication_id: str
    ):
        async with get_async_opensearch_client() as client:
            update_body = {
                "query": {
                    "term": {"duplication_id": duplication_id}
                },
                "script": {
                    "source": """
                    if (!ctx._source.containsKey('starred')) {
                        ctx._source.starred = "true";
                    } else {
                        ctx._source.starred = (ctx._source.starred == "true") ? "false" : "true";
                    }
                """,
                    "lang": "painless"
                }
            }

            response = await client.update_by_query(
                index="emails",
                body=update_body,
                refresh=True
            )

            invalidate_cache(CACHE_KEY_PATTERN)
            return response

    async def update_email_task_complete(
            self,
            duplication_id: str
    ):
        async with get_async_opensearch_client() as client:
            update_body = {
                "query": {
                    "term": {"duplication_id": duplication_id}
                },
                "script": {
                    "source": """
                    if (!ctx._source.containsKey('is_task_complete')) {
                        ctx._source.is_task_complete = true;
                    } else {
                        ctx._source.is_task_complete = !ctx._source.is_task_complete;
                    }
                """,
                    "lang": "painless"
                }
            }

            response = await client.update_by_query(
                index="emails",
                body=update_body,
                refresh=True
            )

            invalidate_cache(CACHE_KEY_PATTERN)
            return response

    async def update_individual_email_starred_opensearch(
            self,
            email_id: str,
            is_starred: bool
    ):
        """
        Update star status in OpenSearch for a specific individual email by email_id.
        This method sets the is_starred field to a specific boolean value.
        """
        async with get_async_opensearch_client() as client:
            update_body = {
                "query": {
                    "term": {"id.keyword": email_id}
                },
                "script": {
                    "source": f"ctx._source.is_starred = {str(is_starred).lower()}",
                    "lang": "painless"
                }
            }

            response = await client.update_by_query(
                index="emails",
                body=update_body,
                refresh=True
            )

            invalidate_cache(CACHE_KEY_PATTERN)
            return response

    async def update_individual_email_task_complete_opensearch(
            self,
            email_id: str,
            is_task_complete: bool
    ):
        """
        Update task completion status in OpenSearch for a specific individual email by email_id.
        This method sets the is_task_complete field to a specific boolean value.
        """
        async with get_async_opensearch_client() as client:
            update_body = {
                "query": {
                    "term": {"id.keyword": email_id}
                },
                "script": {
                    "source": f"ctx._source.is_task_complete = {str(is_task_complete).lower()}",
                    "lang": "painless"
                }
            }

            response = await client.update_by_query(
                index="emails",
                body=update_body,
                refresh=True
            )

            invalidate_cache(CACHE_KEY_PATTERN)
            return response

    async def add_comment_to_email(
            self,
            email_id: str,
            text: str,
            user_email: str
    ):
        comment_id = uuid.uuid4().hex
        current_time = datetime.now(timezone.utc).isoformat()

        new_comment = {
            "id": comment_id,
            "text": text,
            "author": user_email,
            "created_at": current_time
        }

        async with get_async_opensearch_client() as client:
            update_body = {
                "query": {
                    "term": {"id.keyword": email_id}
                },
                "script": {
                    "source": """
                            if (!ctx._source.containsKey('comments') || ctx._source.comments == null) {
                                ctx._source.comments = [];
                            }
                            ctx._source.comments.add(params.new_comment);
                        """,
                    "lang": "painless",
                    "params": {
                        "new_comment": new_comment
                    }
                }
            }

            response = await client.update_by_query(
                index="emails",
                body=update_body,
                refresh=True
            )

            return response

    async def add_comment_to_email_group(
            self,
            duplication_id: str,
            text: str,
            user_email: str
    ):
        comment_id = uuid.uuid4().hex
        current_time = datetime.now(timezone.utc).isoformat()

        new_comment = {
            "id": comment_id,
            "text": text,
            "author": user_email,
            "created_at": current_time
        }

        async with get_async_opensearch_client() as client:
            query = {
                "query": {
                    "term": {"duplication_id": duplication_id}
                },
                "sort": [
                    {"date": {"order": "asc"}},
                    {"id.keyword": {"order": "asc"}}
                ],
                "size": 1
            }

            search_response = await client.search(
                index="emails",
                body=query
            )

            if search_response["hits"]["total"]["value"] == 0:
                raise HTTPException(
                    status_code=404,
                    detail=f"No emails found with duplication_id {duplication_id}"
                )

            oldest_email_id = search_response["hits"]["hits"][0]["_id"]

            update_body = {
                "script": {
                    "source": """
                        if (!ctx._source.containsKey('group_comments') || ctx._source.group_comments == null) {
                            ctx._source.group_comments = [];
                        }
                         ctx._source.group_comments.add(params.new_comment);
                    """,
                    "lang": "painless",
                    "params": {
                        "new_comment": new_comment
                    }
                }
            }

            response = await client.update(
                index="emails",
                id=oldest_email_id,
                body=update_body,
                refresh=True
            )

            return response

    async def get_email_content(self, email_id: str):
        async with get_async_opensearch_client() as client:
            response = await client.get(index="emails", id=email_id)
            email_body = response['_source'].get("body_html")

            if not email_body:
                body_html_clean: str = response['_source'].get("body_html_clean")
                body_plain: str = response['_source'].get("body_plain")
                return body_html_clean if body_html_clean and body_html_clean.strip() else body_plain

            if not self.is_html_document(email_body):
                try:
                    decoded_bytes = base64.b64decode(email_body)
                    encodings_to_try = ['utf-8', 'iso-8859-1', 'windows-1252', 'latin1']
                    for encoding in encodings_to_try:
                        try:
                            decoded_str = decoded_bytes.decode(encoding)
                            return decoded_str
                        except UnicodeDecodeError:
                            continue

                    decoded_str = decoded_bytes.decode('utf-8', errors='replace')
                    return decoded_str
                except Exception as e:
                    print(f"EmailID: {email_id} \nBase64 decoding error: {e}")

            return email_body

    async def find_flags_from_pg(self, flags: List[str]) -> List[dict]:
        values = {"flag_names": [f.strip().lower() for f in flags if f and f.strip()]}
        sql = """
              SELECT flag_id, flag_name from email.flag
              WHERE LOWER(flag_name) = ANY(:flag_names)
              ORDER BY flag_name 
              """
        res = await get_pg_database().fetch_all(sql, values)
        return [dict(r) for r in res]

    async def update_email_flags_unified(
            self,
            duplication_ids: List[str],
            flags: List[dict],
            user_email: str,
            replace_flags: bool = True
    ):
        """
        Unified function to update email flags for both single and bulk operations.
        
        Args:
            duplication_ids: List of email duplication IDs to update
            flags: List of flag dictionaries with flag_name and flag_id
            user_email: Email of the user making the update
            replace_flags: If True, replace all flags. If False, add flags to existing ones.
        """
        flag_names = [f["flag_name"] for f in flags]

        async with get_async_opensearch_client() as client:
            # Build query based on number of IDs
            if len(duplication_ids) == 1:
                query = {"term": {"duplication_id": duplication_ids[0]}}
            else:
                query = {"terms": {"duplication_id": duplication_ids}}

            update_body = {
                "query": query,
                "script": {
                    "source": """
                        // Check if archived flag was previously present
                        boolean hadArchivedFlag = false;
                        if (ctx._source.containsKey('flags') && ctx._source.flags != null) {
                            for (String existingFlag : ctx._source.flags) {
                                if (existingFlag.toLowerCase() == 'archived') {
                                    hadArchivedFlag = true;
                                    break;
                                }
                            }
                        }
                        
                        // Check if archived flag is in the new flags list
                        boolean hasArchivedFlag = false;
                        for (String flag : params.flag_names) {
                            if (flag.toLowerCase() == 'archived') {
                                hasArchivedFlag = true;
                                break;
                            }
                        }
                        
                        // Update flags based on replace_flags parameter
                        if (params.replace_flags) {
                            // Replace all flags (single endpoint behavior)
                            ctx._source.flags = params.flag_names;
                        } else {
                            // Add flags to existing ones (bulk endpoint behavior)
                            if (ctx._source.containsKey('flags') && ctx._source.flags != null) {
                                for (int i = 0; i < params.flag_names.length; i++) {
                                    if (!ctx._source.flags.contains(params.flag_names[i])) {
                                        ctx._source.flags.add(params.flag_names[i]);
                                    }
                                }
                            } else {
                                ctx._source.flags = params.flag_names;
                            }
                        }
                        
                        // Handle archived flag logic (same for both behaviors)
                        if (hasArchivedFlag) {
                            // Adding archived flag - mark as archived
                            ctx._source.archived = true;
                        } else if (hadArchivedFlag && !hasArchivedFlag) {
                            // Only unarchive if email meets ALL three conditions:
                            // 1. is_deleted = true
                            // 2. archived = true  
                            // 3. had archived flag
                            boolean isDeleted = ctx._source.containsKey('is_deleted') && ctx._source.is_deleted == true;
                            boolean isArchived = ctx._source.containsKey('archived') && ctx._source.archived == true;
                            
                            if (isDeleted && isArchived) {
                                // Removing archived flag - unarchive and undelete
                                ctx._source.archived = false;
                                ctx._source.is_deleted = false;
                            }
                        }

                        // Initialize flag_user_mapping if it doesn't exist
                        if (!ctx._source.containsKey('flag_user_mapping') || ctx._source.flag_user_mapping == null) {
                            ctx._source.flag_user_mapping = [];
                        }

                        // Create a mapping of existing flag_names to their positions for faster lookup
                        Map flagMap = new HashMap();
                        for (int j = 0; j < ctx._source.flag_user_mapping.length; j++) {
                            flagMap.put(ctx._source.flag_user_mapping[j].flag_name, j);
                        }

                        // Process each flag entry
                        List newFlagUserMapping = [];
                        long currentTime = new Date().getTime();
                        for (int i = 0; i < params.flags.length; i++) {
                            String flagName = params.flags[i].flag_name;

                            if (flagMap.containsKey(flagName)) {
                                int index = flagMap.get(flagName);
                                newFlagUserMapping.add(ctx._source.flag_user_mapping[index]);
                            } else {
                                Map newMapping = new HashMap();
                                newMapping.put("flag_name", flagName);
                                newMapping.put("flag_id", params.flags[i].flag_id);
                                newMapping.put("assigned_by", params.user_email);
                                newMapping.put("created_at", currentTime);
                                newFlagUserMapping.add(newMapping);
                            }
                        }

                        // Update flag_user_mapping based on replace_flags parameter
                        if (params.replace_flags) {
                            // Replace all mappings (single endpoint behavior)
                            ctx._source.flag_user_mapping = newFlagUserMapping;
                        } else {
                            // Add new mappings to existing ones (bulk endpoint behavior)
                            for (int i = 0; i < newFlagUserMapping.length; i++) {
                                String flagName = newFlagUserMapping[i].flag_name;
                                boolean exists = false;
                                
                                // Check if this flag already exists in the mapping
                                for (int j = 0; j < ctx._source.flag_user_mapping.length; j++) {
                                    if (ctx._source.flag_user_mapping[j].flag_name == flagName) {
                                        exists = true;
                                        break;
                                    }
                                }
                                
                                if (!exists) {
                                    ctx._source.flag_user_mapping.add(newFlagUserMapping[i]);
                                }
                            }
                        }
                    """,
                    "lang": "painless",
                    "params": {
                        "flag_names": flag_names,
                        "flags": flags,
                        "user_email": user_email,
                        "replace_flags": replace_flags
                    }
                }
            }

            response = await client.update_by_query(
                index="emails",
                body=update_body,
                refresh=True
            )

            invalidate_cache(CACHE_KEY_PATTERN)
            return response

    async def update_email_flags(
            self,
            duplication_id: str,
            flags: List[dict],
            user_email: str
    ):
        """Single email flags update - replaces all flags"""
        return await self.update_email_flags_unified(
            duplication_ids=[duplication_id],
            flags=flags,
            user_email=user_email,
            replace_flags=True
        )

    async def update_email_flags_bulk(
            self,
            duplication_ids: List[str],
            flags: List[dict],
            user_email: str
    ):
        """Bulk email flags update - adds flags to existing ones"""
        if not duplication_ids or not flags:
            raise HTTPException(status_code=400, detail="At least one duplication_id and flag must be provided!")
        
        return await self.update_email_flags_unified(
            duplication_ids=duplication_ids,
            flags=flags,
            user_email=user_email,
            replace_flags=False
        )

    async def validate_and_update_email_flags(
            self,
            duplication_id: str,
            flags: List[str],
            user_email: str
    ):
        start_time = time.time()
        valid_flags = await self.find_flags_from_pg(flags)
        print(f"Finding flags completed in {time.time() - start_time:.2f}s")
        if len(valid_flags) != len(flags):
            valid_flags_set = {f["flag_name"] for f in valid_flags}
            raise HTTPException(
                status_code=400,
                detail=f"These flags were not found: '{', '.join(valid_flags_set.difference(flags))}'"
            )
        return await self.update_email_flags(duplication_id, valid_flags, user_email)

    async def validate_and_update_email_flags_bulk(
            self,
            duplication_ids: List[str],
            flags: List[str],
            user_email: str
    ):
        start_time = time.time()
        valid_flags = await self.find_flags_from_pg(flags)
        print(f"Finding flags completed in {time.time() - start_time:.2f}s")
        if len(valid_flags) != len(flags):
            valid_flags_set = {f["flag_name"] for f in valid_flags}
            raise HTTPException(
                status_code=400,
                detail=f"These flags were not found: '{', '.join(valid_flags_set.difference(flags))}'"
            )

        return await self.update_email_flags_bulk(duplication_ids, valid_flags, user_email)

    async def delete_emails(
            self,
            email_group_ids: List[str]
    ):
        async with get_async_opensearch_client() as client:
            update_body = {
                "query": {
                    "terms": {"duplication_id": email_group_ids}
                },
                "script": {
                    "source": """
                    // Mark as deleted
                    ctx._source.is_deleted = true;
                    
                    // Add archived flag to flags array
                    if (!ctx._source.containsKey('flags') || ctx._source.flags == null) {
                        ctx._source.flags = [];
                    }
                    if (!ctx._source.flags.contains('archived')) {
                        ctx._source.flags.add('archived');
                    }
                    
                    // Set archived field to true
                    ctx._source.archived = true;
                    
                    // Initialize flag_user_mapping if it doesn't exist
                    if (!ctx._source.containsKey('flag_user_mapping') || ctx._source.flag_user_mapping == null) {
                        ctx._source.flag_user_mapping = [];
                    }
                    
                    // Check if archived flag mapping already exists
                    boolean archivedMappingExists = false;
                    for (int i = 0; i < ctx._source.flag_user_mapping.length; i++) {
                        if (ctx._source.flag_user_mapping[i].flag_name == 'archived') {
                            archivedMappingExists = true;
                            break;
                        }
                    }
                    
                    // Add archived flag mapping if it doesn't exist
                    if (!archivedMappingExists) {
                        Map archivedMapping = new HashMap();
                        archivedMapping.put("flag_name", "archived");
                        archivedMapping.put("flag_id", null);
                        archivedMapping.put("assigned_by", "system");
                        archivedMapping.put("created_at", new Date().getTime());
                        ctx._source.flag_user_mapping.add(archivedMapping);
                    }
                """,
                    "lang": "painless"
                }
            }

            response = await client.update_by_query(
                index="emails",
                body=update_body,
                refresh=True
            )

            invalidate_cache(CACHE_KEY_PATTERN)
            return response

    async def update_email_read(
            self,
            email_id: str,
            is_read: bool
    ):
        async with get_async_opensearch_client() as client:
            update_body = {
                "query": {
                    "term": {"id.keyword": email_id}
                },
                "script": {
                    "source": """
                        ctx._source.read = params.is_read ? "true" : "false";
                    """,
                    "lang": "painless",
                    "params": {
                        "is_read": is_read
                    }
                }
            }

            response = await client.update_by_query(
                index="emails",
                body=update_body,
                refresh=True
            )

            invalidate_cache(CACHE_KEY_PATTERN)
            return response

    async def get_email_applied_filters(self, duplication_id: str):
        query = """
                SELECT 
                    af.filter_id as "filter_id",
                    efi.archive as "archive",
                    efi.mark_as_read as "mark_as_read", 
                    efi.star as "star",
                    efi.add_comment as "add_comment",
                    efi.flags as "flags",
                    efi.users as "users",
                    efi."from" as "from",
                    efi."to" as "to",
                    efi.subject as "subject",
                    efi.does_not_have as "does_not_have",
                    efi.search_term as "search_term"
                FROM email.applied_filter af
                LEFT JOIN email_filter efi 
                ON efi.id = af.filter_id 
                WHERE af.email_duplication_id = :duplication_id
              """
        values = {"duplication_id": duplication_id}
        try:
            rows = await get_pg_database().fetch_all(query, values)
            return [dict(row) for row in rows] if rows else []
        except Exception as e:
            print(f"Failed to fetch comments: {e}")
            traceback.print_exc()
            raise HTTPException(status_code=500, detail=f"An error occurred: {e}")

    async def forward_email(self, email_id: str, forward_to: List[str]) -> str:
        """
        Forward an email to specified recipients using AWS SES SendRawEmail.
        
        Args:
            email_id: The ID of the email to forward
            forward_to: List of email addresses to forward to
            
        Returns:
            Message ID from SES
            
        Raises:
            HTTPException: With detailed error information if forwarding fails
        """
        import os
        
        logger.info(f"Starting email forward request for email_id: {email_id}, recipients: {forward_to}")
        
        # Validate inputs
        if not email_id or not email_id.strip():
            error_msg = "Email ID is required and cannot be empty"
            logger.error(error_msg)
            raise HTTPException(status_code=400, detail=error_msg)
        
        if not forward_to or len(forward_to) == 0:
            error_msg = "At least one recipient email address is required"
            logger.error(error_msg)
            raise HTTPException(status_code=400, detail=error_msg)
        
        # Validate email addresses
        email_pattern = re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')
        invalid_emails = [email for email in forward_to if not email_pattern.match(email)]
        if invalid_emails:
            error_msg = f"Invalid email address(es): {', '.join(invalid_emails)}"
            logger.error(error_msg)
            raise HTTPException(status_code=400, detail=error_msg)
        
        # Get email content from OpenSearch
        try:
            logger.info(f"Fetching email from OpenSearch with id: {email_id}")
            async with get_async_opensearch_client() as client:
                try:
                    response = await client.get(index="emails", id=email_id)
                    email_source = response.get('_source', {})
                    if not email_source:
                        error_msg = f"Email found but source data is empty for id: {email_id}"
                        logger.error(error_msg)
                        raise HTTPException(status_code=404, detail=error_msg)
                    logger.info(f"Successfully retrieved email from OpenSearch")
                except Exception as e:
                    error_msg = f"Failed to retrieve email from OpenSearch: {str(e)}"
                    logger.error(f"{error_msg}. Traceback: {traceback.format_exc()}")
                    if isinstance(e, HTTPException):
                        raise
                    raise HTTPException(status_code=404, detail=f"Email not found with id {email_id}: {str(e)}")
        except HTTPException:
            raise
        except Exception as e:
            error_msg = f"Unexpected error connecting to OpenSearch: {str(e)}"
            logger.error(f"{error_msg}. Traceback: {traceback.format_exc()}")
            raise HTTPException(status_code=500, detail=error_msg)
        
        # Extract email fields
        try:
            from_email = email_source.get("from", "")
            to_email = email_source.get("to", "")
            subject = email_source.get("subject", "")
            date_str = email_source.get("date", "")
            body_html = email_source.get("body_html", "")
            body_plain = email_source.get("body_plain", "")
            body_html_clean = email_source.get("body_html_clean", "")
            
            logger.info(f"Extracted email fields - From: {from_email}, Subject: {subject[:50] if subject else 'N/A'}")
        except Exception as e:
            error_msg = f"Failed to extract email fields: {str(e)}"
            logger.error(f"{error_msg}. Traceback: {traceback.format_exc()}")
            raise HTTPException(status_code=500, detail=error_msg)
        
        # Decode quoted-printable encoding from all body fields
        try:
            body_html = self.decode_quoted_printable(body_html)
            body_plain = self.decode_quoted_printable(body_plain)
            body_html_clean = self.decode_quoted_printable(body_html_clean)
            logger.debug("Decoded quoted-printable encoding from email body fields")
        except Exception as e:
            logger.warning(f"Error decoding quoted-printable encoding: {str(e)}. Continuing with original content.")
        
        # Get forwarder email from environment variable
        forwarder_from_email = os.getenv("FORWARDER_FROM_EMAIL", "forwarder@tb-portal.com")
        if not forwarder_from_email:
            error_msg = "FORWARDER_FROM_EMAIL environment variable is not set"
            logger.error(error_msg)
            raise HTTPException(status_code=500, detail=error_msg)
        
        logger.info(f"Using forwarder email: {forwarder_from_email}")
        
        # Get email body content
        email_body = body_html
        if not email_body:
            email_body = body_html_clean if body_html_clean and body_html_clean.strip() else body_plain
        
        if not email_body:
            logger.warning("No email body content found (body_html, body_html_clean, and body_plain are all empty)")
        
        # Decode base64 if needed
        if email_body and not self.is_html_document(email_body):
            try:
                decoded_bytes = base64.b64decode(email_body)
                encodings_to_try = ['utf-8', 'iso-8859-1', 'windows-1252', 'latin1']
                for encoding in encodings_to_try:
                    try:
                        email_body = decoded_bytes.decode(encoding)
                        logger.debug(f"Successfully decoded base64 email body using {encoding} encoding")
                        break
                    except UnicodeDecodeError:
                        continue
                else:
                    email_body = decoded_bytes.decode('utf-8', errors='replace')
                    logger.debug("Decoded base64 email body using UTF-8 with error replacement")
            except Exception as e:
                logger.warning(f"Failed to decode base64 email body: {str(e)}. Using original content.")
        
        # Create the forwarded email message
        try:
            msg = MIMEMultipart('alternative')
            msg['From'] = forwarder_from_email
            msg['To'] = ', '.join(forward_to)
            msg['Subject'] = f"Fwd: {subject}" if subject else "Fwd: (No Subject)"
            msg['Date'] = formatdate(localtime=True)
            
            # Add original email headers as reference
            original_headers = []
            if from_email:
                original_headers.append(f"From: {from_email}")
            if to_email:
                original_headers.append(f"To: {to_email}")
            if date_str:
                original_headers.append(f"Date: {date_str}")
            if subject:
                original_headers.append(f"Subject: {subject}")
            
            # Create the forwarded message body
            forward_text = "---------- Forwarded message ----------\n"
            forward_text += "\n".join(original_headers)
            forward_text += "\n\n"
            
            # Add plain text version
            if body_plain:
                forward_text += body_plain
            else:
                # Strip HTML tags for plain text
                plain_text = re.sub(r'<[^>]+>', '', email_body if email_body else '')
                forward_text += plain_text
            
            # Add HTML version - escape HTML in email_body to prevent injection
            email_body_safe = email_body if email_body else body_plain
            forward_html = f"""
            <html>
            <body>
            <p>---------- Forwarded message ----------</p>
            <p><strong>From:</strong> {from_email or '(Unknown)'}<br>
            <strong>To:</strong> {to_email or '(Unknown)'}<br>
            <strong>Date:</strong> {date_str or '(Unknown)'}<br>
            <strong>Subject:</strong> {subject or '(No Subject)'}</p>
            <hr>
            {email_body_safe if email_body_safe else '(No content)'}
            </body>
            </html>
            """
            
            # Attach both plain and HTML versions
            msg.attach(MIMEText(forward_text, 'plain'))
            msg.attach(MIMEText(forward_html, 'html'))
            
            logger.info("Successfully created MIME message")
        except Exception as e:
            error_msg = f"Failed to create email message: {str(e)}"
            logger.error(f"{error_msg}. Traceback: {traceback.format_exc()}")
            raise HTTPException(status_code=500, detail=error_msg)
        
        # Get SES client and send email
        try:
            logger.info(f"Initializing SES client and preparing to send email to: {forward_to}")
            ses_client = get_ses_client()
            
            if not ses_client:
                error_msg = "Failed to initialize SES client"
                logger.error(error_msg)
                raise HTTPException(status_code=500, detail=error_msg)
            
            # Convert message to string
            try:
                message_string = msg.as_string()
                logger.debug(f"Message size: {len(message_string)} bytes")
            except Exception as e:
                error_msg = f"Failed to convert message to string: {str(e)}"
                logger.error(f"{error_msg}. Traceback: {traceback.format_exc()}")
                raise HTTPException(status_code=500, detail=error_msg)
            
            # Send email via SES
            try:
                logger.info(f"Sending email via SES from {forwarder_from_email} to {forward_to}")
                response = ses_client.send_raw_email(
                    Source=forwarder_from_email,
                    Destinations=forward_to,
                    RawMessage={
                        'Data': message_string
                    }
                )
                
                message_id = response.get('MessageId', 'Unknown')
                logger.info(f"Email sent successfully via SES. MessageId: {message_id}")
                return message_id
                
            except Exception as e:
                error_type = type(e).__name__
                error_msg = str(e)
                
                # Handle specific SES errors
                if 'MessageRejected' in error_type or 'MessageRejected' in error_msg:
                    detailed_error = f"SES rejected the email: {error_msg}. Check recipient email addresses and SES configuration."
                    logger.error(f"{detailed_error}. Traceback: {traceback.format_exc()}")
                    raise HTTPException(status_code=400, detail=detailed_error)
                elif 'InvalidParameterValue' in error_type or 'InvalidParameterValue' in error_msg:
                    detailed_error = f"Invalid parameter in email: {error_msg}. Check email format and content."
                    logger.error(f"{detailed_error}. Traceback: {traceback.format_exc()}")
                    raise HTTPException(status_code=400, detail=detailed_error)
                elif 'AccessDenied' in error_type or 'AccessDenied' in error_msg:
                    detailed_error = f"SES access denied: {error_msg}. Check AWS credentials and SES permissions."
                    logger.error(f"{detailed_error}. Traceback: {traceback.format_exc()}")
                    raise HTTPException(status_code=403, detail=detailed_error)
                else:
                    detailed_error = f"Failed to send email via SES: {error_msg}"
                    logger.error(f"{detailed_error}. Traceback: {traceback.format_exc()}")
                    raise HTTPException(status_code=500, detail=detailed_error)
                    
        except HTTPException:
            raise
        except Exception as e:
            error_msg = f"Unexpected error during email sending: {str(e)}"
            logger.error(f"{error_msg}. Traceback: {traceback.format_exc()}")
            raise HTTPException(status_code=500, detail=error_msg)

    async def forward_emails(self, email_ids: List[str], forward_to: List[str]) -> List[str]:
        """
        Forward multiple emails to specified recipients using AWS SES SendRawEmail.
        Optimized to fetch all emails in a single OpenSearch mget call.
        
        Args:
            email_ids: List of email IDs to forward
            forward_to: List of email addresses to forward to
            
        Returns:
            List of message IDs from SES (one per email forwarded)
            
        Raises:
            HTTPException: With detailed error information if forwarding fails
        """
        import os
        
        logger.info(f"Starting bulk email forward request for {len(email_ids)} email(s), recipients: {forward_to}")
        
        # Validate inputs
        if not email_ids or len(email_ids) == 0:
            error_msg = "At least one email ID is required"
            logger.error(error_msg)
            raise HTTPException(status_code=400, detail=error_msg)
        
        # Validate email_ids are not empty
        email_ids = [eid.strip() for eid in email_ids if eid and eid.strip()]
        if not email_ids:
            error_msg = "All email IDs are empty or invalid"
            logger.error(error_msg)
            raise HTTPException(status_code=400, detail=error_msg)
        
        if not forward_to or len(forward_to) == 0:
            error_msg = "At least one recipient email address is required"
            logger.error(error_msg)
            raise HTTPException(status_code=400, detail=error_msg)
        
        # Validate email addresses
        email_pattern = re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')
        invalid_emails = [email for email in forward_to if not email_pattern.match(email)]
        if invalid_emails:
            error_msg = f"Invalid email address(es): {', '.join(invalid_emails)}"
            logger.error(error_msg)
            raise HTTPException(status_code=400, detail=error_msg)
        
        # Get forwarder email from environment variable (do this once)
        forwarder_from_email = os.getenv("FORWARDER_FROM_EMAIL", "forwarder@tb-portal.com")
        if not forwarder_from_email:
            error_msg = "FORWARDER_FROM_EMAIL environment variable is not set"
            logger.error(error_msg)
            raise HTTPException(status_code=500, detail=error_msg)
        
        logger.info(f"Using forwarder email: {forwarder_from_email}")
        
        # Get all emails from OpenSearch using mget (bulk fetch - optimized)
        emails_data = {}
        try:
            logger.info(f"Fetching {len(email_ids)} email(s) from OpenSearch using mget")
            async with get_async_opensearch_client() as client:
                try:
                    # Use mget for efficient bulk retrieval
                    # mget API: body can have "ids" or "docs" array
                    response = await client.mget(
                        body={"ids": email_ids},
                        index="emails"
                    )
                    
                    # Process mget response
                    for doc in response.get('docs', []):
                        if doc.get('found', False):
                            email_id = doc.get('_id')
                            email_source = doc.get('_source', {})
                            if email_source:
                                emails_data[email_id] = email_source
                            else:
                                logger.warning(f"Email {email_id} found but source data is empty")
                        else:
                            email_id = doc.get('_id', 'unknown')
                            logger.warning(f"Email not found with id: {email_id}")
                    
                    if not emails_data:
                        error_msg = f"None of the requested emails were found. Requested IDs: {email_ids}"
                        logger.error(error_msg)
                        raise HTTPException(status_code=404, detail=error_msg)
                    
                    logger.info(f"Successfully retrieved {len(emails_data)} email(s) from OpenSearch")
                except HTTPException:
                    raise
                except Exception as e:
                    error_msg = f"Failed to retrieve emails from OpenSearch: {str(e)}"
                    logger.error(f"{error_msg}. Traceback: {traceback.format_exc()}")
                    raise HTTPException(status_code=500, detail=error_msg)
        except HTTPException:
            raise
        except Exception as e:
            error_msg = f"Unexpected error connecting to OpenSearch: {str(e)}"
            logger.error(f"{error_msg}. Traceback: {traceback.format_exc()}")
            raise HTTPException(status_code=500, detail=error_msg)
        
        # Get SES client once (reuse for all emails)
        try:
            ses_client = get_ses_client()
            if not ses_client:
                error_msg = "Failed to initialize SES client"
                logger.error(error_msg)
                raise HTTPException(status_code=500, detail=error_msg)
        except Exception as e:
            error_msg = f"Failed to initialize SES client: {str(e)}"
            logger.error(f"{error_msg}. Traceback: {traceback.format_exc()}")
            raise HTTPException(status_code=500, detail=error_msg)
        
        # Process and forward each email
        message_ids = []
        failed_emails = []
        
        for email_id, email_source in emails_data.items():
            try:
                # Extract email fields
                from_email = email_source.get("from", "")
                to_email = email_source.get("to", "")
                subject = email_source.get("subject", "")
                date_str = email_source.get("date", "")
                body_html = email_source.get("body_html", "")
                body_plain = email_source.get("body_plain", "")
                body_html_clean = email_source.get("body_html_clean", "")
                
                # Decode quoted-printable encoding from all body fields
                try:
                    body_html = self.decode_quoted_printable(body_html)
                    body_plain = self.decode_quoted_printable(body_plain)
                    body_html_clean = self.decode_quoted_printable(body_html_clean)
                except Exception as e:
                    logger.warning(f"Error decoding quoted-printable for email {email_id}: {str(e)}. Continuing with original content.")
                
                # Get email body content
                email_body = body_html
                if not email_body:
                    email_body = body_html_clean if body_html_clean and body_html_clean.strip() else body_plain
                
                if not email_body:
                    logger.warning(f"No email body content found for email {email_id}")
                
                # Decode base64 if needed
                if email_body and not self.is_html_document(email_body):
                    try:
                        decoded_bytes = base64.b64decode(email_body)
                        encodings_to_try = ['utf-8', 'iso-8859-1', 'windows-1252', 'latin1']
                        for encoding in encodings_to_try:
                            try:
                                email_body = decoded_bytes.decode(encoding)
                                break
                            except UnicodeDecodeError:
                                continue
                        else:
                            email_body = decoded_bytes.decode('utf-8', errors='replace')
                    except Exception as e:
                        logger.warning(f"Failed to decode base64 email body for {email_id}: {str(e)}. Using original content.")
                
                # Create the forwarded email message
                msg = MIMEMultipart('alternative')
                msg['From'] = forwarder_from_email
                msg['To'] = ', '.join(forward_to)
                msg['Subject'] = f"Fwd: {subject}" if subject else "Fwd: (No Subject)"
                msg['Date'] = formatdate(localtime=True)
                
                # Add original email headers as reference
                original_headers = []
                if from_email:
                    original_headers.append(f"From: {from_email}")
                if to_email:
                    original_headers.append(f"To: {to_email}")
                if date_str:
                    original_headers.append(f"Date: {date_str}")
                if subject:
                    original_headers.append(f"Subject: {subject}")
                
                # Create the forwarded message body
                forward_text = "---------- Forwarded message ----------\n"
                forward_text += "\n".join(original_headers)
                forward_text += "\n\n"
                
                # Add plain text version
                if body_plain:
                    forward_text += body_plain
                else:
                    # Strip HTML tags for plain text
                    plain_text = re.sub(r'<[^>]+>', '', email_body if email_body else '')
                    forward_text += plain_text
                
                # Add HTML version
                email_body_safe = email_body if email_body else body_plain
                forward_html = f"""
                <html>
                <body>
                <p>---------- Forwarded message ----------</p>
                <p><strong>From:</strong> {from_email or '(Unknown)'}<br>
                <strong>To:</strong> {to_email or '(Unknown)'}<br>
                <strong>Date:</strong> {date_str or '(Unknown)'}<br>
                <strong>Subject:</strong> {subject or '(No Subject)'}</p>
                <hr>
                {email_body_safe if email_body_safe else '(No content)'}
                </body>
                </html>
                """
                
                # Attach both plain and HTML versions
                msg.attach(MIMEText(forward_text, 'plain'))
                msg.attach(MIMEText(forward_html, 'html'))
                
                # Convert message to string
                message_string = msg.as_string()
                
                # Send email via SES
                logger.info(f"Sending email {email_id} via SES from {forwarder_from_email} to {forward_to}")
                response = ses_client.send_raw_email(
                    Source=forwarder_from_email,
                    Destinations=forward_to,
                    RawMessage={
                        'Data': message_string
                    }
                )
                
                message_id = response.get('MessageId', 'Unknown')
                message_ids.append(message_id)
                logger.info(f"Email {email_id} sent successfully via SES. MessageId: {message_id}")
                
            except Exception as e:
                error_type = type(e).__name__
                error_msg = str(e)
                
                # Handle specific SES errors
                if 'MessageRejected' in error_type or 'MessageRejected' in error_msg:
                    detailed_error = f"SES rejected email {email_id}: {error_msg}"
                    logger.error(f"{detailed_error}. Traceback: {traceback.format_exc()}")
                    failed_emails.append({"email_id": email_id, "error": detailed_error})
                elif 'InvalidParameterValue' in error_type or 'InvalidParameterValue' in error_msg:
                    detailed_error = f"Invalid parameter in email {email_id}: {error_msg}"
                    logger.error(f"{detailed_error}. Traceback: {traceback.format_exc()}")
                    failed_emails.append({"email_id": email_id, "error": detailed_error})
                elif 'AccessDenied' in error_type or 'AccessDenied' in error_msg:
                    detailed_error = f"SES access denied for email {email_id}: {error_msg}"
                    logger.error(f"{detailed_error}. Traceback: {traceback.format_exc()}")
                    failed_emails.append({"email_id": email_id, "error": detailed_error})
                else:
                    detailed_error = f"Failed to forward email {email_id}: {error_msg}"
                    logger.error(f"{detailed_error}. Traceback: {traceback.format_exc()}")
                    failed_emails.append({"email_id": email_id, "error": detailed_error})
        
        # If all emails failed, raise an error
        if len(message_ids) == 0 and len(failed_emails) > 0:
            error_details = "; ".join([f"{fe['email_id']}: {fe['error']}" for fe in failed_emails])
            raise HTTPException(
                status_code=500,
                detail=f"Failed to forward all emails. Errors: {error_details}"
            )
        
        # If some emails failed, log warning but return success for successful ones
        if len(failed_emails) > 0:
            logger.warning(f"Some emails failed to forward: {failed_emails}")
        
        logger.info(f"Successfully forwarded {len(message_ids)} out of {len(emails_data)} email(s)")
        return message_ids

    @staticmethod
    def is_html_document(text: str) -> bool:
        if not text or not text.strip():
            return False

        common_tags_pattern = re.compile(
            r'<(?:head|body|div|p|span|table|a|img|script|link|meta)\b[^>]*>',
            re.IGNORECASE
        )
        if common_tags_pattern.search(text):
            return True

        multiple_tags_pattern = re.compile(r'<[a-z][^>]*>.*?</[a-z][^>]*>', re.IGNORECASE | re.DOTALL)
        tag_matches = multiple_tags_pattern.findall(text)
        if len(tag_matches) >= 2:
            return True

        return False

    @staticmethod
    def decode_quoted_printable(text: str) -> str:
        """
        Decode quoted-printable encoded text.
        Quoted-printable encoding uses =XX format where XX is hex code.
        Common patterns: =3D for =, =20 for space, =0A for newline, etc.
        """
        if not text:
            return text
        
        try:
            # Check if text contains quoted-printable patterns (e.g., =3D, =20, =0A)
            if '=' in text and re.search(r'=[0-9A-Fa-f]{2}', text):
                # Convert string to bytes for quopri decoding
                # Try UTF-8 first, fallback to latin-1 if needed
                try:
                    text_bytes = text.encode('utf-8')
                except UnicodeEncodeError:
                    text_bytes = text.encode('latin-1', errors='ignore')
                
                # Decode quoted-printable
                decoded_bytes = quopri.decodestring(text_bytes)
                
                # Try to decode bytes back to string
                # Try UTF-8 first, then latin-1, then ISO-8859-1
                encodings_to_try = ['utf-8', 'latin-1', 'iso-8859-1', 'windows-1252']
                for encoding in encodings_to_try:
                    try:
                        return decoded_bytes.decode(encoding)
                    except UnicodeDecodeError:
                        continue
                
                # If all encodings fail, use replace mode
                return decoded_bytes.decode('utf-8', errors='replace')
        except Exception:
            # If decoding fails, return original text
            pass
        
        return text

    async def update_email_user_assignment_bulk(
            self,
            email_group_ids: List[str],
            assigned_user_emails: List[str]
    ):
        """
        Bulk update assigned users for multiple email groups in OpenSearch.
        Replaces all existing assignments for each email group with the new user_ids.
        """
        async with get_async_opensearch_client() as client:
            update_body = {
                "query": {
                    "terms": {"duplication_id.keyword": email_group_ids}
                },
                "script": {
                    "source": """
                        ctx._source.assigned_user_ids = params.assigned_user_emails;
                    """,
                    "lang": "painless",
                    "params": {
                        "assigned_user_emails": assigned_user_emails
                    }
                }
            }

            response = await client.update_by_query(
                index="emails",
                body=update_body,
                refresh=True
            )

            invalidate_cache(CACHE_KEY_PATTERN)
            return response