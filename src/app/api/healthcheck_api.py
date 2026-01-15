import logging
from os import environ
from typing import Any, Dict

from opensearchpy import OpenSearch
from fastapi import APIRouter, status, Query

router = APIRouter()


@router.get("/healthcheck", status_code=status.HTTP_200_OK)
def healthcheck():
    logging.info("healthcheck")
    return {"healthcheck": "Everything is OK!"}


@router.post("/test-opensearch", status_code=status.HTTP_200_OK)
def search_opensearch(
        index: str = Query(default="email"),
        query: Dict[str, Any] = None
):
    logging.info("Initialize OpenSearch client")

    print("index:", type(index), index)
    print("query:", type(query), query)
    print("OPENSEARCH_ENDPOINT", type(environ["OPENSEARCH_ENDPOINT"]), environ["OPENSEARCH_ENDPOINT"])

    client = OpenSearch(
        hosts=[{'host': environ["OPENSEARCH_ENDPOINT"], 'port': 443}],
        use_ssl=True,
        verify_certs=True,
        ssl_assert_hostname=False,
        ssl_show_warn=False,
        timeout=30
    )

    response = client.search(index=index, body=query)
    print(response)
    return response
