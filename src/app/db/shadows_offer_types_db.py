import hashlib
import json
import os
import traceback

import boto3
from fastapi import HTTPException

from app.cache import invalidate_shadows_cache
from app.database import get_pg_realtime_catalog_database
from app.model.shadows_offer_types import ShadowsOfferTypesModel


async def get_all_offer_types(offer_filter, page, page_size, search_term, verified) -> dict:
    try:
        is_valid = ' 1=1 '
        verify_check = ' 2=2 '
        if offer_filter == 'valid':
            is_valid = 'valid=true'
        elif offer_filter == 'invalid':
            is_valid = 'valid=false'
        if verified:
            verify_check = ' valid is null '
        where = f''' where {is_valid} and {verify_check} and offer_type_name ilike '%{search_term}%' '''
        sql = f"""
            select 
                *
            from shadows_offer_types
            {where}
            order by offer_type_name
            limit {page_size}
            OFFSET {page_size * (page - 1)}
        """
        results = await get_pg_realtime_catalog_database().fetch_all(sql)
        total_count = await get_pg_realtime_catalog_database().fetch_val(
            query=f'''
            select 
                count(1) as total
            from shadows_offer_types
            {where}
            '''
        )
        items = []
        for result in results:
            res = {
                'name': result['offer_type_name'],
                'valid': result['valid'],
                'url': result['url'],
                'offer_hash': result['offer_hash']
            }
            items.append(ShadowsOfferTypesModel(**res))
        return {'items': items, 'total_count': total_count}

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail="An error occurred while getting shadows stats") from e


async def update_offer_type(name, action) -> dict:
    insert_query = f"""
        UPDATE shadows_offer_types
        SET valid = :valid
        where offer_type_name = :name
        """
    insert_values = {"valid": action, 'name': name}

    await get_pg_realtime_catalog_database().execute(insert_query, insert_values)
    redis_key = f"shadows_offer_types_{hashlib.md5(name.encode()).hexdigest()}"
    invalidate_shadows_cache(redis_key)
    return {"message": "updated successfully"}


async def trigger_stat_checker():
    sqs = boto3.client('sqs')
    queue_url = f'https://sqs.us-east-1.amazonaws.com/317822790556/shadows-status-checker-{os.getenv("ENVIRONMENT")}'
    sqs.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps({'action': 'trigger'})
    )
