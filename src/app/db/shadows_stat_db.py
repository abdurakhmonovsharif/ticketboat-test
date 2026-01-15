import json
import os
import traceback

import boto3
from fastapi import HTTPException

from app.database import get_pg_realtime_catalog_database
from app.model.shadows_listings import *
from app.model.shadows_stats import ShadowsStatsModel, ShadowsStatsConfigModel


async def get_stats() -> List[ShadowsStatsModel]:
    try:
        sql = """
            select 
                ssc.name,
                ssc.type,
                ssc.data config_data,
                ss.data stat_data,
                ss.last_updated
            from shadows_status_config ssc left join shadows_status ss on ssc.name=ss.name and  ssc.type=ss.type
            order by ssc.name desc
        """
        results = await get_pg_realtime_catalog_database().fetch_all(sql)
        items = []
        for result in results:
            res = {
                'name': result['name'],
                'type': result['type'],
                'last_updated': str(result['last_updated']),
                'config_data': json.loads(result['config_data']) if result['config_data'] else {},
                'stat_data': json.loads(result['stat_data']) if result['stat_data'] else {}
            }
            items.append(ShadowsStatsModel(**res))
        return items

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail="An error occurred while getting shadows stats") from e


async def get_stats_config() -> List[ShadowsStatsConfigModel]:
    try:
        sql = """ select * from shadows_status_config order by name desc """
        results = await get_pg_realtime_catalog_database().fetch_all(sql)
        print(results)
        items = []
        for result in results:
            res = {
                'name': result['name'],
                'type': result['type'],
                'data': json.loads(result['data'])
            }
            items.append(ShadowsStatsConfigModel(**res))
        return items
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail="An error occurred while getting shadows stats") from e


async def delete_stats_config(name, type) -> dict:
    try:
        sql = f""" delete from shadows_status_config where name='{name}' and type='{type}'"""
        await get_pg_realtime_catalog_database().execute(sql)
        return {"message": "config deleted successfully"}
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail="An error occurred while getting shadows stats") from e


async def store_stat_config(params: ShadowsStatsConfigModel) -> dict:
    insert_query = """
        INSERT INTO shadows_status_config (
            name, type, data
        ) VALUES (
            :name, :type, :data
        )
        """
    insert_values = {
        "name": params.name,
        "type": params.type,
        "data": json.dumps(params.data)
    }
    await get_pg_realtime_catalog_database().execute(insert_query, insert_values)
    await trigger_stat_checker()
    return {"message": "config created successfully"}


async def update_stat_config(params: ShadowsStatsConfigModel, name) -> dict:
    insert_query = f"""
        UPDATE shadows_status_config
        SET data = :data
        where name='{name}' and type='{params.type}'
        """
    insert_values = {"data": json.dumps(params.data)}

    await get_pg_realtime_catalog_database().execute(insert_query, insert_values)
    await trigger_stat_checker()
    return {"message": "config created successfully"}


async def trigger_stat_checker():
    sqs = boto3.client('sqs')
    queue_url = f'https://sqs.us-east-1.amazonaws.com/317822790556/shadows-status-checker-{os.getenv("ENVIRONMENT")}'
    sqs.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps({'action': 'trigger'})
    )
