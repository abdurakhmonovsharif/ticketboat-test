import asyncio
import json
from app.db.onsale_email_analysis_db import get_onsale_email_analyses
from app.database import init_pg_database, close_pg_database

async def test_source_urls():
    await init_pg_database()
    try:
        result = await get_onsale_email_analyses(page=1, page_size=3)
        print(f"Found {len(result['items'])} items")
        
        for i, item in enumerate(result['items']):
            print(f"\nItem {i+1}:")
            print(f"  Event: {item.get('event_name')}")
            print(f"  Additional details type: {type(item.get('additional_details'))}")
            print(f"  Additional details: {item.get('additional_details')}")
            
            if item.get('additional_details'):
                source_urls = item['additional_details'].get('source_urls', [])
                print(f"  Source URLs: {source_urls}")
                print(f"  Source URLs count: {len(source_urls)}")
            else:
                print("  No additional_details")
                
    finally:
        await close_pg_database()

if __name__ == "__main__":
    asyncio.run(test_source_urls())
