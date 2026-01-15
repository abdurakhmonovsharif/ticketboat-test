import os
import asyncio
from dotenv import load_dotenv
from src.app.database import get_pg_database

# Load environment variables from .env file
load_dotenv()

async def check_schema():
    try:
        database = get_pg_database()
        result = await database.fetch_all(
            "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'onsale_email_analysis' ORDER BY ordinal_position"
        )
        print("OnSale Email Analysis Table Columns:")
        for row in result:
            print(f"  {row['column_name']}: {row['data_type']}")
    except Exception as e:
        print(f"Error checking schema: {e}")

if __name__ == "__main__":
    asyncio.run(check_schema())
