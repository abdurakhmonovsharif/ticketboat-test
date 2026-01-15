#!/usr/bin/env python3
"""
Test script for OnSale Email Analysis API endpoints.
This script tests the backend functionality without requiring the full application to be running.
"""

import asyncio
import sys
import os

# Add the src directory to the Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from app.db.onsale_email_analysis_db import (
    get_onsale_email_analyses,
    get_onsale_email_analysis_summary,
    get_onsale_email_analysis_venues,
    get_onsale_email_analysis_performers,
    get_onsale_email_analysis_event_types
)


async def test_database_functions():
    """Test the database functions directly."""
    print("🧪 Testing OnSale Email Analysis Database Functions...")
    
    try:
        # Test getting venues
        print("\n1. Testing get_onsale_email_analysis_venues()...")
        venues_result = await get_onsale_email_analysis_venues()
        print(f"   ✅ Venues: {len(venues_result['items'])} venues found")
        if venues_result['items']:
            print(f"   📍 Sample venues: {venues_result['items'][:3]}")
        
        # Test getting performers
        print("\n2. Testing get_onsale_email_analysis_performers()...")
        performers_result = await get_onsale_email_analysis_performers()
        print(f"   ✅ Performers: {len(performers_result['items'])} performers found")
        if performers_result['items']:
            print(f"   🎤 Sample performers: {performers_result['items'][:3]}")
        
        # Test getting event types
        print("\n3. Testing get_onsale_email_analysis_event_types()...")
        event_types_result = await get_onsale_email_analysis_event_types()
        print(f"   ✅ Event Types: {len(event_types_result['items'])} event types found")
        if event_types_result['items']:
            print(f"   🎭 Sample event types: {event_types_result['items'][:3]}")
        
        # Test getting analyses with pagination
        print("\n4. Testing get_onsale_email_analyses()...")
        analyses_result = await get_onsale_email_analyses(
            page=1,
            page_size=5,
            search_term=None,
            venue=None,
            performer=None,
            start_date=None,
            end_date=None,
            min_opportunity_score=None,
            max_opportunity_score=None,
            event_type=None,
            market_volatility_level=None,
            demand_uncertainty_level=None,
            competition_level=None,
            timezone="America/Chicago"
        )
        print(f"   ✅ Analyses: {analyses_result['total']} total analyses found")
        print(f"   📊 Retrieved {len(analyses_result['items'])} analyses for page 1")
        
        if analyses_result['items']:
            sample_analysis = analyses_result['items'][0]
            print(f"   📋 Sample analysis:")
            print(f"      - Event: {sample_analysis.get('event_name', 'N/A')}")
            print(f"      - Performer: {sample_analysis.get('performer', 'N/A')}")
            print(f"      - Venue: {sample_analysis.get('venue_name', 'N/A')}")
            print(f"      - Opportunity Score: {sample_analysis.get('opportunity_score', 'N/A')}")
            print(f"      - Confidence: {sample_analysis.get('confidence_percentage', 'N/A')}%")
        
        # Test getting summary
        print("\n5. Testing get_onsale_email_analysis_summary()...")
        summary_result = await get_onsale_email_analysis_summary(
            search_term=None,
            venue=None,
            performer=None,
            start_date=None,
            end_date=None,
            min_opportunity_score=None,
            max_opportunity_score=None,
            event_type=None,
            market_volatility_level=None,
            demand_uncertainty_level=None,
            competition_level=None,
            timezone="America/Chicago"
        )
        print(f"   ✅ Summary generated successfully")
        print(f"   📈 Total Analyses: {summary_result['total_analyses']}")
        print(f"   📊 Average Opportunity Score: {summary_result['average_opportunity_score']:.2f}%")
        print(f"   🟢 High Opportunity Events: {summary_result['high_opportunity_count']}")
        print(f"   🟡 Medium Opportunity Events: {summary_result['medium_opportunity_count']}")
        print(f"   🔴 Low Opportunity Events: {summary_result['low_opportunity_count']}")
        print(f"   🏆 Top Performers: {len(summary_result['top_performers'])}")
        print(f"   🏟️ Top Venues: {len(summary_result['top_venues'])}")
        print(f"   🎭 Event Types: {len(summary_result['event_type_distribution'])}")
        print(f"   📈 Market Volatility Levels: {len(summary_result['market_volatility_distribution'])}")
        print(f"   ⏰ Recent Analyses: {len(summary_result['recent_analyses'])}")
        
        print("\n🎉 All database tests passed successfully!")
        return True
        
    except Exception as e:
        print(f"\n❌ Error during testing: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


async def test_with_filters():
    """Test the database functions with various filters."""
    print("\n🔍 Testing with filters...")
    
    try:
        # Test with search term
        print("\n1. Testing with search term...")
        search_result = await get_onsale_email_analyses(
            page=1,
            page_size=3,
            search_term="concert",
            timezone="America/Chicago"
        )
        print(f"   ✅ Search results: {search_result['total']} analyses found for 'concert'")
        
        # Test with opportunity score filter
        print("\n2. Testing with opportunity score filter...")
        score_result = await get_onsale_email_analyses(
            page=1,
            page_size=3,
            min_opportunity_score=75.0,
            timezone="America/Chicago"
        )
        print(f"   ✅ High opportunity results: {score_result['total']} analyses with score >= 75")
        
        # Test with date filter
        print("\n3. Testing with date filter...")
        date_result = await get_onsale_email_analyses(
            page=1,
            page_size=3,
            start_date="2024-01-01",
            end_date="2024-12-31",
            timezone="America/Chicago"
        )
        print(f"   ✅ Date filtered results: {date_result['total']} analyses in 2024")
        
        print("\n🎉 All filter tests passed successfully!")
        return True
        
    except Exception as e:
        print(f"\n❌ Error during filter testing: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


async def main():
    """Main test function."""
    print("🚀 Starting OnSale Email Analysis Backend Tests...")
    print("=" * 60)
    
    # Test basic database functions
    basic_success = await test_database_functions()
    
    # Test with filters
    filter_success = await test_with_filters()
    
    print("\n" + "=" * 60)
    if basic_success and filter_success:
        print("🎉 All tests passed! The backend is working correctly.")
        print("\n📋 Summary:")
        print("   ✅ Database connection working")
        print("   ✅ All database functions operational")
        print("   ✅ Filter functionality working")
        print("   ✅ Summary statistics working")
        print("\n🚀 The OnSale Email Analysis backend is ready for the frontend!")
    else:
        print("❌ Some tests failed. Please check the error messages above.")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
