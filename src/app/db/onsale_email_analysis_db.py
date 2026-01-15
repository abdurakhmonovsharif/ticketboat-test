from typing import Optional, List, Dict, Any
from datetime import datetime
import asyncpg
import json
from app.database import get_pg_database
from app.time_utils.timezone_utils import convert_utc_to_timezone, get_timezone_fallback_order


async def get_onsale_email_analyses(
    page: int = 1,
    page_size: int = 20,
    search_term: Optional[str] = None,
    venue: Optional[List[str]] = None,
    performer: Optional[List[str]] = None,
    event_date_start: Optional[str] = None,
    event_date_end: Optional[str] = None,
    onsale_date_start: Optional[str] = None,
    onsale_date_end: Optional[str] = None,
    presale_date_start: Optional[str] = None,
    presale_date_end: Optional[str] = None,
    event_type: Optional[str] = None,
    market_volatility_level: Optional[str] = None,
    demand_uncertainty_level: Optional[str] = None,
    competition_level: Optional[str] = None,
    overall_opportunity_level: Optional[str] = None,
    min_estimated_profit: Optional[float] = None,
    sort_field: Optional[str] = None,
    sort_order: Optional[str] = None,
    timezone: str = "America/Chicago"
) -> Dict[str, Any]:
    """
    Get paginated onsale email analysis data with filters.
    """
    database = get_pg_database()
    
    # Build WHERE conditions
    where_conditions = []
    params = {}
    
    if search_term:
        where_conditions.append("""
            (LOWER(event_name) LIKE :search_term OR 
             LOWER(venue_name) LIKE :search_term OR 
             LOWER(performer) LIKE :search_term)
        """)
        params['search_term'] = f"%{search_term.lower()}%"
    
    if venue:
        where_conditions.append("venue_name = ANY(:venue)")
        params['venue'] = venue
    
    if performer:
        where_conditions.append("performer = ANY(:performer)")
        params['performer'] = performer
    
    if event_date_start and event_date_start.strip():
        where_conditions.append("event_date >= :event_date_start")
        params['event_date_start'] = datetime.strptime(event_date_start, '%Y-%m-%d').date()
    
    if event_date_end and event_date_end.strip():
        where_conditions.append("event_date <= :event_date_end")
        params['event_date_end'] = datetime.strptime(event_date_end, '%Y-%m-%d').date()
    
    if onsale_date_start and onsale_date_start.strip():
        where_conditions.append("onsale_date >= :onsale_date_start")
        params['onsale_date_start'] = datetime.strptime(onsale_date_start, '%Y-%m-%d').date()
    
    if onsale_date_end and onsale_date_end.strip():
        where_conditions.append("onsale_date <= :onsale_date_end")
        params['onsale_date_end'] = datetime.strptime(onsale_date_end, '%Y-%m-%d').date()
    
    if presale_date_start and presale_date_start.strip():
        where_conditions.append("presale_date >= :presale_date_start")
        params['presale_date_start'] = datetime.strptime(presale_date_start, '%Y-%m-%d').date()
    
    if presale_date_end and presale_date_end.strip():
        where_conditions.append("presale_date <= :presale_date_end")
        params['presale_date_end'] = datetime.strptime(presale_date_end, '%Y-%m-%d').date()
    
    if event_type:
        where_conditions.append("event_type = :event_type")
        params['event_type'] = event_type
    
    if market_volatility_level:
        where_conditions.append("LOWER(market_volatility_level) = LOWER(:market_volatility_level)")
        params['market_volatility_level'] = market_volatility_level
    
    if demand_uncertainty_level:
        where_conditions.append("LOWER(demand_uncertainty_level) = LOWER(:demand_uncertainty_level)")
        params['demand_uncertainty_level'] = demand_uncertainty_level
    
    if competition_level:
        where_conditions.append("LOWER(competition_level) = LOWER(:competition_level)")
        params['competition_level'] = competition_level
    
    # Handle Overall Opportunity level filtering using the computed column
    if overall_opportunity_level:
        if overall_opportunity_level == 'hot':
            # HOT: Overall Opportunity score >= 80
            where_conditions.append("overall_opportunity_score >= 80")
        elif overall_opportunity_level == 'great':
            # GREAT: Overall Opportunity score >= 70
            where_conditions.append("overall_opportunity_score >= 70")
        elif overall_opportunity_level == 'good':
            # GOOD: Overall Opportunity score >= 60
            where_conditions.append("overall_opportunity_score >= 60")
    
    if min_estimated_profit is not None:
        where_conditions.append("estimated_total_profit >= :min_estimated_profit")
        params['min_estimated_profit'] = min_estimated_profit
    
    # Always filter out records without event dates
    where_conditions.append("event_date IS NOT NULL")
    
    where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"
    
    # Build ORDER BY clause
    order_by_clause = "analysis_generated_at DESC"  # Default sorting
    
    if sort_field and sort_order:
        # Map frontend field names to database column names
        field_mapping = {
            'overallOpportunity': 'overall_opportunity_score',
            'buyability': 'buyability_score',
            'estimated_profit': 'estimated_total_profit',
            'event_date': 'event_date',
            'email_ts': 'email_ts'
        }
        
        db_field = field_mapping.get(sort_field)
        if db_field:
            order_direction = "ASC" if sort_order == "ascend" else "DESC"
            order_by_clause = f"{db_field} {order_direction}"
    
    # Calculate offset
    offset = (page - 1) * page_size
    
    # Main query
    query = f"""
        SELECT 
            id,
            email_id,
            email_subject,
            email_from,
            email_to,
            email_ts,
            analysis_generated_at,
            event_name,
            venue_name,
            venue_location,
            performer,
            event_type,
            event_url,
            opportunity_score,
            confidence_percentage,
            target_margin_percentage,
            risk_factors,
            opportunities,
            reasoning_summary,
            historical_context,
            buying_guidance,
            risk_management,
            next_steps,
            market_volatility_level,
            demand_uncertainty_level,
            competition_level,
            recommended_buy_amount_min,
            recommended_buy_amount_max,
            target_resale_markup_percentage,
            stop_loss_percentage,
            created_at,
            updated_at,
            onsale_date,
            presale_date,
            discount_code,
            buyability_score,
            event_date,
            event_date_timezone,
            onsale_date_timezone,
            presale_date_timezone,
            overall_opportunity_score,
            estimated_total_profit,
            additional_details
        FROM onsale_email_analysis
        WHERE {where_clause}
        ORDER BY {order_by_clause}
        LIMIT :page_size OFFSET :offset
    """
    
    # Count query
    count_query = f"""
        SELECT COUNT(*) as total
        FROM onsale_email_analysis
        WHERE {where_clause}
    """
    
    params_with_pagination = {**params, 'page_size': page_size, 'offset': offset}
    
    try:
        # Get data
        rows = await database.fetch_all(query=query, values=params_with_pagination)
        
        # Get total count
        count_result = await database.fetch_one(query=count_query, values=params)
        total = count_result['total'] if count_result else 0
    except Exception as e:
        # If table doesn't exist yet, return empty results
        if "relation \"onsale_email_analysis\" does not exist" in str(e):
            return {"items": [], "total": 0}
        raise e
    
    # Convert rows to list of dicts
    items = []
    for row in rows:
        item = dict(row)
        # Convert arrays to lists
        if item.get('risk_factors'):
            item['risk_factors'] = item['risk_factors'] if isinstance(item['risk_factors'], list) else []
        else:
            item['risk_factors'] = []
            
        if item.get('opportunities'):
            item['opportunities'] = item['opportunities'] if isinstance(item['opportunities'], list) else []
        else:
            item['opportunities'] = []
        
        # Parse additional_details JSON if it exists
        if item.get('additional_details'):
            try:
                if isinstance(item['additional_details'], str):
                    item['additional_details'] = json.loads(item['additional_details'])
                elif item['additional_details'] is None:
                    item['additional_details'] = None
            except (json.JSONDecodeError, TypeError):
                item['additional_details'] = None
        else:
            item['additional_details'] = None
        
        # Convert dates to their proper timezones
        event_date_tz = item.get('event_date_timezone')
        onsale_date_tz = item.get('onsale_date_timezone')
        presale_date_tz = item.get('presale_date_timezone')
        
        # Get fallback timezone (event_date_timezone if valid)
        fallback_tz = get_timezone_fallback_order(event_date_tz, onsale_date_tz, presale_date_tz)
        
        # Convert event_date
        if item.get('event_date'):
            item['event_date'] = convert_utc_to_timezone(
                item['event_date'], 
                event_date_tz, 
                fallback_tz
            )
        
        # Convert onsale_date
        if item.get('onsale_date'):
            item['onsale_date'] = convert_utc_to_timezone(
                item['onsale_date'], 
                onsale_date_tz, 
                fallback_tz
            )
        
        # Convert presale_date
        if item.get('presale_date'):
            item['presale_date'] = convert_utc_to_timezone(
                item['presale_date'], 
                presale_date_tz, 
                fallback_tz
            )
        
        items.append(item)
    
    return {"items": items, "total": total}


async def get_onsale_email_analysis_summary(
    search_term: Optional[str] = None,
    venue: Optional[List[str]] = None,
    performer: Optional[List[str]] = None,
    event_date_start: Optional[str] = None,
    event_date_end: Optional[str] = None,
    onsale_date_start: Optional[str] = None,
    onsale_date_end: Optional[str] = None,
    presale_date_start: Optional[str] = None,
    presale_date_end: Optional[str] = None,
    event_type: Optional[str] = None,
    market_volatility_level: Optional[str] = None,
    demand_uncertainty_level: Optional[str] = None,
    competition_level: Optional[str] = None,
    overall_opportunity_level: Optional[str] = None,
    min_estimated_profit: Optional[float] = None,
    timezone: str = "America/Chicago"
) -> Dict[str, Any]:
    """
    Get summary statistics for onsale email analysis.
    """
    database = get_pg_database()
    
    # Build WHERE conditions (same as main query)
    where_conditions = []
    params = {}
    
    if search_term:
        where_conditions.append("""
            (LOWER(event_name) LIKE :search_term OR 
             LOWER(venue_name) LIKE :search_term OR 
             LOWER(performer) LIKE :search_term)
        """)
        params['search_term'] = f"%{search_term.lower()}%"
    
    if venue:
        where_conditions.append("venue_name = ANY(:venue)")
        params['venue'] = venue
    
    if performer:
        where_conditions.append("performer = ANY(:performer)")
        params['performer'] = performer
    
    if event_date_start and event_date_start.strip():
        where_conditions.append("event_date >= :event_date_start")
        params['event_date_start'] = datetime.strptime(event_date_start, '%Y-%m-%d').date()
    
    if event_date_end and event_date_end.strip():
        where_conditions.append("event_date <= :event_date_end")
        params['event_date_end'] = datetime.strptime(event_date_end, '%Y-%m-%d').date()
    
    if onsale_date_start and onsale_date_start.strip():
        where_conditions.append("onsale_date >= :onsale_date_start")
        params['onsale_date_start'] = datetime.strptime(onsale_date_start, '%Y-%m-%d').date()
    
    if onsale_date_end and onsale_date_end.strip():
        where_conditions.append("onsale_date <= :onsale_date_end")
        params['onsale_date_end'] = datetime.strptime(onsale_date_end, '%Y-%m-%d').date()
    
    if presale_date_start and presale_date_start.strip():
        where_conditions.append("presale_date >= :presale_date_start")
        params['presale_date_start'] = datetime.strptime(presale_date_start, '%Y-%m-%d').date()
    
    if presale_date_end and presale_date_end.strip():
        where_conditions.append("presale_date <= :presale_date_end")
        params['presale_date_end'] = datetime.strptime(presale_date_end, '%Y-%m-%d').date()
    
    if event_type:
        where_conditions.append("event_type = :event_type")
        params['event_type'] = event_type
    
    if market_volatility_level:
        where_conditions.append("LOWER(market_volatility_level) = LOWER(:market_volatility_level)")
        params['market_volatility_level'] = market_volatility_level
    
    if demand_uncertainty_level:
        where_conditions.append("LOWER(demand_uncertainty_level) = LOWER(:demand_uncertainty_level)")
        params['demand_uncertainty_level'] = demand_uncertainty_level
    
    if competition_level:
        where_conditions.append("LOWER(competition_level) = LOWER(:competition_level)")
        params['competition_level'] = competition_level
    
    # Handle Overall Opportunity level filtering using the computed column
    if overall_opportunity_level:
        if overall_opportunity_level == 'hot':
            # HOT: Overall Opportunity score >= 80
            where_conditions.append("overall_opportunity_score >= 80")
        elif overall_opportunity_level == 'great':
            # GREAT: Overall Opportunity score >= 70
            where_conditions.append("overall_opportunity_score >= 70")
        elif overall_opportunity_level == 'good':
            # GOOD: Overall Opportunity score >= 60
            where_conditions.append("overall_opportunity_score >= 60")
    
    if min_estimated_profit is not None:
        where_conditions.append("estimated_total_profit >= :min_estimated_profit")
        params['min_estimated_profit'] = min_estimated_profit
    
    # Always filter out records without event dates
    where_conditions.append("event_date IS NOT NULL")
    
    where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"
    
    try:
        # Get basic statistics
        stats_query = f"""
            SELECT 
                COUNT(*) as total_analyses,
                AVG(opportunity_score) as average_opportunity_score,
                COUNT(CASE WHEN opportunity_score >= 75 THEN 1 END) as high_opportunity_count,
                COUNT(CASE WHEN opportunity_score >= 50 AND opportunity_score < 75 THEN 1 END) as medium_opportunity_count,
                COUNT(CASE WHEN opportunity_score < 50 THEN 1 END) as low_opportunity_count,
                COUNT(CASE WHEN overall_opportunity_score >= 80 THEN 1 END) as hot_opportunity_count,
                COUNT(CASE WHEN overall_opportunity_score >= 70 THEN 1 END) as great_opportunity_count,
                COUNT(CASE WHEN overall_opportunity_score >= 60 THEN 1 END) as good_opportunity_count,
                COUNT(CASE WHEN overall_opportunity_score < 60 THEN 1 END) as pass_opportunity_count
            FROM onsale_email_analysis
            WHERE {where_clause}
        """
        
        stats_result = await database.fetch_one(query=stats_query, values=params)
    except Exception as e:
        # If table doesn't exist yet, return empty results
        if "relation \"onsale_email_analysis\" does not exist" in str(e):
            return {
                "total_analyses": 0,
                "average_opportunity_score": 0.0,
                "high_opportunity_count": 0,
                "medium_opportunity_count": 0,
                "low_opportunity_count": 0,
                "hot_opportunity_count": 0,
                "great_opportunity_count": 0,
                "good_opportunity_count": 0,
                "pass_opportunity_count": 0,
                "top_performers": [],
                "top_venues": [],
                "event_type_distribution": [],
                "market_volatility_distribution": [],
                "recent_analyses": []
            }
        raise e
    
    # Get top performers
    performers_query = f"""
        SELECT 
            performer,
            AVG(opportunity_score) as avg_score,
            COUNT(*) as count
        FROM onsale_email_analysis
        WHERE {where_clause} AND performer IS NOT NULL AND performer != ''
        GROUP BY performer
        ORDER BY avg_score DESC
        LIMIT 10
    """
    
    performers_result = await database.fetch_all(query=performers_query, values=params)
    
    # Get top venues
    venues_query = f"""
        SELECT 
            venue_name as venue,
            AVG(opportunity_score) as avg_score,
            COUNT(*) as count
        FROM onsale_email_analysis
        WHERE {where_clause} AND venue_name IS NOT NULL AND venue_name != ''
        GROUP BY venue_name
        ORDER BY avg_score DESC
        LIMIT 10
    """
    
    venues_result = await database.fetch_all(query=venues_query, values=params)
    
    # Get event type distribution
    event_types_query = f"""
        SELECT 
            event_type,
            COUNT(*) as count
        FROM onsale_email_analysis
        WHERE {where_clause} AND event_type IS NOT NULL AND event_type != ''
        GROUP BY event_type
        ORDER BY count DESC
        LIMIT 10
    """
    
    event_types_result = await database.fetch_all(query=event_types_query, values=params)
    
    # Get market volatility distribution
    volatility_query = f"""
        SELECT 
            market_volatility_level as level,
            COUNT(*) as count
        FROM onsale_email_analysis
        WHERE {where_clause} AND market_volatility_level IS NOT NULL AND market_volatility_level != ''
        GROUP BY market_volatility_level
        ORDER BY count DESC
    """
    
    volatility_result = await database.fetch_all(query=volatility_query, values=params)
    
    # Get recent analyses
    recent_query = f"""
        SELECT 
            id,
            email_id,
            email_subject,
            email_from,
            email_to,
            email_ts,
            analysis_generated_at,
            event_name,
            venue_name,
            venue_location,
            performer,
            event_type,
            event_url,
            opportunity_score,
            confidence_percentage,
            target_margin_percentage,
            risk_factors,
            opportunities,
            reasoning_summary,
            historical_context,
            buying_guidance,
            risk_management,
            next_steps,
            market_volatility_level,
            demand_uncertainty_level,
            competition_level,
            recommended_buy_amount_min,
            recommended_buy_amount_max,
            target_resale_markup_percentage,
            stop_loss_percentage,
            created_at,
            updated_at,
            onsale_date,
            presale_date,
            discount_code,
            buyability_score,
            event_date,
            event_date_timezone,
            onsale_date_timezone,
            presale_date_timezone,
            overall_opportunity_score,
            estimated_total_profit,
            additional_details
        FROM onsale_email_analysis
        WHERE {where_clause}
        ORDER BY analysis_generated_at DESC
        LIMIT 5
    """
    
    recent_result = await database.fetch_all(query=recent_query, values=params)
    
    # Process results
    summary = {
        "total_analyses": stats_result['total_analyses'] if stats_result else 0,
        "average_opportunity_score": float(stats_result['average_opportunity_score']) if stats_result and stats_result['average_opportunity_score'] else 0.0,
        "high_opportunity_count": stats_result['high_opportunity_count'] if stats_result else 0,
        "medium_opportunity_count": stats_result['medium_opportunity_count'] if stats_result else 0,
        "low_opportunity_count": stats_result['low_opportunity_count'] if stats_result else 0,
        "hot_opportunity_count": stats_result['hot_opportunity_count'] if stats_result else 0,
        "great_opportunity_count": stats_result['great_opportunity_count'] if stats_result else 0,
        "good_opportunity_count": stats_result['good_opportunity_count'] if stats_result else 0,
        "pass_opportunity_count": stats_result['pass_opportunity_count'] if stats_result else 0,
        "top_performers": [dict(row) for row in performers_result],
        "top_venues": [dict(row) for row in venues_result],
        "event_type_distribution": [dict(row) for row in event_types_result],
        "market_volatility_distribution": [dict(row) for row in volatility_result],
        "recent_analyses": []
    }
    
    # Process recent analyses
    for row in recent_result:
        item = dict(row)
        if item.get('risk_factors'):
            item['risk_factors'] = item['risk_factors'] if isinstance(item['risk_factors'], list) else []
        else:
            item['risk_factors'] = []
            
        if item.get('opportunities'):
            item['opportunities'] = item['opportunities'] if isinstance(item['opportunities'], list) else []
        else:
            item['opportunities'] = []
        
        # Parse additional_details JSON if it exists
        if item.get('additional_details'):
            try:
                if isinstance(item['additional_details'], str):
                    item['additional_details'] = json.loads(item['additional_details'])
                elif item['additional_details'] is None:
                    item['additional_details'] = None
            except (json.JSONDecodeError, TypeError):
                item['additional_details'] = None
        else:
            item['additional_details'] = None
        
        # Convert dates to their proper timezones
        event_date_tz = item.get('event_date_timezone')
        onsale_date_tz = item.get('onsale_date_timezone')
        presale_date_tz = item.get('presale_date_timezone')
        
        # Get fallback timezone (event_date_timezone if valid)
        fallback_tz = get_timezone_fallback_order(event_date_tz, onsale_date_tz, presale_date_tz)
        
        # Convert event_date
        if item.get('event_date'):
            item['event_date'] = convert_utc_to_timezone(
                item['event_date'], 
                event_date_tz, 
                fallback_tz
            )
        
        # Convert onsale_date
        if item.get('onsale_date'):
            item['onsale_date'] = convert_utc_to_timezone(
                item['onsale_date'], 
                onsale_date_tz, 
                fallback_tz
            )
        
        # Convert presale_date
        if item.get('presale_date'):
            item['presale_date'] = convert_utc_to_timezone(
                item['presale_date'], 
                presale_date_tz, 
                fallback_tz
            )
        
        summary["recent_analyses"].append(item)
    
    return summary


async def get_onsale_email_analysis_venues() -> Dict[str, Any]:
    """
    Get unique venues from onsale email analysis.
    """
    database = get_pg_database()
    
    query = """
        SELECT DISTINCT venue_name
        FROM onsale_email_analysis
        WHERE venue_name IS NOT NULL AND venue_name != ''
        ORDER BY venue_name
    """
    
    try:
        result = await database.fetch_all(query=query)
        venues = [row['venue_name'] for row in result]
        return {"items": venues, "total": len(venues)}
    except Exception as e:
        # If table doesn't exist yet, return empty results
        if "relation \"onsale_email_analysis\" does not exist" in str(e):
            return {"items": [], "total": 0}
        raise e


async def get_onsale_email_analysis_performers() -> Dict[str, Any]:
    """
    Get unique performers from onsale email analysis.
    """
    database = get_pg_database()
    
    query = """
        SELECT DISTINCT performer
        FROM onsale_email_analysis
        WHERE performer IS NOT NULL AND performer != ''
        ORDER BY performer
    """
    
    try:
        result = await database.fetch_all(query=query)
        performers = [row['performer'] for row in result]
        return {"items": performers, "total": len(performers)}
    except Exception as e:
        # If table doesn't exist yet, return empty results
        if "relation \"onsale_email_analysis\" does not exist" in str(e):
            return {"items": [], "total": 0}
        raise e


async def get_onsale_email_analysis_event_types() -> Dict[str, Any]:
    """
    Get unique event types from onsale email analysis.
    """
    database = get_pg_database()
    
    query = """
        SELECT DISTINCT event_type
        FROM onsale_email_analysis
        WHERE event_type IS NOT NULL AND event_type != ''
        ORDER BY event_type
    """
    
    try:
        result = await database.fetch_all(query=query)
        event_types = [row['event_type'] for row in result]
        return {"items": event_types, "total": len(event_types)}
    except Exception as e:
        # If table doesn't exist yet, return empty results
        if "relation \"onsale_email_analysis\" does not exist" in str(e):
            return {"items": [], "total": 0}
        raise e


async def get_onsale_email_analysis_by_id(analysis_id: str) -> Optional[Dict[str, Any]]:
    """
    Get a single onsale email analysis by ID.
    """
    database = get_pg_database()
    
    query = """
        SELECT 
            id,
            email_id,
            email_subject,
            email_from,
            email_to,
            email_ts,
            analysis_generated_at,
            event_name,
            venue_name,
            venue_location,
            performer,
            event_type,
            event_url,
            opportunity_score,
            confidence_percentage,
            target_margin_percentage,
            risk_factors,
            opportunities,
            reasoning_summary,
            historical_context,
            buying_guidance,
            risk_management,
            next_steps,
            market_volatility_level,
            demand_uncertainty_level,
            competition_level,
            recommended_buy_amount_min,
            recommended_buy_amount_max,
            target_resale_markup_percentage,
            stop_loss_percentage,
            created_at,
            updated_at,
            onsale_date,
            presale_date,
            discount_code,
            buyability_score,
            event_date,
            event_date_timezone,
            onsale_date_timezone,
            presale_date_timezone,
            overall_opportunity_score,
            estimated_total_profit
        FROM onsale_email_analysis
        WHERE id = :analysis_id
    """
    
    try:
        result = await database.fetch_one(query=query, values={"analysis_id": analysis_id})
        if not result:
            return None
        
        item = dict(result)
        
        # Process arrays
        if item.get('risk_factors'):
            item['risk_factors'] = item['risk_factors'] if isinstance(item['risk_factors'], list) else []
        else:
            item['risk_factors'] = []
            
        if item.get('opportunities'):
            item['opportunities'] = item['opportunities'] if isinstance(item['opportunities'], list) else []
        else:
            item['opportunities'] = []
        
        # Convert dates to their proper timezones
        event_date_tz = item.get('event_date_timezone')
        onsale_date_tz = item.get('onsale_date_timezone')
        presale_date_tz = item.get('presale_date_timezone')
        
        # Get fallback timezone (event_date_timezone if valid)
        fallback_tz = get_timezone_fallback_order(event_date_tz, onsale_date_tz, presale_date_tz)
        
        # Convert event_date
        if item.get('event_date'):
            item['event_date'] = convert_utc_to_timezone(
                item['event_date'], 
                event_date_tz, 
                fallback_tz
            )
        
        # Convert onsale_date
        if item.get('onsale_date'):
            item['onsale_date'] = convert_utc_to_timezone(
                item['onsale_date'], 
                onsale_date_tz, 
                fallback_tz
            )
        
        # Convert presale_date
        if item.get('presale_date'):
            item['presale_date'] = convert_utc_to_timezone(
                item['presale_date'], 
                presale_date_tz, 
                fallback_tz
            )
        
        return item
    except Exception as e:
        # If table doesn't exist yet, return None
        if "relation \"onsale_email_analysis\" does not exist" in str(e):
            return None
        raise e
