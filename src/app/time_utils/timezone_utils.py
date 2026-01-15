from typing import Optional
from datetime import datetime
import pytz
from pytz import timezone

try:
    from geopy.geocoders import Nominatim
    from timezonefinder import TimezoneFinder
    TIMEZONE_LIBS_AVAILABLE = True
except ImportError:
    TIMEZONE_LIBS_AVAILABLE = False


def get_timezone_from_location(city: Optional[str], state: Optional[str], country: Optional[str]) -> Optional[str]:
    """
    Get IANA timezone identifier from city, state, and country using geocoding.
    
    Uses geopy (OpenStreetMap Nominatim) for geocoding and timezonefinder for timezone lookup.
    Both libraries are free and don't require API keys.
    
    Returns None if timezone cannot be determined (caller should fallback to UTC).
    
    Args:
        city: City name (e.g., "Grand Prairie")
        state: State name or code (e.g., "Texas" or "TX")
        country: Country name (e.g., "United States")
    
    Returns:
        IANA timezone identifier (e.g., "America/Chicago") or None if not found
    
    Example:
        >>> get_timezone_from_location("Grand Prairie", "Texas", "United States")
        'America/Chicago'
    """
    if not TIMEZONE_LIBS_AVAILABLE:
        print("Warning: geopy and/or timezonefinder not installed. Install with: pip install geopy timezonefinder")
        return None
    
    if not city or not country:
        return None
    
    try:
        # Build location query string
        location_parts = [city]
        if state:
            location_parts.append(state)
        location_parts.append(country)
        location_query = ", ".join(location_parts)
        
        # Initialize geocoder (using OpenStreetMap's free Nominatim service)
        # user_agent should be a descriptive string identifying your application
        geolocator = Nominatim(user_agent="ticketboat_venue_timezone_lookup")
        
        # Geocode the location (convert address to coordinates)
        location = geolocator.geocode(location_query, timeout=10)
        
        if not location:
            print(f"Could not geocode location: {location_query}")
            return None
        
        # Find timezone from coordinates
        tf = TimezoneFinder()
        timezone_str = tf.timezone_at(lat=location.latitude, lng=location.longitude)
        
        if timezone_str:
            print(f"Found timezone for {location_query}: {timezone_str} (lat: {location.latitude}, lng: {location.longitude})")
            return timezone_str
        else:
            print(f"Could not determine timezone for coordinates: {location.latitude}, {location.longitude}")
            return None
            
    except Exception as e:
        print(f"Error getting timezone for {city}, {state}, {country}: {e}")
        return None


def convert_utc_to_timezone(utc_datetime: Optional[datetime], target_timezone: Optional[str], fallback_timezone: Optional[str] = None) -> Optional[datetime]:
    """
    Convert a UTC datetime to the specified timezone.
    
    Args:
        utc_datetime: The UTC datetime to convert
        target_timezone: The target timezone (IANA identifier)
        fallback_timezone: Fallback timezone if target_timezone is invalid/missing
    
    Returns:
        The datetime converted to the target timezone, or None if conversion fails
    """
    if not utc_datetime:
        return None
    
    # Determine which timezone to use
    tz_to_use = target_timezone
    
    # Check if timezone is null, empty, "Unknown", or "N/A"
    if not tz_to_use or tz_to_use.strip() == '' or tz_to_use.lower() in ['unknown', 'n/a']:
        tz_to_use = fallback_timezone
    
    # If we still don't have a valid timezone, return the UTC datetime as-is
    if not tz_to_use or tz_to_use.strip() == '' or tz_to_use.lower() in ['unknown', 'n/a']:
        return utc_datetime
    
    try:
        # Ensure the datetime is timezone-aware and in UTC
        if utc_datetime.tzinfo is None:
            utc_datetime = pytz.UTC.localize(utc_datetime)
        elif utc_datetime.tzinfo != pytz.UTC:
            utc_datetime = utc_datetime.astimezone(pytz.UTC)
        
        # Convert to target timezone
        target_tz = timezone(tz_to_use)
        local_datetime = utc_datetime.astimezone(target_tz)
        
        return local_datetime
    except Exception:
        # If timezone conversion fails, return the original UTC datetime
        return utc_datetime


def format_datetime_with_timezone(dt: Optional[datetime], tz_name: Optional[str] = None) -> Optional[str]:
    """
    Format a datetime with timezone information for display.
    
    Args:
        dt: The datetime to format
        tz_name: The timezone name for context
    
    Returns:
        A formatted string with date, time, and timezone info
    """
    if not dt:
        return None
    
    # Include timezone abbreviation if available
    tz_info = ""
    if dt.tzinfo:
        tz_info = f" {dt.strftime('%Z')}"
        if tz_name and tz_name != dt.strftime('%Z'):
            tz_info = f" {dt.strftime('%Z')} ({tz_name})"
    
    return dt.strftime(f"%Y-%m-%d %H:%M:%S{tz_info}")


def get_timezone_fallback_order(event_date_timezone: Optional[str], 
                               onsale_date_timezone: Optional[str], 
                               presale_date_timezone: Optional[str]) -> Optional[str]:
    """
    Determine the best timezone to use based on the fallback logic:
    If a timezone field is null, empty, "Unknown" or "N/A", default to event_date_timezone if it exists.
    
    Args:
        event_date_timezone: Event date timezone
        onsale_date_timezone: Onsale date timezone  
        presale_date_timezone: Presale date timezone
    
    Returns:
        The best timezone to use as fallback, or None if none are valid
    """
    # Check if event_date_timezone is valid
    if (event_date_timezone and 
        event_date_timezone.strip() != '' and 
        event_date_timezone.lower() not in ['unknown', 'n/a']):
        return event_date_timezone
    
    return None
