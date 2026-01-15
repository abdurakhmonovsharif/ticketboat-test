import httpx


async def geocode_ams_address(address: str) -> str | None:
    """Geocodes an address using OpenStreetMap Nominatim."""
    try:
        async with httpx.AsyncClient() as client:
            url = f"https://nominatim.openstreetmap.org/search?q={address}&format=jsonv2"
            response = await client.get(url)
            response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
            data = response.json()
            if data:
                return f"{data[0]['lat']}/{data[0]['lon']}"
            else:
                return None  # Address not found
    except httpx.HTTPError as e:
        print(f"Geocoding error: {e}")
        return None
    except Exception as e:
        print(f"Geocoding error: {e}")
        return None
