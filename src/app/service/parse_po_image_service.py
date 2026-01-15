# parse_po_image_service.py

import base64
import json
import re
from datetime import datetime, timezone

from app.service.openai_service import OpenAiService

DATETIME_FORMAT = "%Y-%m-%d"


def encode_image_to_base64(file_content: bytes, content_type: str) -> str:
    """
    Convert image content to a base64-encoded string with a data URI.
    """
    image_base64 = base64.b64encode(file_content).decode('utf-8')
    return f"data:{content_type};base64,{image_base64}"


def generate_prompt() -> str:
    """
    Generate a standard prompt for parsing purchase order data from images.
    """
    return (
        f"Please analyze the following image(s) and extract the relevant details to complete a Purchase Order. "
        f"Return the data in JSON format with the following fields:\n\n"
        "- account: Any account information available.\n"
        "- card: Any card information available, especially the last 4 digits if visible.\n"
        "- event: The name of the event.\n"
        "- opponent: If applicable, the opponent in the event (e.g., for sports events).\n"
        "- venue: The location where the event will take place.\n"
        f"- date: The event date in the format '{DATETIME_FORMAT}'.\n"
        "- time: The event time.\n"
        "- tba: Set to true if the time is 'to be announced', otherwise false.\n"
        "- shipping_method: The shipping method for tickets, if specified.\n"
        "- quantity: The number of tickets or items purchased.\n"
        "- section: The section for seating.\n"
        "- row: The row for seating.\n"
        "- start_seat: The starting seat number.\n"
        "- end_seat: The ending seat number.\n"
        "- total_cost: The total cost as a float.\n"
        "- conf_number: Any confirmation number present.\n"
        "- consecutive: Set to true if the seats are consecutive, false if not, or null if unclear.\n"
        "- internal_note: Any internal notes or remarks.\n"
        "- external_note: Any notes directed towards the customer or external parties.\n"
        "- po_note: Any purchase order notes.\n"
        "- po_number: Any purchase order number present.\n\n"
        "If any information is not visible in the image(s), set its value to null. Use the most recent or prominent information if multiple entries are found. Ensure that all data is accurate and relevant."
    )


async def parse_image(image_data: bytes, content_type: str) -> dict:
    try:
        image_url = encode_image_to_base64(image_data, content_type)
        prompt = generate_prompt()
        openai_extractor = OpenAiService()

        raw_output = openai_extractor.generate_response(prompt=prompt, images=[image_url])
        extracted_data = extract_json_from_response(raw_output)
        return post_process_extracted_data(extracted_data)
    except Exception as e:
        print(f"Error in parse_image: {str(e)}")
        raise


def post_process_extracted_data(extracted_data: dict, email_received_timestamp: datetime = None) -> dict:
    """
    Post-process the extracted data to ensure correct formatting and handle any special cases.
    """
    if not email_received_timestamp:
        email_received_timestamp = datetime.now(timezone.utc)

    # Convert date to correct format if it exists
    if extracted_data.get('date'):
        try:
            date_obj = datetime.strptime(extracted_data['date'], DATETIME_FORMAT)
            extracted_data['date'] = date_obj.strftime(DATETIME_FORMAT)
        except ValueError:
            # If parsing fails, keep the original value
            pass

    # Ensure quantity is an integer
    if extracted_data.get('quantity'):
        try:
            extracted_data['quantity'] = int(extracted_data['quantity'])
        except ValueError:
            extracted_data['quantity'] = None

    # Ensure total_cost is a float
    if extracted_data.get('total_cost'):
        try:
            extracted_data['total_cost'] = float(extracted_data['total_cost'])
        except ValueError:
            extracted_data['total_cost'] = None

    # Ensure tba and consecutive are boolean values
    for field in ['tba', 'consecutive']:
        if extracted_data.get(field) is not None:
            extracted_data[field] = str(extracted_data[field]).lower() == 'true'

    return extracted_data


def extract_json_from_response(raw_string: str) -> dict:
    """
    Extracts a JSON object from a raw string response.
    """
    json_match = re.search(r'\{[\s\S]*\}', raw_string)

    if json_match:
        json_string = json_match.group(0)
        json_string = re.sub(r'//.*$', '', json_string, flags=re.MULTILINE)
        json_string = re.sub(r'/\*[\s\S]*?\*/', '', json_string)
        try:
            json_object = json.loads(json_string)
            return json_object
        except json.JSONDecodeError:
            print("Error: Unable to parse JSON")
            return {}
    else:
        print("Error: No JSON object found in the string")
        return {}
