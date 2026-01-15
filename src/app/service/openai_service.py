import os

import openai


class OpenAiService:

    def __init__(self, open_api_key: str = os.getenv('OPENAI_API_KEY')):
        openai.api_key = open_api_key

    @staticmethod
    def generate_response(
            prompt: str,
            images: list[str] = None
    ) -> str | None:

        messages = [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": prompt}
                ]
            }
        ]

        if images:
            for image_url in images:
                messages[0]["content"].append({
                    "type": "image_url",
                    "image_url": {"url": image_url}
                })

        response = openai.chat.completions.create(
            model="gpt-4o-mini",
            messages=messages,
        )

        # Extract the response content
        if response and response.choices:
            return response.choices[0].message.content
        else:
            raise Exception("Failed to get a valid response from OpenAI")
