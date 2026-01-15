"""
OnSale Chat Handler Service
Single Responsibility: Handle AI-powered chat interactions for onsale email analysis
"""

import json
import logging
from typing import List, Dict, Any, AsyncGenerator
from openai import AsyncOpenAI
from openai.types.chat import ChatCompletionMessageParam

from app.model.onsale_chat import ChatMessage, ChatRole
from app.model.onsale_email_analysis import OnsaleEmailAnalysisItem
from .onsale_chat_prompt_builder import OnSaleChatPromptBuilder

logger = logging.getLogger(__name__)


class OnSaleChatHandler:
    """Single responsibility: Handle chat interactions with onsale analysis context"""

    def __init__(self, api_key: str):
        self.client = AsyncOpenAI(api_key=api_key)
        self.prompt_builder = OnSaleChatPromptBuilder()

    async def process_chat_message(
        self,
        message: str,
        analysis: OnsaleEmailAnalysisItem,
        chat_history: List[ChatMessage],
    ) -> str:
        """Process a chat message with analysis context using function calling"""

        # Define function schema for chat responses
        chat_function = {
            "name": "respond_to_onsale_analysis_question",
            "description": "Provide a helpful response to questions about onsale email analysis",
            "parameters": {
                "type": "object",
                "properties": {
                    "response": {
                        "type": "string",
                        "description": "A helpful, specific response to the user's question about their onsale analysis",
                    },
                    "confidence_level": {
                        "type": "string",
                        "enum": ["high", "medium", "low"],
                        "description": "Confidence level in the response based on available analysis data",
                    },
                },
                "required": ["response"],
            },
        }

        try:
            # Convert chat history to format expected by prompt builder
            history_dict = [
                {"role": msg.role.value, "content": msg.content}
                for msg in chat_history
            ]

            # Build system prompt with context using PromptBuilder
            system_prompt = self.prompt_builder.build_chat_prompt(message, analysis, history_dict)

            # Build messages array
            messages: List[ChatCompletionMessageParam] = [
                {"role": "system", "content": system_prompt}
            ]

            # Call OpenAI API with function calling
            response = await self.client.chat.completions.create(
                model="o4-mini",
                messages=messages,
                tools=[{"type": "function", "function": chat_function}],  # type: ignore
                tool_choice={  # type: ignore
                    "type": "function",
                    "function": {"name": "respond_to_onsale_analysis_question"},
                },
            )

            # Extract function call response
            if response.choices[0].message.tool_calls:
                function_call = response.choices[0].message.tool_calls[0]
                if function_call.function.name == "respond_to_onsale_analysis_question":
                    function_args = json.loads(function_call.function.arguments)
                    return function_args.get(
                        "response", "I couldn't generate a response. Please try again."
                    )

            # Fallback to content if no function call
            content = response.choices[0].message.content
            if not content:
                return "I couldn't generate a response. Please try again."
            return content

        except Exception as e:
            logger.error(f"Error processing chat message: {e}")
            return f"I encountered an error processing your message. Please try again."

    async def process_chat_message_stream(
        self,
        message: str,
        analysis: OnsaleEmailAnalysisItem,
        chat_history: List[ChatMessage],
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """Process a chat message and stream the response"""

        try:
            # Convert chat history to format expected by prompt builder
            history_dict = [
                {"role": msg.role.value, "content": msg.content}
                for msg in chat_history
            ]

            # Build system prompt with context using PromptBuilder
            system_prompt = self.prompt_builder.build_chat_prompt(message, analysis, history_dict)

            # Build messages array
            messages: List[ChatCompletionMessageParam] = [
                {"role": "system", "content": system_prompt}
            ]

            # Call OpenAI API with streaming
            stream = await self.client.chat.completions.create(
                model="o4-mini",
                messages=messages,
                stream=True,
            )

            # Stream the response chunks
            async for chunk in stream:
                if chunk.choices[0].delta.content is not None:
                    yield {"type": "content", "content": chunk.choices[0].delta.content}

        except Exception as e:
            logger.error(f"Error streaming chat message: {e}")
            yield {
                "type": "error",
                "message": f"I encountered an error processing your message: {str(e)}",
            }
