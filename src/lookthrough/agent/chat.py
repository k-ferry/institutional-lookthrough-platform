"""AI agent for answering natural language questions about portfolio exposures.

This module provides an AI chat interface that uses tool-calling to query portfolio
data. Supports three providers: Claude (Anthropic), OpenAI, and Ollama (local).

The agent follows the AI Contract (docs/ai_contract.md):
- Never fabricates holdings or exposure data
- Surfaces uncertainty and confidence levels
- Uses provided tools to query data before answering
"""
from __future__ import annotations

import inspect
import json
import logging
from typing import Any, Callable, Optional

from src.lookthrough.agent.tools import TOOLS_REGISTRY

logger = logging.getLogger(__name__)

# ============================================================================
# SYSTEM PROMPT
# ============================================================================

SYSTEM_PROMPT = """You are an institutional portfolio exposure analyst assistant.

Your role is to help users understand their portfolio's look-through exposures,
including sector, industry, geography, fund-level, and company-level breakdowns.

Key principles:
1. Always surface uncertainty and confidence levels in your answers. Never present
   low-confidence data as certain. When data has low confidence or coverage gaps,
   explicitly mention it (e.g., "Note: 15% of the portfolio has unclassified sector
   exposure" or "Classification confidence for Healthcare is only 65%").

2. Use the provided tools to query portfolio data BEFORE answering questions.
   Do not guess or fabricate holdings, exposure values, or percentages.

3. For analytical questions about risks, market context, or investment implications,
   clearly label your analysis as AI-generated insight separate from portfolio facts.
   Use phrases like "Based on the data, my analysis suggests..." or "From an analytical
   perspective..."

4. When presenting numbers, be precise but readable. Use appropriate units (USD, %),
   round sensibly, and format large numbers (e.g., $1.2M instead of $1,234,567).

5. If a question cannot be answered with the available tools or data, say so clearly
   rather than speculating.

Available capabilities:
- Portfolio summary and high-level metrics
- Sector, industry, and geography exposure breakdowns
- Fund-level and company-level exposure details
- Review queue items requiring attention
- Confidence distribution analysis
"""


# ============================================================================
# TOOL CONVERSION
# ============================================================================

def _get_type_schema(annotation: Any) -> dict:
    """Convert Python type annotation to JSON schema type."""
    if annotation is inspect.Parameter.empty or annotation is None:
        return {"type": "string"}

    # Handle Optional types
    origin = getattr(annotation, "__origin__", None)
    if origin is type(None):
        return {"type": "null"}

    # Handle Optional[X] which is Union[X, None]
    if origin is type(None) or str(origin) == "typing.Union":
        args = getattr(annotation, "__args__", ())
        # Filter out NoneType
        non_none_args = [a for a in args if a is not type(None)]
        if non_none_args:
            return _get_type_schema(non_none_args[0])
        return {"type": "string"}

    # Basic types
    if annotation is str:
        return {"type": "string"}
    if annotation is int:
        return {"type": "integer"}
    if annotation is float:
        return {"type": "number"}
    if annotation is bool:
        return {"type": "boolean"}

    # Default to string
    return {"type": "string"}


def _generate_json_schema(func: Callable) -> dict:
    """Generate JSON schema for function parameters using inspect module."""
    sig = inspect.signature(func)
    doc = func.__doc__ or ""

    # Parse Args section from docstring for descriptions
    arg_descriptions = {}
    in_args = False
    current_arg = None
    for line in doc.split("\n"):
        stripped = line.strip()
        if stripped.startswith("Args:"):
            in_args = True
            continue
        if in_args:
            if stripped.startswith("Returns:") or stripped.startswith("Raises:"):
                break
            # Check for argument definition (name: description)
            if ":" in stripped and not stripped.startswith("-"):
                parts = stripped.split(":", 1)
                if len(parts) == 2:
                    arg_name = parts[0].strip()
                    arg_desc = parts[1].strip()
                    arg_descriptions[arg_name] = arg_desc
                    current_arg = arg_name
            elif current_arg and stripped:
                # Continuation of previous arg description
                arg_descriptions[current_arg] += " " + stripped

    properties = {}
    required = []

    for param_name, param in sig.parameters.items():
        if param_name in ("self", "cls"):
            continue

        prop = _get_type_schema(param.annotation)
        if param_name in arg_descriptions:
            prop["description"] = arg_descriptions[param_name]

        properties[param_name] = prop

        # If no default, it's required
        if param.default is inspect.Parameter.empty:
            required.append(param_name)

    schema = {
        "type": "object",
        "properties": properties,
    }
    if required:
        schema["required"] = required

    return schema


def convert_tools_for_provider(provider: str) -> list:
    """Convert TOOLS_REGISTRY to the format expected by each provider.

    Args:
        provider: One of "claude", "openai", or "ollama".

    Returns:
        List of tool definitions in provider-specific format.
    """
    tools = []

    for tool_def in TOOLS_REGISTRY:
        name = tool_def["name"]
        description = tool_def["description"]
        func = tool_def["function"]
        schema = _generate_json_schema(func)

        if provider == "claude":
            # Anthropic tool-calling format
            tools.append({
                "name": name,
                "description": description,
                "input_schema": schema,
            })
        elif provider in ("openai", "ollama"):
            # OpenAI function-calling format (Ollama uses same format)
            tools.append({
                "type": "function",
                "function": {
                    "name": name,
                    "description": description,
                    "parameters": schema,
                },
            })
        else:
            raise ValueError(f"Unknown provider: {provider}")

    return tools


# ============================================================================
# TOOL EXECUTION
# ============================================================================

def execute_tool(tool_name: str, tool_args: dict) -> dict:
    """Execute a tool by name with the provided arguments.

    Args:
        tool_name: Name of the tool to execute (must exist in TOOLS_REGISTRY).
        tool_args: Dictionary of arguments to pass to the tool function.

    Returns:
        Dictionary with tool result or error information.
    """
    # Find the tool
    tool_func = None
    for tool_def in TOOLS_REGISTRY:
        if tool_def["name"] == tool_name:
            tool_func = tool_def["function"]
            break

    if tool_func is None:
        return {"error": f"Unknown tool: {tool_name}"}

    try:
        result = tool_func(**tool_args)
        return result
    except TypeError as e:
        logger.warning(f"Tool {tool_name} argument error: {e}")
        return {"error": f"Invalid arguments for {tool_name}: {e}"}
    except Exception as e:
        logger.exception(f"Tool {tool_name} execution error")
        return {"error": f"Tool execution failed: {e}"}


# ============================================================================
# CLAUDE (ANTHROPIC)
# ============================================================================

async def chat_with_claude(
    message: str,
    conversation_history: Optional[list] = None,
) -> dict:
    """Chat using Claude (Anthropic) with tool-calling support.

    Args:
        message: User message to process.
        conversation_history: Optional list of previous messages for context.

    Returns:
        Dictionary with response, tools_used, and provider.
    """
    try:
        import anthropic
    except ImportError:
        return {
            "response": "Anthropic SDK not installed. Run: pip install anthropic",
            "tools_used": [],
            "provider": "claude",
        }

    client = anthropic.Anthropic()
    tools = convert_tools_for_provider("claude")
    tools_used = []

    # Build messages list
    messages = []
    if conversation_history:
        messages.extend(conversation_history)
    messages.append({"role": "user", "content": message})

    # Initial request
    response = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=4096,
        system=SYSTEM_PROMPT,
        tools=tools,
        messages=messages,
    )

    # Handle tool calls in a loop (model may call multiple tools)
    while response.stop_reason == "tool_use":
        # Process all tool calls in the response
        tool_results = []
        assistant_content = response.content

        for block in assistant_content:
            if block.type == "tool_use":
                tool_name = block.name
                tool_args = block.input
                tool_id = block.id

                logger.info(f"Claude calling tool: {tool_name} with args: {tool_args}")
                tools_used.append({"name": tool_name, "args": tool_args})

                result = execute_tool(tool_name, tool_args)

                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": tool_id,
                    "content": json.dumps(result),
                })

        # Add assistant message and tool results
        messages.append({"role": "assistant", "content": assistant_content})
        messages.append({"role": "user", "content": tool_results})

        # Continue conversation
        response = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=4096,
            system=SYSTEM_PROMPT,
            tools=tools,
            messages=messages,
        )

    # Extract final text response
    final_text = ""
    for block in response.content:
        if hasattr(block, "text"):
            final_text += block.text

    return {
        "response": final_text,
        "tools_used": tools_used,
        "provider": "claude",
    }


# ============================================================================
# OPENAI
# ============================================================================

async def chat_with_openai(
    message: str,
    conversation_history: Optional[list] = None,
) -> dict:
    """Chat using OpenAI with function-calling support.

    Args:
        message: User message to process.
        conversation_history: Optional list of previous messages for context.

    Returns:
        Dictionary with response, tools_used, and provider.
    """
    try:
        from openai import OpenAI
    except ImportError:
        return {
            "response": "OpenAI SDK not installed. Run: pip install openai",
            "tools_used": [],
            "provider": "openai",
        }

    client = OpenAI()
    tools = convert_tools_for_provider("openai")
    tools_used = []

    # Build messages list with system prompt
    messages = [{"role": "system", "content": SYSTEM_PROMPT}]
    if conversation_history:
        messages.extend(conversation_history)
    messages.append({"role": "user", "content": message})

    # Initial request
    response = client.chat.completions.create(
        model="gpt-4o",
        messages=messages,
        tools=tools,
    )

    choice = response.choices[0]

    # Handle tool calls in a loop
    while choice.finish_reason == "tool_calls":
        assistant_message = choice.message
        messages.append(assistant_message)

        # Process all tool calls
        for tool_call in assistant_message.tool_calls:
            tool_name = tool_call.function.name
            tool_args = json.loads(tool_call.function.arguments)

            logger.info(f"OpenAI calling tool: {tool_name} with args: {tool_args}")
            tools_used.append({"name": tool_name, "args": tool_args})

            result = execute_tool(tool_name, tool_args)

            messages.append({
                "role": "tool",
                "tool_call_id": tool_call.id,
                "content": json.dumps(result),
            })

        # Continue conversation
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=messages,
            tools=tools,
        )
        choice = response.choices[0]

    return {
        "response": choice.message.content or "",
        "tools_used": tools_used,
        "provider": "openai",
    }


# ============================================================================
# OLLAMA (LOCAL)
# ============================================================================

async def chat_with_ollama(
    message: str,
    conversation_history: Optional[list] = None,
    model: str = "llama3.1",
) -> dict:
    """Chat using Ollama local API with tool-calling support.

    Args:
        message: User message to process.
        conversation_history: Optional list of previous messages for context.
        model: Ollama model to use (default: llama3.1).

    Returns:
        Dictionary with response, tools_used, and provider.
    """
    try:
        import httpx
    except ImportError:
        return {
            "response": "httpx not installed. Run: pip install httpx",
            "tools_used": [],
            "provider": "ollama",
        }

    base_url = "http://localhost:11434/api/chat"
    tools = convert_tools_for_provider("ollama")
    tools_used = []

    # Build messages list with system prompt
    messages = [{"role": "system", "content": SYSTEM_PROMPT}]
    if conversation_history:
        messages.extend(conversation_history)
    messages.append({"role": "user", "content": message})

    async with httpx.AsyncClient(timeout=120.0) as client:
        # Initial request
        payload = {
            "model": model,
            "messages": messages,
            "tools": tools,
            "stream": False,
        }

        try:
            response = await client.post(base_url, json=payload)
            response.raise_for_status()
        except httpx.ConnectError:
            return {
                "response": "Cannot connect to Ollama. Is it running at localhost:11434?",
                "tools_used": [],
                "provider": "ollama",
            }
        except httpx.HTTPStatusError as e:
            return {
                "response": f"Ollama API error: {e.response.status_code}",
                "tools_used": [],
                "provider": "ollama",
            }

        data = response.json()
        assistant_message = data.get("message", {})

        # Handle tool calls in a loop
        while assistant_message.get("tool_calls"):
            messages.append(assistant_message)

            # Process all tool calls
            for tool_call in assistant_message["tool_calls"]:
                func = tool_call.get("function", {})
                tool_name = func.get("name", "")
                tool_args = func.get("arguments", {})

                # Arguments may be a string that needs parsing
                if isinstance(tool_args, str):
                    try:
                        tool_args = json.loads(tool_args)
                    except json.JSONDecodeError:
                        tool_args = {}

                logger.info(f"Ollama calling tool: {tool_name} with args: {tool_args}")
                tools_used.append({"name": tool_name, "args": tool_args})

                result = execute_tool(tool_name, tool_args)

                messages.append({
                    "role": "tool",
                    "content": json.dumps(result),
                })

            # Continue conversation
            payload = {
                "model": model,
                "messages": messages,
                "tools": tools,
                "stream": False,
            }

            response = await client.post(base_url, json=payload)
            response.raise_for_status()
            data = response.json()
            assistant_message = data.get("message", {})

        return {
            "response": assistant_message.get("content", ""),
            "tools_used": tools_used,
            "provider": "ollama",
        }


# ============================================================================
# UNIFIED CHAT INTERFACE
# ============================================================================

async def chat(
    message: str,
    provider: str = "claude",
    conversation_history: Optional[list] = None,
) -> dict:
    """Route chat requests to the appropriate provider.

    Args:
        message: User message to process.
        provider: Provider to use ("claude", "openai", or "ollama").
        conversation_history: Optional list of previous messages for context.

    Returns:
        Dictionary with response, tools_used, and provider.
    """
    try:
        if provider == "claude":
            return await chat_with_claude(message, conversation_history)
        elif provider == "openai":
            return await chat_with_openai(message, conversation_history)
        elif provider == "ollama":
            return await chat_with_ollama(message, conversation_history)
        else:
            return {
                "response": f"Unknown provider: {provider}. Use 'claude', 'openai', or 'ollama'.",
                "tools_used": [],
                "provider": provider,
            }
    except Exception as e:
        logger.exception(f"Chat error with provider {provider}")
        return {
            "response": f"Error: {e}",
            "tools_used": [],
            "provider": provider,
        }
