"""AI agent chat endpoint — wraps the agent.chat module behind JWT auth."""
from __future__ import annotations

import logging
from typing import Optional

from fastapi import APIRouter, Depends
from pydantic import BaseModel

from src.lookthrough.agent.chat import chat as agent_chat
from src.lookthrough.auth.dependencies import get_current_user
from src.lookthrough.db.models import User

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/agent", tags=["agent"])


class ChatRequest(BaseModel):
    message: str
    conversation_history: Optional[list] = None


class ChatResponse(BaseModel):
    response: str
    sources: list


@router.post("/chat", response_model=ChatResponse)
async def chat(
    request: ChatRequest,
    _current_user: User = Depends(get_current_user),
) -> dict:
    """Natural-language portfolio query endpoint.

    Routes the user message through the AI agent (Claude by default), which
    uses tool-calling to query live portfolio data before answering.

    Returns the agent's text response and the list of data-query tools it
    called (surfaced to the UI as 'sources').
    """
    try:
        result = await agent_chat(
            message=request.message,
            conversation_history=request.conversation_history,
        )
        return {
            "response": result.get("response", ""),
            "sources": result.get("tools_used", []),
        }
    except Exception:
        logger.exception("Agent chat error")
        return {
            "response": (
                "I'm sorry, I encountered an error while processing your request. "
                "Please try again, or contact support if the issue persists."
            ),
            "sources": [],
        }
