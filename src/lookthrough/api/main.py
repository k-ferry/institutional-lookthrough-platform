"""FastAPI application exposing agent tools and chat endpoint as REST API.

This module provides REST endpoints for portfolio analysis tools and an AI chat
interface. The tools query Gold tables and return structured JSON responses.

Start the server with:
    uvicorn src.lookthrough.api.main:app --reload
"""
from __future__ import annotations

import os
from typing import Optional

from fastapi import Depends, FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from src.lookthrough.agent.tools import (
    get_company_exposure,
    get_confidence_distribution,
    get_fund_exposure,
    get_geography_exposure,
    get_industry_exposure,
    get_portfolio_summary,
    get_review_queue,
    get_sector_exposure,
)

app = FastAPI(
    title="Institutional Lookthrough Platform API",
    description="REST API for portfolio exposure analysis and AI-powered insights",
    version="1.0.0",
)

# CORS middleware - allow all origins for development
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ----------------------------------------------------------------------------
# Authentication
# ----------------------------------------------------------------------------

async def verify_api_key(x_api_key: Optional[str] = Header(None)) -> None:
    """Verify API key from header against environment variable.

    If PLATFORM_API_KEY is not set, all requests are allowed (dev mode).
    """
    expected_key = os.environ.get("PLATFORM_API_KEY")
    if expected_key is None:
        # Dev mode - no auth required
        return
    if x_api_key != expected_key:
        raise HTTPException(status_code=401, detail="Invalid or missing API key")


# ----------------------------------------------------------------------------
# Request/Response Models
# ----------------------------------------------------------------------------

class ChatRequest(BaseModel):
    """Request body for chat endpoint."""

    message: str
    provider: str = "claude"  # "claude", "openai", or "ollama"


class ChatResponse(BaseModel):
    """Response body for chat endpoint."""

    response: str
    provider: str


class HealthResponse(BaseModel):
    """Response body for health endpoint."""

    status: str


# ----------------------------------------------------------------------------
# Health Endpoint
# ----------------------------------------------------------------------------

@app.get("/api/v1/health", response_model=HealthResponse)
async def health() -> dict:
    """Health check endpoint."""
    return {"status": "ok"}


# ----------------------------------------------------------------------------
# Exposure Endpoints
# ----------------------------------------------------------------------------

@app.get("/api/v1/exposure/sector")
async def exposure_sector(
    as_of_date: Optional[str] = None,
    _: None = Depends(verify_api_key),
) -> dict:
    """Get portfolio exposure breakdown by sector."""
    return get_sector_exposure(as_of_date=as_of_date)


@app.get("/api/v1/exposure/industry")
async def exposure_industry(
    sector: Optional[str] = None,
    as_of_date: Optional[str] = None,
    _: None = Depends(verify_api_key),
) -> dict:
    """Get portfolio exposure breakdown by industry."""
    return get_industry_exposure(sector=sector, as_of_date=as_of_date)


@app.get("/api/v1/exposure/geography")
async def exposure_geography(
    as_of_date: Optional[str] = None,
    _: None = Depends(verify_api_key),
) -> dict:
    """Get portfolio exposure breakdown by geography."""
    return get_geography_exposure(as_of_date=as_of_date)


@app.get("/api/v1/exposure/fund")
async def exposure_fund(
    fund_name: Optional[str] = None,
    _: None = Depends(verify_api_key),
) -> dict:
    """Get portfolio exposure breakdown by fund."""
    return get_fund_exposure(fund_name=fund_name)


@app.get("/api/v1/exposure/company")
async def exposure_company(
    company_name: Optional[str] = None,
    top_n: int = 20,
    _: None = Depends(verify_api_key),
) -> dict:
    """Get portfolio exposure breakdown by company."""
    return get_company_exposure(company_name=company_name, top_n=top_n)


# ----------------------------------------------------------------------------
# Review Queue Endpoint
# ----------------------------------------------------------------------------

@app.get("/api/v1/review-queue")
async def review_queue(
    status: str = "pending",
    priority: Optional[str] = None,
    _: None = Depends(verify_api_key),
) -> dict:
    """Get items in the review queue requiring human attention."""
    return get_review_queue(status=status, priority=priority)


# ----------------------------------------------------------------------------
# Portfolio Summary Endpoint
# ----------------------------------------------------------------------------

@app.get("/api/v1/portfolio/summary")
async def portfolio_summary(
    as_of_date: Optional[str] = None,
    _: None = Depends(verify_api_key),
) -> dict:
    """Get a high-level summary of the portfolio."""
    return get_portfolio_summary(as_of_date=as_of_date)


# ----------------------------------------------------------------------------
# Confidence Distribution Endpoint
# ----------------------------------------------------------------------------

@app.get("/api/v1/confidence")
async def confidence(
    taxonomy_type: str = "sector",
    _: None = Depends(verify_api_key),
) -> dict:
    """Get confidence statistics per taxonomy bucket."""
    return get_confidence_distribution(taxonomy_type=taxonomy_type)


# ----------------------------------------------------------------------------
# Chat Endpoint
# ----------------------------------------------------------------------------

@app.post("/api/v1/chat", response_model=ChatResponse)
async def chat(
    request: ChatRequest,
    _: None = Depends(verify_api_key),
) -> dict:
    """AI chat endpoint for natural language portfolio queries.

    Currently returns a placeholder response. AI agent integration coming next.
    """
    return {
        "response": "Chat endpoint ready. AI agent integration coming next.",
        "provider": request.provider,
    }
