from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field
from datetime import datetime


class OnsaleEmailAnalysisItem(BaseModel):
    id: str
    email_id: str
    email_subject: str
    email_from: str
    email_to: str
    email_ts: Optional[datetime]
    analysis_generated_at: datetime
    event_name: str
    venue_name: Optional[str]
    venue_location: Optional[str]
    performer: Optional[str]
    event_type: Optional[str]
    event_url: Optional[str]
    opportunity_score: float
    confidence_percentage: Optional[float]
    target_margin_percentage: Optional[float]
    risk_factors: List[str]
    opportunities: List[str]
    reasoning_summary: Optional[str]
    historical_context: Optional[str]
    buying_guidance: Optional[str]
    risk_management: Optional[str]
    next_steps: Optional[str]
    market_volatility_level: Optional[str]
    demand_uncertainty_level: Optional[str]
    competition_level: Optional[str]
    recommended_buy_amount_min: Optional[int]
    recommended_buy_amount_max: Optional[int]
    target_resale_markup_percentage: Optional[float]
    stop_loss_percentage: Optional[float]
    created_at: datetime
    updated_at: datetime
    onsale_date: Optional[datetime]
    presale_date: Optional[datetime]
    discount_code: Optional[str]
    buyability_score: Optional[float]
    event_date: Optional[datetime]
    event_date_timezone: Optional[str]
    onsale_date_timezone: Optional[str]
    presale_date_timezone: Optional[str]
    overall_opportunity_score: Optional[int]
    estimated_total_profit: Optional[float]
    additional_details: Optional[Dict[str, Any]] = None


class OnsaleEmailAnalysisResponse(BaseModel):
    items: List[OnsaleEmailAnalysisItem]
    total: int


class OnsaleEmailAnalysisSummary(BaseModel):
    total_analyses: int
    average_opportunity_score: float
    high_opportunity_count: int
    medium_opportunity_count: int
    low_opportunity_count: int
    hot_opportunity_count: int
    great_opportunity_count: int
    good_opportunity_count: int
    pass_opportunity_count: int
    top_performers: List[dict] = Field(description="List of top performers with avg_score and count")
    top_venues: List[dict] = Field(description="List of top venues with avg_score and count")
    event_type_distribution: List[dict] = Field(description="List of event types with count")
    market_volatility_distribution: List[dict] = Field(description="List of volatility levels with count")
    recent_analyses: List[OnsaleEmailAnalysisItem]


class FilterOptionsResponse(BaseModel):
    items: List[str]
    total: int
