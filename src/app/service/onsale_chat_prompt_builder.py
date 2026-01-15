"""
OnSale Chat Prompt Builder Service
Single Responsibility: Build contextual prompts for AI chat interactions
"""

from typing import Dict, Any, List
from app.model.onsale_email_analysis import OnsaleEmailAnalysisItem


class OnSaleChatPromptBuilder:
    """Single responsibility: Build prompts for onsale email analysis chat"""

    def build_system_prompt(self, analysis: OnsaleEmailAnalysisItem) -> str:
        """Build the system prompt with analysis context"""
        
        # Format dates for better readability
        event_date_str = analysis.event_date.strftime("%B %d, %Y") if analysis.event_date else "Not specified"
        onsale_date_str = analysis.onsale_date.strftime("%B %d, %Y") if analysis.onsale_date else "Not specified"
        presale_date_str = analysis.presale_date.strftime("%B %d, %Y") if analysis.presale_date else "Not specified"
        email_date_str = analysis.email_ts.strftime("%B %d, %Y at %I:%M %p") if analysis.email_ts else "Not specified"
        
        # Build risk factors and opportunities lists
        risk_factors_text = "\n".join([f"• {risk}" for risk in analysis.risk_factors]) if analysis.risk_factors else "None identified"
        opportunities_text = "\n".join([f"• {opp}" for opp in analysis.opportunities]) if analysis.opportunities else "None identified"
        
        system_prompt = f"""You are an expert ticket resale analyst assistant. You have access to detailed analysis of an onsale email for a specific event. Use this information to provide helpful, accurate, and actionable advice.

## EVENT INFORMATION
**Event:** {analysis.event_name}
**Performer:** {analysis.performer or "Not specified"}
**Venue:** {analysis.venue_name or "Not specified"}
**Location:** {analysis.venue_location or "Not specified"}
**Event Type:** {analysis.event_type or "Not specified"}
**Event Date:** {event_date_str}
**Event URL:** {analysis.event_url or "Not available"}

## TIMING INFORMATION
**Email Received:** {email_date_str}
**Onsale Date:** {onsale_date_str}
**Presale Date:** {presale_date_str}
**Analysis Generated:** {analysis.analysis_generated_at.strftime("%B %d, %Y at %I:%M %p")}

## FINANCIAL ANALYSIS
**Opportunity Score:** {analysis.opportunity_score}/100
**Buyability Score:** {analysis.buyability_score or "Not available"}/100
**Overall Opportunity Score:** {analysis.overall_opportunity_score or "Not available"}/100
**Target Margin:** {analysis.target_margin_percentage or "Not available"}%
**Recommended Buy Range:** ${analysis.recommended_buy_amount_min or "Not available"} - ${analysis.recommended_buy_amount_max or "Not available"}
**Target Resale Markup:** {analysis.target_resale_markup_percentage or "Not available"}%
**Stop Loss:** {analysis.stop_loss_percentage or "Not available"}%

## MARKET CONDITIONS
**Market Volatility:** {analysis.market_volatility_level or "Not available"}
**Demand Uncertainty:** {analysis.demand_uncertainty_level or "Not available"}
**Competition Level:** {analysis.competition_level or "Not available"}

## RISK FACTORS
{risk_factors_text}

## OPPORTUNITIES
{opportunities_text}

## AI ANALYSIS
**Reasoning Summary:** {analysis.reasoning_summary or "Not available"}

**Historical Context:** {analysis.historical_context or "Not available"}

**Buying Guidance:** {analysis.buying_guidance or "Not available"}

**Risk Management:** {analysis.risk_management or "Not available"}

**Next Steps:** {analysis.next_steps or "Not available"}

## YOUR ROLE
You are a knowledgeable assistant that can help users understand this analysis and make informed decisions about ticket resale opportunities. You can:

1. **Explain the analysis** - Help users understand what the scores and metrics mean
2. **Provide insights** - Offer additional context about market conditions, timing, and strategy
3. **Answer questions** - Respond to specific questions about the event, venue, performer, or market
4. **Give recommendations** - Suggest strategies based on the analysis data
5. **Clarify risks** - Help users understand potential risks and how to mitigate them

Always be helpful, accurate, and provide actionable advice. Use the analysis data to support your responses, but also acknowledge when information might be limited or uncertain.

Remember: You're helping with ticket resale decisions, so be clear about risks and never guarantee profits. Focus on providing informed analysis and strategic guidance."""

        return system_prompt

    def build_chat_prompt(self, user_message: str, analysis: OnsaleEmailAnalysisItem, chat_history: List[Dict[str, Any]]) -> str:
        """Build a chat prompt with context and history"""
        
        system_prompt = self.build_system_prompt(analysis)
        
        # Build conversation history
        conversation_history = ""
        if chat_history:
            conversation_history = "\n\n## CONVERSATION HISTORY\n"
            for msg in chat_history[-5:]:  # Keep last 5 messages for context
                role = "User" if msg["role"] == "user" else "Assistant"
                conversation_history += f"**{role}:** {msg['content']}\n\n"
        
        full_prompt = f"{system_prompt}{conversation_history}\n\n## CURRENT QUESTION\n**User:** {user_message}\n\n**Assistant:**"
        
        return full_prompt
