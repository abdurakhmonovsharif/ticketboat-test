import base64
import os
from typing import Dict, Optional

import requests


class OutboxPdfService:
    """Service for fetching PDF tickets from Outbox by document_name."""

    def __init__(self):
        self.outbox_pdf_base_url = "https://tickets.cirquedusoleil.com/extapi/pdfg_output"

    def fetch_pdf_by_document_name(self, document_name: str) -> Dict:
        """
        Fetch PDF from Outbox CDS by document_name.
        
        Args:
            document_name: The Outbox document_name
            
        Returns:
            Dictionary with document_name, pdf_url, pdf_base64, status, and error_message
        """
        # Fetch PDF from Outbox CDS
        pdf_url = f"{self.outbox_pdf_base_url}/pdfg_{document_name}"
        
        try:
            response = requests.get(pdf_url, timeout=30)
            
            if response.status_code == 200:
                pdf_data = response.content
                pdf_base64 = base64.b64encode(pdf_data).decode("utf-8")
                
                return {
                    "document_name": document_name,
                    "pdf_url": pdf_url,
                    "pdf_base64": pdf_base64,
                    "status": "success",
                    "error_message": None,
                }
            else:
                return {
                    "document_name": document_name,
                    "pdf_url": pdf_url,
                    "pdf_base64": None,
                    "status": "error",
                    "error_message": f"Failed to fetch PDF. Status code: {response.status_code}",
                }
        except Exception as e:
            return {
                "document_name": document_name,
                "pdf_url": pdf_url,
                "pdf_base64": None,
                "status": "error",
                "error_message": f"Error fetching PDF: {str(e)}",
            }
