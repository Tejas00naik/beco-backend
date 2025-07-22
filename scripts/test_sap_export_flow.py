#!/usr/bin/env python
"""
End-to-end test for the BatchWorkerV2 to SAP Export flow.

This script tests:
1. Processing a payment advice with BatchWorkerV2
2. Enriching payment advice lines with BP/GL codes
3. Generating SAP export
4. Uploading to GCS with a presigned URL
5. Updating payment_advice record with the URL
"""

import os
import sys
import asyncio
import logging
import argparse
from dotenv import load_dotenv
from datetime import datetime
from uuid import uuid4

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.batch_worker.batch_worker_v2 import BatchWorkerV2
from src.services.account_enrichment_service import AccountEnrichmentService
from src.services.sap_export_service import SAPExportService
from src.repositories.firestore_dao import FirestoreDAO
from src.repositories.payment_advice_repository import PaymentAdviceRepository

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def run_batch_worker(email_id=None):
    """
    Run the BatchWorkerV2 to process emails.
    
    Args:
        email_id: Optional specific email ID to process
    
    Returns:
        List of payment advice UUIDs created
    """
    try:
        logger.info("Initializing BatchWorkerV2")
        batch_worker = BatchWorkerV2()
        
        if email_id:
            # Process specific email
            logger.info(f"Processing specific email: {email_id}")
            email_data = {"email_id": email_id}
            success = await batch_worker.process_email(email_data)
            if not success:
                logger.error(f"Failed to process email {email_id}")
                return []
                
            # Get payment advice UUIDs from the batch worker
            # This is a simplified approach as we don't have direct access to the created payment advices
            # We'll query them from Firestore instead in the next step
        else:
            # Process the last email using the built-in method
            logger.info("Processing last unprocessed email")
            await batch_worker.process_last_email()
        
        # Get the most recent payment advice UUIDs from Firestore
        # Limited to 5 to avoid too many results but ensure we get the ones just created
        dao = FirestoreDAO()
        recent_advices = await dao.query_documents("payment_advice", [], limit=5, order_by="created_at", direction="desc")
        
        if not recent_advices:
            logger.warning("No payment advices found after processing")
            return []
            
        payment_advice_uuids = [advice.get("payment_advice_uuid") for advice in recent_advices if advice.get("payment_advice_uuid")]
        logger.info(f"Found {len(payment_advice_uuids)} recent payment advices")
        return payment_advice_uuids
    except Exception as e:
        logger.error(f"Error running BatchWorkerV2: {str(e)}")
        return []

async def enrich_payment_advice_lines(payment_advice_uuid, dao):
    """
    Enrich payment advice lines with BP/GL codes.
    
    Args:
        payment_advice_uuid: UUID of the payment advice to enrich
        dao: FirestoreDAO instance
    
    Returns:
        List of enriched payment advice lines
    """
    try:
        logger.info(f"Enriching payment advice lines for {payment_advice_uuid}")
        
        # Create enrichment service
        enrichment_service = AccountEnrichmentService(dao)
        
        # Get payment advice lines for the given payment advice
        payment_advice_lines = await dao.query_documents(
            "paymentadvice_lines", 
            [("payment_advice_uuid", "==", payment_advice_uuid)]
        )
        
        if not payment_advice_lines:
            logger.warning(f"No payment advice lines found for {payment_advice_uuid}")
            return []
            
        logger.info(f"Found {len(payment_advice_lines)} payment advice lines to enrich")
        
        # Enrich the lines
        enriched_lines = await enrichment_service.enrich_payment_advice_lines(payment_advice_uuid, payment_advice_lines)
        
        # Update the lines in Firestore
        for line in enriched_lines:
            line_uuid = line.get("payment_advice_line_uuid")
            if not line_uuid:
                logger.warning("Payment advice line missing UUID, skipping update")
                continue
                
            await dao.update_document("paymentadvice_lines", line_uuid, line)
            
        logger.info(f"Enriched and updated {len(enriched_lines)} payment advice lines")
        return enriched_lines
    except Exception as e:
        logger.error(f"Error enriching payment advice lines: {str(e)}")
        return []

async def generate_sap_export(payment_advice_uuid, dao):
    """
    Generate SAP export for a payment advice.
    
    Args:
        payment_advice_uuid: UUID of the payment advice to export
        dao: FirestoreDAO instance
    
    Returns:
        Export URL if successful, None otherwise
    """
    try:
        logger.info(f"Generating SAP export for payment advice {payment_advice_uuid}")
        
        # Create SAP export service
        sap_export_service = SAPExportService(dao)
        
        # Generate export
        export_url = await sap_export_service.generate_sap_export_for_payment_advice(payment_advice_uuid)
        
        if export_url:
            logger.info(f"SAP export generated successfully, URL: {export_url}")
        else:
            logger.warning("Failed to generate SAP export")
            
        return export_url
    except Exception as e:
        logger.error(f"Error generating SAP export: {str(e)}")
        return None

async def main():
    """Main function to run the end-to-end test."""
    try:
        # Parse arguments
        parser = argparse.ArgumentParser(description='Test BatchWorkerV2 to SAP Export flow')
        parser.add_argument('--email-id', '-e', type=str, help='Specific email ID to process')
        parser.add_argument('--gcs-bucket', '-g', type=str, help='GCS bucket name for exports')
        args = parser.parse_args()
        
        # Load environment variables
        dotenv_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.env')
        load_dotenv(dotenv_path)
        
        # Get Firestore project ID
        project_id = os.environ.get('FIRESTORE_PROJECT_ID')
        if not project_id:
            logger.error("Firestore project ID not provided in env vars")
            return
        
        # Set GCS bucket for exports (from args or env vars)
        gcs_bucket = args.gcs_bucket or os.environ.get('GCP_STORAGE_BUCKET', 'beco-mails')
        os.environ['GCP_STORAGE_BUCKET'] = gcs_bucket
            
        logger.info(f"Starting end-to-end test with project ID: {project_id}")
        logger.info(f"Using GCS bucket: {gcs_bucket} for exports")
        
        # Initialize DAO
        dao = FirestoreDAO(project_id=project_id)
        
        # 1. Run BatchWorkerV2
        payment_advice_uuids = await run_batch_worker(args.email_id)
        
        if not payment_advice_uuids:
            logger.error("No payment advices were created, test cannot continue")
            return
            
        logger.info(f"Created {len(payment_advice_uuids)} payment advices")
        
        # Process each payment advice
        for pa_uuid in payment_advice_uuids:
            logger.info(f"Processing payment advice {pa_uuid}")
            
            # 2. Enrich payment advice lines
            enriched_lines = await enrich_payment_advice_lines(pa_uuid, dao)
            
            if not enriched_lines:
                logger.warning(f"No enriched lines for payment advice {pa_uuid}, skipping export")
                continue
                
            # 3. Generate SAP export and upload to GCS
            export_url = await generate_sap_export(pa_uuid, dao)
            
            if export_url:
                logger.info(f"End-to-end test successful for payment advice {pa_uuid}")
                logger.info(f"SAP export URL: {export_url}")
            else:
                logger.error(f"Failed to complete end-to-end test for payment advice {pa_uuid}")
        
        logger.info("End-to-end test completed")
        
    except Exception as e:
        logger.error(f"Error in end-to-end test: {str(e)}")

if __name__ == "__main__":
    asyncio.run(main())
