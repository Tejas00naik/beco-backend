"""Batch run management functionality for the batch worker."""

import logging
import uuid
from datetime import datetime
from typing import Optional

# Import models
from src.models.schemas import BatchRun, BatchRunStatus

logger = logging.getLogger(__name__)


class BatchManager:
    """
    Handles batch run management operations for the batch worker.
    """
    
    def __init__(self, dao, is_test: bool = False, mailbox_id: str = "default", run_mode: str = "incremental"):
        """
        Initialize the batch manager.
        
        Args:
            dao: Firestore DAO instance
            is_test: If True, use test mode prefix for collections
            mailbox_id: ID of the mailbox being processed
            run_mode: Run mode (incremental or full_refresh)
        """
        self.dao = dao
        self.is_test = is_test
        self.mailbox_id = mailbox_id
        self.run_mode = run_mode
        self.batch_run = None
        self.emails_processed = 0
        self.errors = 0
    
    async def start_batch_run(self):
        """
        Start a new batch run and log it.
        
        Returns:
            Batch run ID
        """
        try:
            # Create unique batch run ID
            run_id = str(uuid.uuid4())
            
            # Create BatchRun object
            self.batch_run = BatchRun(
                run_id=run_id,
                start_ts=datetime.utcnow(),
                status=BatchRunStatus.PARTIAL,  # Use PARTIAL for in-progress runs
                emails_processed=0,
                errors=0,
                mailbox_id=self.mailbox_id,
                run_mode=self.run_mode
            )
            
            # Add to Firestore
            await self.dao.add_document("batch_run", run_id, self.batch_run.__dict__)
            logger.info(f"Started batch run with ID {run_id}")
            
            return run_id
            
        except Exception as e:
            logger.error(f"Failed to start batch run: {str(e)}")
            raise
    
    async def finish_batch_run(self):
        """
        Complete the batch run and update the status.
        """
        if not self.batch_run:
            logger.warning("finish_batch_run called without active batch run")
            return
            
        try:
            # Update BatchRun object
            await self.dao.update_document("batch_run", self.batch_run.run_id, {
                "end_ts": datetime.utcnow(),
                "status": BatchRunStatus.SUCCESS,
                "emails_processed": self.emails_processed,
                "errors": self.errors
            })
            
            logger.info(f"Completed batch run {self.batch_run.run_id} with {self.emails_processed} emails processed, {self.errors} errors")
            
        except Exception as e:
            logger.error(f"Failed to finish batch run: {str(e)}")
    
    def increment_processed_count(self):
        """
        Increment the count of successfully processed emails.
        """
        self.emails_processed += 1
    
    def increment_error_count(self):
        """
        Increment the error count.
        """
        self.errors += 1
