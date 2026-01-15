"""
Service for triggering on-demand actions in cirque-listing-monitor and marketplace-sync-manager.
Used when users manually change sync status or map new events in the admin portal.
"""
import os
import json
import boto3
from typing import Optional


class SyncTriggerService:
    """
    Triggers event-driven actions via SQS:
    1. On-demand inventory fetch in cirque-listing-monitor (for new mappings)
    2. On-demand syncs in marketplace-sync-manager (for status changes)
    """
    
    def __init__(self):
        """Initialize SQS client and queue URLs from environment."""
        self.sqs = boto3.client("sqs", region_name="us-east-1")
        self.marketplace_sync_queue_url = os.getenv("MARKETPLACE_SYNC_MANAGER_QUEUE_URL")
        self.cirque_listing_monitor_queue_url = os.getenv("CIRQUE_LISTING_MONITOR_QUEUE_URL")
        
        if not self.marketplace_sync_queue_url:
            raise ValueError("Error: MARKETPLACE_SYNC_MANAGER_QUEUE_URL environment variable not set")
        if not self.cirque_listing_monitor_queue_url:
            raise ValueError("Error: CIRQUE_LISTING_MONITOR_QUEUE_URL environment variable not set")
    
    async def trigger_inventory_fetch(self, event_id: str) -> dict:
        """
        Trigger on-demand inventory fetch in cirque-listing-monitor.
        Used when a new event is mapped to immediately fetch inventory.
        
        Args:
            event_id: The outbox_event_id to fetch inventory for
        
        Returns:
            dict with message ID and status
        """
        message_body = {
            "action": "fetch_event_inventory",
            "event_id": event_id,
            "trigger_type": "user_mapping",
        }
        
        try:
            response = self.sqs.send_message(
                QueueUrl=self.cirque_listing_monitor_queue_url,
                MessageBody=json.dumps(message_body),
            )
            
            print(f"✅ Triggered inventory fetch for event {event_id}")
            print(f"   - Message ID: {response['MessageId']}")
            
            return {
                "message_id": response["MessageId"],
                "status": "success",
            }
        
        except Exception as e:
            print(f"Error: ❌ Failed to send inventory fetch trigger: {e}")
            raise
    
    async def trigger_immediate_sync(
        self,
        event_id: str,
        sync_active: bool,
        collection_time: Optional[str] = None
    ) -> dict:
        """
        Trigger two sync messages: one immediate and one delayed by 5 minutes.
        Used when users manually change sync_active status.
        
        Args:
            event_id: The outbox_event_id to sync
            sync_active: Whether sync is being activated or deactivated
            collection_time: Optional collection_time to use (defaults to latest in DB)
        
        Returns:
            dict with message IDs for both immediate and delayed messages
        """
        # Message payload
        message_body = {
            "action": "init_sync_event",
            "event_id": event_id,
            "collection_time": collection_time,  # Can be null - handler will use latest from DB
            "trigger_type": "user_action",
            "sync_active": sync_active,
        }
        
        try:
            # Send immediate message
            immediate_response = self.sqs.send_message(
                QueueUrl=self.marketplace_sync_queue_url,
                MessageBody=json.dumps(message_body),
            )
            
            # Send delayed message (5 minutes = 300 seconds)
            delayed_response = self.sqs.send_message(
                QueueUrl=self.marketplace_sync_queue_url,
                MessageBody=json.dumps(message_body),
                DelaySeconds=300,  # 5 minutes
            )
            
            print(f"✅ Triggered sync for event {event_id} (sync_active: {sync_active})")
            print(f"   - Immediate message ID: {immediate_response['MessageId']}")
            print(f"   - Delayed message ID: {delayed_response['MessageId']} (5min delay)")
            
            return {
                "immediate_message_id": immediate_response["MessageId"],
                "delayed_message_id": delayed_response["MessageId"],
                "status": "success",
            }
        
        except Exception as e:
            print(f"Error: ❌ Failed to send sync trigger messages: {e}")
            raise

