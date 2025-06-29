#!/usr/bin/env python

import os
import asyncio
from models.firestore_dao import FirestoreDAO

async def main():
    # Set environment variables
    os.environ['FIRESTORE_PROJECT_ID'] = 'beco-technologies'
    
    # Initialize Firestore DAO
    dao = FirestoreDAO()
    
    # Get email log document
    email_uuid = '4218a48c-3e92-4d78-9cac-0f99388d2ad3'
    doc = await dao.get_document(email_uuid, 'email_log')
    
    if not doc:
        print(f"Email log {email_uuid} not found")
        return
        
    # Print group_uuids field
    print(f"Email Log UUID: {email_uuid}")
    print(f"Group UUIDs: {doc.get('group_uuids', [])}")
    print(f"Sender Mail: {doc.get('sender_mail')}")
    print(f"Original Sender Mail: {doc.get('original_sender_mail')}")
    
    # Check related payment advices
    payment_advices = await dao.query_documents('payment_advice', [('email_log_uuid', '==', email_uuid)])
    print(f"\nRelated Payment Advices: {len(payment_advices)}")
    
    for pa in payment_advices:
        print(f"\nPayment Advice UUID: {pa.get('payment_advice_uuid')}")
        print(f"Legal Entity UUID: {pa.get('legal_entity_uuid')}")
        print(f"Payer Name: {pa.get('payer_name')}")

if __name__ == "__main__":
    asyncio.run(main())
