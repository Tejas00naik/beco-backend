import asyncio
import sys
import os

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from models.firestore_dao import FirestoreDAO
from dotenv import load_dotenv

load_dotenv()

async def clear_collections():
    dao = FirestoreDAO(collection_prefix='')
    
    print('Deleting all invoice records...')
    invoices = await dao.query_documents('invoice', [])
    for inv in invoices:
        await dao.delete_document('invoice', inv['invoice_uuid'])
    print(f'Deleted {len(invoices)} invoices')
    
    print('Deleting all other_doc records...')
    other_docs = await dao.query_documents('other_doc', [])
    for doc in other_docs:
        await dao.delete_document('other_doc', doc['other_doc_uuid'])
    print(f'Deleted {len(other_docs)} other_docs')
    
    print('Done!')

# Run the async function
asyncio.run(clear_collections())
