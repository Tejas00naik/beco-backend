"""Check and report on environment variable status."""

import os
import sys
from dotenv import load_dotenv

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Load environment variables
load_dotenv(override=True)

# Check for critical environment variables
print("Environment Variable Check:")
print("--------------------------")

def check_var(var_name, sensitive=False):
    """Check if an environment variable is set and print status."""
    value = os.environ.get(var_name)
    if value:
        if sensitive:
            # Show only first and last few characters
            prefix = value[:4] if len(value) > 4 else value
            suffix = value[-4:] if len(value) > 8 else ""
            print(f"✅ {var_name}: {prefix}...{suffix}")
        else:
            print(f"✅ {var_name}: {value}")
    else:
        print(f"❌ {var_name}: Not set")
        
# Check OpenAI API key
check_var('OPENAI_API_KEY', sensitive=True)

# Check Firestore settings
check_var('FIRESTORE_PROJECT_ID')
check_var('FIRESTORE_DATABASE_ID')

# Check Gmail API settings
check_var('GMAIL_TOKEN_FILE', sensitive=False)

# Check other environment variables
check_var('LLM_PROVIDER')
check_var('OPENAI_MODEL')

print("--------------------------")
