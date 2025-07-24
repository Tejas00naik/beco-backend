#!/usr/bin/env python3
"""
Verify that the token.json file contains a refresh token
and test refreshing the access token.
"""

import os
import json
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import logging

# Gmail API scope that includes read/write access
# Only include scopes that are actually being granted
SCOPES = ['https://www.googleapis.com/auth/gmail.modify',
          'https://www.googleapis.com/auth/gmail.readonly']

# Set up logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def verify_and_refresh_token(token_path, credentials_path):
    """
    Verify if the token has a refresh token and test refreshing the access token.
    If necessary, it will trigger a new OAuth flow to get a refresh token.
    
    Args:
        token_path: Path to the token.json file
        credentials_path: Path to the OAuth client secrets file
    """
    creds = None
    
    # Check if token file exists and load it
    if os.path.exists(token_path):
        try:
            with open(token_path, 'r') as token:
                token_data = json.load(token)
                
                # Check if refresh token exists
                if 'refresh_token' in token_data:
                    logger.info("✅ Token contains a refresh token!")
                else:
                    logger.warning("❌ Token does NOT have a refresh token! Will need to regenerate.")
                    # We'll regenerate below, so set creds to None
                    creds = None
                    # Delete the token file to force regeneration
                    os.remove(token_path)
                    logger.info(f"Deleted existing token file {token_path} to force regeneration")
                    
                # If token has refresh_token, load the credentials
                if creds is None and 'refresh_token' in token_data:
                    creds = Credentials.from_authorized_user_info(token_data, SCOPES)
        except json.JSONDecodeError:
            logger.error(f"The token file {token_path} is not valid JSON. Will regenerate.")
            os.remove(token_path)
            creds = None
        except Exception as e:
            logger.error(f"Error loading token: {str(e)}. Will regenerate.")
            creds = None
    else:
        logger.warning(f"Token file {token_path} does not exist. Will generate a new one.")
    
    # If credentials don't exist or are invalid, run the OAuth flow
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            logger.info("Token expired. Attempting to refresh...")
            try:
                creds.refresh(Request())
                logger.info("✅ Successfully refreshed the access token!")
            except Exception as e:
                logger.error(f"Error refreshing token: {str(e)}. Will regenerate.")
                creds = None
        
        # If we still don't have valid credentials, run the OAuth flow
        if not creds:
            try:
                logger.info("Starting OAuth flow to generate new token with refresh capability...")
                flow = InstalledAppFlow.from_client_secrets_file(credentials_path, SCOPES)
                creds = flow.run_local_server(port=0)
                
                # Save the credentials for the next run
                with open(token_path, 'w') as token:
                    token.write(creds.to_json())
                    
                logger.info(f"✅ New token with refresh capability saved to {token_path}")
            except Exception as e:
                logger.error(f"Error during OAuth flow: {str(e)}")
                return False
    
    # Final verification
    with open(token_path, 'r') as token:
        token_data = json.load(token)
        if 'refresh_token' in token_data:
            logger.info("✅ Final verification: Token contains refresh capability")
            return True
        else:
            logger.error("❌ Final verification: Token still does NOT have refresh capability")
            return False

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Verify Gmail API token refresh capability")
    parser.add_argument("--token", default="../token.json", help="Path to token.json file")
    parser.add_argument("--credentials", default="../secrets/email-client-secret.json", 
                        help="Path to OAuth client secret file")
    
    args = parser.parse_args()
    
    success = verify_and_refresh_token(args.token, args.credentials)
    
    if success:
        print("\n✅ Your token is ready to be used in the Cloud Function!")
        print("Next step: Upload it to Secret Manager with:")
        print(f"python setup_secrets.py --credentials-path={args.credentials} --token-path={args.token}")
    else:
        print("\n❌ There was an issue with your token. Please check the logs and try again.")
