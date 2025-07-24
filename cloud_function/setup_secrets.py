#!/usr/bin/env python3
"""
Setup script for Google Secret Manager secrets for Gmail Cloud Function.

This script uploads the necessary Gmail API credentials and token to Google Secret Manager
for use by the Cloud Function.

Prerequisites:
- Google Cloud SDK installed and configured
- Appropriate permissions on the GCP project
- Gmail credentials and token files available locally
"""

import os
import argparse
import json
from google.cloud import secretmanager

def create_or_update_secret(client, project_id, secret_id, secret_data):
    """
    Create or update a secret in Google Secret Manager.
    
    Args:
        client: Secret Manager client
        project_id: Google Cloud project ID
        secret_id: ID for the secret
        secret_data: Secret content as string
    """
    # Create secret if it doesn't exist
    parent = f"projects/{project_id}"
    
    # Check if secret exists
    try:
        client.get_secret(request={"name": f"{parent}/secrets/{secret_id}"})
        print(f"Secret {secret_id} exists, adding new version.")
    except Exception:
        print(f"Creating new secret {secret_id}...")
        client.create_secret(
            request={
                "parent": parent,
                "secret_id": secret_id,
                "secret": {"replication": {"automatic": {}}},
            }
        )
    
    # Add a new version to the secret
    secret_path = client.secret_path(project_id, secret_id)
    response = client.add_secret_version(
        request={
            "parent": secret_path,
            "payload": {"data": secret_data.encode("UTF-8")},
        }
    )
    
    print(f"Added secret version: {response.name}")

def main():
    parser = argparse.ArgumentParser(description="Setup Google Secret Manager for Gmail Cloud Function")
    parser.add_argument("--project-id", default=os.environ.get("GOOGLE_CLOUD_PROJECT"), 
                        help="Google Cloud Project ID")
    parser.add_argument("--credentials-path", default="client_secret.json",
                        help="Path to Gmail API OAuth client secret JSON file")
    parser.add_argument("--token-path", default="token.json",
                        help="Path to Gmail API OAuth token JSON file")
    
    args = parser.parse_args()
    
    if not args.project_id:
        print("Error: No Google Cloud Project ID provided. Use --project-id or set GOOGLE_CLOUD_PROJECT environment variable.")
        return
    
    # Create Secret Manager client
    client = secretmanager.SecretManagerServiceClient()
    
    # Upload credentials
    try:
        with open(args.credentials_path, "r") as f:
            credentials_content = f.read()
        create_or_update_secret(client, args.project_id, "gmail_credentials", credentials_content)
    except FileNotFoundError:
        print(f"Error: Credentials file not found at {args.credentials_path}")
        return
    
    # Upload token
    try:
        with open(args.token_path, "r") as f:
            token_content = f.read()
        create_or_update_secret(client, args.project_id, "gmail_token", token_content)
    except FileNotFoundError:
        print(f"Error: Token file not found at {args.token_path}")
        return
    
    print("\nSecrets setup complete! The Cloud Function can now access these secrets.")
    print("\nNext steps:")
    print("1. Make sure the Cloud Function has the Secret Manager Secret Accessor role (roles/secretmanager.secretAccessor)")
    print("2. Deploy the Cloud Function with the appropriate service account")

if __name__ == "__main__":
    main()
