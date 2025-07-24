# Gmail PubSub Cloud Function Integration

This document explains how to set up and deploy the Cloud Function that processes Gmail notifications via Google Pub/Sub.

## Overview

The system works as follows:

1. Gmail API's push notifications send messages to a Pub/Sub topic when new emails arrive
2. A Pub/Sub subscription triggers our Cloud Function with the notification data
3. The Cloud Function authenticates with Gmail API using OAuth credentials stored in Secret Manager
4. The function processes the email and refreshes the Gmail watch as needed
5. Results are stored in Firestore

## Prerequisites

- Google Cloud project with Gmail API, Pub/Sub, Cloud Functions, and Secret Manager enabled
- OAuth credentials configured for Gmail API (with appropriate scopes)
- Initial OAuth token generated via browser-based consent flow
- Service account for Cloud Function with appropriate permissions

## Setup Steps

### 1. Gmail API OAuth Setup

Gmail API requires OAuth authentication with user consent. This means:

- The initial token must be generated through a browser-based OAuth flow
- The token needs to be securely stored and refreshed automatically
- Cloud Functions cannot trigger the browser-based consent flow

Follow these steps:

1. If you haven't already, create OAuth credentials in Google Cloud Console:
   - Go to APIs & Services > Credentials
   - Create an OAuth Client ID (Web application type)
   - Add appropriate redirect URIs (e.g., http://localhost:8080)
   - Download the JSON file (client_secret.json)

2. Generate the initial token locally:
   - Use our existing token generation flow in `gmail_reader.py`
   - This will open a browser window for consent
   - The resulting token will be saved as `token.json`

3. Upload both credentials and token to Secret Manager using the provided script:

```bash
# Make the script executable
chmod +x setup_secrets.py

# Run the script
python setup_secrets.py --project-id=vaulted-channel-462118-a5 \
                        --credentials-path=/path/to/client_secret.json \
                        --token-path=/path/to/token.json
```

### 2. Service Account Permissions

The Cloud Function needs permissions to:

1. Access Secret Manager to get credentials and token
2. Call Gmail API using the OAuth token
3. Update Secret Manager when the token is refreshed
4. Access Firestore to store processing results

Configure your service account:

```bash
# Create service account (if it doesn't exist)
gcloud iam service-accounts create beco-cloud-function \
  --display-name="Beco Cloud Function Service Account" \
  --project=vaulted-channel-462118-a5

# Grant Secret Manager access
gcloud projects add-iam-policy-binding vaulted-channel-462118-a5 \
  --member="serviceAccount:beco-cloud-function@vaulted-channel-462118-a5.iam.gserviceaccount.com" \
  --role="roles/secretmanager.secretAccessor"

# Grant Pub/Sub subscriber role
gcloud projects add-iam-policy-binding vaulted-channel-462118-a5 \
  --member="serviceAccount:beco-cloud-function@vaulted-channel-462118-a5.iam.gserviceaccount.com" \
  --role="roles/pubsub.subscriber"

# Grant Firestore access
gcloud projects add-iam-policy-binding vaulted-channel-462118-a5 \
  --member="serviceAccount:beco-cloud-function@vaulted-channel-462118-a5.iam.gserviceaccount.com" \
  --role="roles/datastore.user"
```

### 3. Deploy the Cloud Function

To deploy the function:

```bash
# Make the deploy script executable
chmod +x deploy.sh

# Run the deployment script
./deploy.sh
```

This will:
1. Create a symlink to the `src` directory
2. Deploy the Cloud Function with appropriate settings
3. Connect it to the Gmail notifications Pub/Sub topic

### 4. Test the End-to-End Flow

Once deployed:

1. Verify the Cloud Function deployment in GCP Console
2. Check that the Pub/Sub subscription is correctly configured
3. Test by sending an email to the monitored address
4. Check Cloud Function logs to confirm processing
5. Verify data in Firestore

## Maintaining OAuth Access

The OAuth access token expires periodically and must be refreshed. Our implementation:

1. Uses the refresh token to get a new access token when needed
2. Updates the token in Secret Manager with the refreshed values
3. Handles token refresh failures by logging clear error messages

To manually refresh the token if needed, use the OAuth flow locally again and re-upload to Secret Manager.

## Troubleshooting

Common issues and solutions:

1. **Cloud Function failing to access Secret Manager:**
   - Verify service account permissions
   - Check Secret Manager resource names

2. **Gmail API authentication failures:**
   - Token may have expired without refresh capability
   - Consent may be needed for new scopes
   - Re-run OAuth flow locally and update the token in Secret Manager

3. **Pub/Sub message format issues:**
   - Check Cloud Function logs for the exact message format
   - Verify the message parsing code matches Gmail notification format

4. **Missing Gmail notifications:**
   - Check if Gmail watch needs renewal
   - Verify that the Pub/Sub topic and subscription are correctly configured
   - Check if Gmail watch history ID is being tracked properly

For other issues, check the Cloud Function logs for detailed error messages.
