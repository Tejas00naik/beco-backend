#!/bin/bash
# Cloud Function deployment script for Gmail PubSub processor

# Configuration
PROJECT_ID="vaulted-channel-462118-a5"
REGION="asia-south1"
FUNCTION_NAME="process_email"
RUNTIME="python310"
ENTRY_POINT="process_pubsub_message"
MEMORY="512MB"
TIMEOUT="540s" # 9 minutes, maximum is 540s
SERVICE_ACCOUNT="beco-cloud-function@${PROJECT_ID}.iam.gserviceaccount.com"

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${YELLOW}Preparing for deployment...${NC}"

# Check if src directory exists in cloud_function
if [ ! -d "./src" ]; then
    echo -e "${YELLOW}Creating symlink to src directory...${NC}"
    ln -s ../src ./src || { echo -e "${RED}Failed to create src symlink${NC}"; exit 1; }
fi

# Deploy the Cloud Function
echo -e "${YELLOW}Deploying Cloud Function '${FUNCTION_NAME}'...${NC}"
gcloud functions deploy "${FUNCTION_NAME}" \
    --project="${PROJECT_ID}" \
    --region="${REGION}" \
    --runtime="${RUNTIME}" \
    --entry-point="${ENTRY_POINT}" \
    --trigger-topic="gmail-notifications" \
    --memory="${MEMORY}" \
    --timeout="${TIMEOUT}" \
    --service-account="${SERVICE_ACCOUNT}" \
    --set-env-vars="GOOGLE_CLOUD_PROJECT=${PROJECT_ID}" \
    --retry

if [ $? -eq 0 ]; then
    echo -e "${GREEN}Cloud Function deployed successfully!${NC}"
    echo -e "${YELLOW}Next steps:${NC}"
    echo "1. Make sure the service account has the 'Secret Manager Secret Accessor' role"
    echo "2. Ensure Gmail API credentials and token are uploaded to Secret Manager"
    echo "3. Verify the Pub/Sub subscription is properly configured to trigger the function"
else
    echo -e "${RED}Deployment failed.${NC}"
    exit 1
fi
