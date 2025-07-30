#!/bin/bash
# Simplified Cloud Function deployment script that:
# 1. Creates a complete deployment package with all files
# 2. Deploys directly from that package

# Configuration
PROJECT_ID="vaulted-channel-462118-a5"
REGION="asia-south1"
FUNCTION_NAME="process_email"
RUNTIME="python310"
ENTRY_POINT="process_pubsub_message"
MEMORY="512MB"
TIMEOUT="540s" # 9 minutes, maximum is 540s
# Using default compute service account
SERVICE_ACCOUNT="${PROJECT_ID}@appspot.gserviceaccount.com"

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Deployment directory
DEPLOY_DIR="./deployment"

# Check if --delete flag is passed
DELETE_FIRST=false
for arg in "$@"
do
    if [ "$arg" == "--delete" ]; then
        DELETE_FIRST=true
        break
    fi
done

# Delete the function first if requested
if [ "$DELETE_FIRST" = true ]; then
    echo -e "${YELLOW}Deleting existing Cloud Function '${FUNCTION_NAME}'...${NC}"
    gcloud functions delete "${FUNCTION_NAME}" \
        --project="${PROJECT_ID}" \
        --region="${REGION}" \
        --quiet || { echo -e "${YELLOW}Function doesn't exist or delete failed. Continuing with deployment.${NC}"; }
    
    echo -e "${GREEN}Function deleted. Proceeding with deployment...${NC}"
    # Brief pause to ensure deletion is propagated
    sleep 5
fi

# Create deployment package
echo -e "${YELLOW}Creating deployment package...${NC}"

# Clean up previous deployment directory
if [ -d "$DEPLOY_DIR" ]; then
    echo -e "${YELLOW}Removing existing deployment directory...${NC}"
    rm -rf $DEPLOY_DIR
fi

# Create fresh deployment directory
mkdir -p $DEPLOY_DIR
mkdir -p "$DEPLOY_DIR/secrets"

# Copy main.py and requirements.txt
echo -e "${YELLOW}Copying main.py and requirements.txt...${NC}"
cp main.py $DEPLOY_DIR/
cp requirements.txt $DEPLOY_DIR/

# Copy all Python files from the parent src directory to our deployment directory
echo -e "${YELLOW}Copying Python files from src directory...${NC}"
find ../src -type f -name "*.py" | while read file; do
    # Get the relative path from the src directory
    rel_path=${file#../src/}
    # Create the directory structure if it doesn't exist
    mkdir -p "$DEPLOY_DIR/src/$(dirname $rel_path)"
    # Copy the file
    cp "$file" "$DEPLOY_DIR/src/$rel_path"
done

# Copy the secrets directory with credentials
echo -e "${YELLOW}Copying secrets directory...${NC}"

# First check if secrets is in the cloud_function directory
if [ -d "./secrets" ]; then
    cp -r ./secrets/* "$DEPLOY_DIR/secrets/"
    echo -e "${GREEN}✓ Secrets directory copied from cloud_function/secrets/${NC}"

# Then check if it's in the project root
elif [ -d "../secrets" ]; then
    cp -r ../secrets/* "$DEPLOY_DIR/secrets/"
    echo -e "${GREEN}✓ Secrets directory copied from project root${NC}"
else
    echo -e "${RED}❌ Secrets directory not found in cloud_function or project root!${NC}"
    exit 1
fi

# Copy .env file if it exists
env_file="../.env"
if [ -f "$env_file" ]; then
    cp "$env_file" "$DEPLOY_DIR/"
    echo -e "${GREEN}✓ .env file copied successfully${NC}"
else
    echo -e "${YELLOW}⚠️ .env file not found, creating minimal version${NC}"
    echo "FIRESTORE_PROJECT_ID=vaulted-channel-462118-a5" > "$DEPLOY_DIR/.env"
fi

echo -e "${GREEN}✓ Deployment package created successfully${NC}"

# Deploy the Cloud Function
echo -e "${YELLOW}Deploying Cloud Function '${FUNCTION_NAME}'...${NC}"
cd $DEPLOY_DIR && \
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
    --no-gen2 \
    --retry

if [ $? -eq 0 ]; then
    echo -e "${GREEN}Cloud Function deployed successfully!${NC}"
    
    # Print deployment timestamp and log info
    TIMESTAMP=$(date +"%Y-%m-%d %H:%M:%S")
    echo -e "${GREEN}Deployed at: ${TIMESTAMP}${NC}"
    echo -e "${YELLOW}To monitor logs for this deployment, use:${NC}"
    echo "cd /Users/macbookpro/RECOCENT/beco-backend && PYTHONPATH=/Users/macbookpro/RECOCENT/beco-backend python scripts/verify_email_pipeline.py --check-logs --timeout 180"
else
    echo -e "${RED}Deployment failed.${NC}"
    exit 1
fi
