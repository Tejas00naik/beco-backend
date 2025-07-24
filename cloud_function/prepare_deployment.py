#!/usr/bin/env python3
"""
Prepare Cloud Function deployment files with credentials included.
This script creates a deployment-ready directory with all necessary files.
"""

import os
import sys
import shutil
import argparse
import json
from datetime import datetime

def create_deployment_package(token_path, credentials_path, output_dir):
    """
    Create a complete Cloud Function deployment package with credentials included.
    
    Args:
        token_path: Path to the OAuth token.json file
        credentials_path: Path to the OAuth client secrets file
        output_dir: Output directory for deployment package
    """
    print(f"Preparing deployment package in {output_dir}")
    
    # Create output directory if it doesn't exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # Create directories
    os.makedirs(os.path.join(output_dir, 'secrets'), exist_ok=True)
    
    # Copy main.py and requirements.txt
    shutil.copy('main.py', output_dir)
    shutil.copy('requirements.txt', output_dir)
    
    # Copy the entire src directory
    src_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'src')
    dst_dir = os.path.join(output_dir, 'src')
    
    print(f"Copying src directory from {src_dir} to {dst_dir}...")
    
    # Remove existing src directory or symlink
    if os.path.exists(dst_dir) or os.path.islink(dst_dir):
        if os.path.islink(dst_dir):
            os.unlink(dst_dir)  # Remove symlink
            print("✓ Removed existing src symlink")
        else:
            shutil.rmtree(dst_dir)  # Remove regular directory
            print("✓ Removed existing src directory")
    
    # Copy src directory contents
    shutil.copytree(src_dir, dst_dir)
    print("✅ Copied src directory successfully")
    
    # Copy token.json and all credential files from secrets directory
    try:
        # Copy token.json
        shutil.copy(token_path, os.path.join(output_dir, 'secrets', 'token.json'))
        print("✅ Copied token.json")
        
        # Copy all files from secrets directory
        secrets_dir = os.path.dirname(credentials_path)
        for filename in os.listdir(secrets_dir):
            if filename.endswith('.json'):
                source = os.path.join(secrets_dir, filename)
                destination = os.path.join(output_dir, 'secrets', filename)
                shutil.copy(source, destination)
                print(f"✅ Copied {filename}")
        
        print("✅ All credential files copied successfully")
    except Exception as e:
        print(f"❌ Error copying credentials: {str(e)}")
        sys.exit(1)
    
    # Create deployment script
    deploy_script = os.path.join(output_dir, 'deploy.sh')
    with open(deploy_script, 'w') as f:
        f.write('''#!/bin/bash
# Cloud Function deployment script with credentials included

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
GREEN='\\033[0;32m'
YELLOW='\\033[1;33m'
RED='\\033[0;31m'
NC='\\033[0m' # No Color

echo -e "${YELLOW}Preparing for deployment...${NC}"

# Deploy the Cloud Function
echo -e "${YELLOW}Deploying Cloud Function '${FUNCTION_NAME}'...${NC}"
gcloud functions deploy "${FUNCTION_NAME}" \\
    --project="${PROJECT_ID}" \\
    --region="${REGION}" \\
    --runtime="${RUNTIME}" \\
    --entry-point="${ENTRY_POINT}" \\
    --trigger-topic="gmail-notifications" \\
    --memory="${MEMORY}" \\
    --timeout="${TIMEOUT}" \\
    --service-account="${SERVICE_ACCOUNT}" \\
    --no-gen2 \\
    --retry

if [ $? -eq 0 ]; then
    echo -e "${GREEN}Cloud Function deployed successfully!${NC}"
else
    echo -e "${RED}Deployment failed.${NC}"
    exit 1
fi
''')
    
    # Make the deployment script executable
    os.chmod(deploy_script, 0o755)
    
    # Modify main.py to use local files instead of Secret Manager
    modify_main_for_local_files(os.path.join(output_dir, 'main.py'))
    
    print("\n✅ Deployment package created successfully!")
    print(f"To deploy the Cloud Function, run: cd {output_dir} && ./deploy.sh")
    print("\nNOTE: This package includes OAuth credentials. Keep it secure!")

def modify_main_for_local_files(main_path):
    """
    Modify the main.py file to use local files instead of Secret Manager.
    """
    with open(main_path, 'r') as f:
        content = f.read()
    
    # Replace Secret Manager code with local file access
    new_content = content.replace(
        """            # Get credentials and token from Secret Manager
            project_id = os.environ.get('GOOGLE_CLOUD_PROJECT', 'vaulted-channel-462118-a5')
            try:
                credentials_json = access_secret(project_id, 'gmail_credentials')
                token_json = access_secret(project_id, 'gmail_token')
                
                # Write credentials and token to temporary files
                credentials_path = '/tmp/client_secret.json'
                token_path = '/tmp/token.json'
                
                with open(credentials_path, 'w') as f:
                    f.write(credentials_json)
                    
                with open(token_path, 'w') as f:
                    f.write(token_json)
                
                logger.info("Credentials and token retrieved from Secret Manager")
            except Exception as e:
                logger.error(f"Error accessing credentials: {str(e)}")
                return f"Error accessing credentials: {str(e)}", 500""",
        
        """            # Use local credential and token files
            try:
                # Paths to credential files included in deployment
                current_dir = os.path.dirname(os.path.abspath(__file__))
                credentials_path = os.path.join(current_dir, 'secrets', 'client_secret.json')
                token_path = os.path.join(current_dir, 'secrets', 'token.json')
                
                # Verify files exist
                if not os.path.exists(credentials_path) or not os.path.exists(token_path):
                    error_msg = f"Credential files not found: {credentials_path}, {token_path}"
                    logger.error(error_msg)
                    return error_msg, 500
                
                logger.info("Using local credential files")
            except Exception as e:
                logger.error(f"Error accessing credentials: {str(e)}")
                return f"Error accessing credentials: {str(e)}", 500"""
    )
    
    # Remove Secret Manager import and function
    lines = new_content.split("\n")
    filtered_lines = []
    skip_lines = False
    
    for line in lines:
        if "from google.cloud import secretmanager" in line:
            filtered_lines.append("# Local file approach - no Secret Manager needed")
            continue
            
        if "def access_secret(" in line:
            skip_lines = True
            continue
            
        if skip_lines and line.startswith("def process_pubsub_message"):
            skip_lines = False
            
        if not skip_lines:
            filtered_lines.append(line)
    
    # Write modified content back
    with open(main_path, 'w') as f:
        f.write("\n".join(filtered_lines))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Prepare Gmail Cloud Function deployment package")
    parser.add_argument("--token", default="../token.json", help="Path to token.json file")
    parser.add_argument("--credentials", default="../secrets/email-client-secret.json", 
                        help="Path to OAuth client secret file")
    parser.add_argument("--output", default="./deployment", help="Output directory for deployment package")
    
    args = parser.parse_args()
    
    create_deployment_package(args.token, args.credentials, args.output)
