#!/bin/bash

add_service_principal_to_bundle() {
    # Create a modified databricks.yml with service principal permissions for redeployment
    
    if [ -z "$APP_SP_ID" ] || [ "$APP_SP_ID" = "null" ]; then
        echo "No service principal ID available, skipping SP permissions"
        return
    fi
    
    echo "Adding service principal $APP_SP_ID to bundle permissions..."
    
    # Backup original
    cp databricks.yml databricks.yml.backup
    
    # Add service principal to dev section's permissions after deploying_user
    # This adds it after the first occurrence of "level: CAN_MANAGE" in the dev section
    awk '
        /^  dev:/ { in_dev=1 }
        /^  prod:/ { in_dev=0 }
        {
            print
            if (in_dev && /level: CAN_MANAGE/ && !added) {
                print "      - service_principal_name: ${var.app_service_principal_application_id}"
                print "        level: CAN_MANAGE"
                added=1
            }
        }
    ' databricks.yml.backup > databricks.yml
    
    rm -f databricks.yml.tmp
    echo "Service principal permissions added to databricks.yml"
}

restore_original_bundle() {
    # Restore the original databricks.yml after deployment
    if [ -f databricks.yml.backup ]; then
        echo "Restoring original databricks.yml..."
        mv databricks.yml.backup databricks.yml
        rm -f databricks.yml.tmp
    fi
}

check_for_deployed_app() {
    # Check Terraform state file to get the correct app's service principal
    # This is the source of truth for what was actually deployed
    echo "============================================"
    echo "DEBUG: Checking for deployed app via Terraform state"
    echo "Profile: $PROFILE"
    echo "Target: $TARGET"
    echo "============================================"
    
    # Terraform state file is the most reliable source
    TF_STATE_FILE=".databricks/bundle/$TARGET/terraform/terraform.tfstate"
    
    if [ -f "$TF_STATE_FILE" ]; then
        echo "Found Terraform state file: $TF_STATE_FILE"
        
        # Extract service_principal_client_id from the databricks_app resource
        APP_SP_ID=$(jq -r '.resources[] | select(.type == "databricks_app" and .name == "dbxmetagen_app") | .instances[0].attributes.service_principal_client_id // empty' "$TF_STATE_FILE" 2>/dev/null)
        
        if [ -n "$APP_SP_ID" ] && [ "$APP_SP_ID" != "null" ] && [ "$APP_SP_ID" != "" ]; then
            echo "✓ Found deployed app with Service Principal Client ID: $APP_SP_ID"
            
            # Also show the app ID for verification
            APP_ID=$(jq -r '.resources[] | select(.type == "databricks_app" and .name == "dbxmetagen_app") | .instances[0].attributes.id // empty' "$TF_STATE_FILE" 2>/dev/null)
            echo "  App ID: $APP_ID"
            
            export APP_SP_ID="$APP_SP_ID"
            echo "Will add service principal permissions to bundle before deployment"
        else
            echo "State file exists but no service principal ID found yet"
            echo "This is likely the first deployment - SP will be created during deployment"
            export APP_SP_ID=""
        fi
    else
        echo "No Terraform state file found at $TF_STATE_FILE"
        echo "This is the first deployment - app will be created"
        export APP_SP_ID=""
    fi
    echo "============================================"
}

validate_bundle() {
    echo "Validating bundle..."
    if ! databricks bundle validate -t "$TARGET" --profile "$PROFILE"; then
        echo "Error: Bundle validation failed"
        exit 1
    fi
}

deploy_bundle() {

    TARGET="${TARGET:-dev}"
    echo "Deploying to $TARGET..."
    
    # If app exists, add service principal permissions to bundle
    if [ -n "$APP_SP_ID" ] && [ "$APP_SP_ID" != "null" ]; then
        add_service_principal_to_bundle
    fi
    
    local DEPLOY_VARS=()
    if [ "$DEBUG_MODE" = true ]; then
        DEPLOY_VARS+=("--var" "debug_mode=true")
    fi
    if [ "$CREATE_TEST_DATA" = true ]; then
        DEPLOY_VARS+=("--var" "create_test_data=true")
    fi
    
    # Add service principal ID and deploying user for variable substitution
    if [ -n "$APP_SP_ID" ] && [ "$APP_SP_ID" != "null" ]; then
        DEPLOY_VARS+=("--var" "app_service_principal_application_id=$APP_SP_ID")
    fi
    DEPLOY_VARS+=("--var" "deploying_user=$CURRENT_USER")
    
    if ! databricks bundle deploy --target "$TARGET" --profile "$PROFILE" "${DEPLOY_VARS[@]}"; then
        echo "Error: Bundle deployment failed with target $TARGET"
        restore_original_bundle  # Restore even on failure
        exit 1
    fi
    
    # Restore original databricks.yml after successful deployment
    restore_original_bundle
    
    export DEPLOY_TARGET="$TARGET"
    echo "Bundle deployed successfully"
}

# May need to update this
run_permissions_setup() {
    echo "Setting up permissions..."
    
    local catalog_name="${catalog_name:-dbxmetagen}"
    if [ -f "variables.yml" ]; then
        catalog_name=$(awk '/catalog_name:/{flag=1; next} flag && /default:/{print $2; exit}' variables.yml | xargs)
        catalog_name=${catalog_name:-dbxmetagen}
    fi
    
    local job_id
    job_id=$(databricks jobs list --profile "$PROFILE" --output json | grep -B5 -A5 "dbxmetagen_permissions_setup" | grep '"job_id"' | head -1 | sed 's/.*"job_id": *\([0-9]*\).*/\1/' || echo "")
    
    if [ -n "$job_id" ]; then
        echo "Running permissions setup job..."
        databricks jobs run-now --profile "$PROFILE" --json "{\"job_id\": $job_id, \"job_parameters\": {\"catalog_name\": \"$catalog_name\"}}"
        echo "Permissions job started (ID: $job_id)"
    else
        echo "Warning: Could not find permissions setup job"
    fi
}

create_deploying_user_yml() {
    echo "Creating deploying_user.yml with current user..."
    
    cat > app/deploying_user.yml << EOF
# Auto-generated during deployment - contains the user who deployed this app
# This file is created by deploy.sh and should not be committed to version control
# However, it cannot be added to gitignore because asset bundles obeys gitignore.
deploying_user: "$CURRENT_USER"
EOF
    
    echo "deploying_user.yml created with user: $CURRENT_USER"
}

create_app_env_yml() {
    echo "Creating app_env.yml with target..."
    cat > app/app_env.yml << EOF
# Auto-generated during deployment - contains the user who deployed this app
# This file is created by deploy.sh and should not be committed to version control
app_env: "$APP_ENV"
EOF
}

create_env_overrides_yml() {
    echo "Creating env_overrides.yml with environment-specific values..."
    cat > app/env_overrides.yml << EOF
# Auto-generated during deployment from dev.env
# This file is created by deploy.sh and should not be committed to version control
EOF
    
    # Add workspace_host if set
    if [ -n "$DATABRICKS_HOST" ]; then
        echo "workspace_host: \"$DATABRICKS_HOST\"" >> app/env_overrides.yml
    fi
    
    # Add permission_groups if set
    if [ -n "$permission_groups" ]; then
        echo "permission_groups: \"$permission_groups\"" >> app/env_overrides.yml
    fi
    
    # Add permission_users if set
    if [ -n "$permission_users" ]; then
        echo "permission_users: \"$permission_users\"" >> app/env_overrides.yml
    fi
    
    echo "env_overrides.yml created"
}

cleanup_temp_yml_files() {
    if [ -f app/deploying_user.yml ]; then
        echo "Cleaning up deploying_user.yml..."
        rm app/deploying_user.yml
    fi
    if [ -f app/app_env.yml ]; then
        echo "Cleaning up app_env.yml..."
        rm app/app_env.yml
    fi
    if [ -f app/env_overrides.yml ]; then
        echo "Cleaning up env_overrides.yml..."
        rm app/env_overrides.yml
    fi
    if [ -f databricks_final.yml ]; then
        echo "Cleaning up databricks_final.yml..."
        rm databricks_final.yml
    fi
    if [ -f databricks_final.yml.tmp ]; then
        echo "Cleaning up databricks_final.yml.tmp..."
        rm databricks_final.yml.tmp
    fi
    if [ -f databricks.yml.backup ]; then
        echo "Cleaning up databricks.yml.backup..."
        rm databricks.yml.backup
    fi
    if [ -f databricks.yml.tmp ]; then
        echo "Cleaning up databricks.yml.tmp..."
        rm databricks.yml.tmp
    fi
    if [ -f app/domain_config.yml ]; then
        echo "Cleaning up domain_config.yml..."
        rm app/domain_config.yml
    fi
    if [ -f variables.bkp ]; then
        echo "Cleaning up variables.bkp..."
        mv variables.bkp variables.yml
    fi
}

update_variables_yml() {
    echo "Creating variables override file from dev.env..."

    cp variables.yml variables.bkp
    
    if [ ! -f "dev.env" ]; then
        echo "No dev.env found."
        return
    fi
    
    if [ -n "$DATABRICKS_HOST" ]; then
        cat >> variables_override.yml << EOF
  
  workspace_host:
    default: "$DATABRICKS_HOST"
EOF
        echo "Setting workspace_host from DATABRICKS_HOST"
    fi
    
    if [ -n "$permission_groups" ]; then
        cat >> variables_override.yml << EOF
  permission_groups:
    default: "$permission_groups"
EOF
        echo "Setting permission_groups: $permission_groups"
    fi
    
    if [ -n "$permission_users" ]; then
        cat >> variables_override.yml << EOF
  permission_users:
    default: "$permission_users"
EOF
        echo "Setting permission_users: $permission_users"
    fi
    
    echo "Updated variables.yml"
}


start_app() {
    echo "App ID: $APP_ID"
    echo "Service Principal ID: $APP_SP_ID"

    # Create deploying_user.yml before deployment

    # Deploy and run the app
    databricks bundle run -t "$TARGET" --profile "$PROFILE" \
        --var "deploying_user=$CURRENT_USER" \
        --var "app_service_principal_application_id=$APP_SP_ID" \
        dbxmetagen_app
}

# Parse arguments
RUN_PERMISSIONS=false
DEBUG_MODE=false
CREATE_TEST_DATA=false
TARGET="dev"
PROFILE="DEFAULT"
ENV="dev"

while [[ $# -gt 0 ]]; do
    case $1 in
        --env)
            ENV="$2"
            shift 2
            ;;
        --host)
            HOST_URL="$2"
            shift 2
            ;;
        --profile)
            PROFILE="$2"
            shift 2
            ;;
        --permissions)
            RUN_PERMISSIONS=true
            shift
            ;;
        --debug)
            DEBUG_MODE=true
            shift
            ;;
        --create-test-data)
            CREATE_TEST_DATA=true
            shift
            ;;
        --target)
            TARGET="$2"
            shift 2
            ;;
        --help|-h)
            echo "Usage: $0 [OPTIONS]"
            echo "Options:"
            echo "  --profile PROFILE    Databricks CLI profile to use (default: DEFAULT)"
            echo "  --target TARGET      Deploy to specific target (dev/prod, default: dev)"
            echo "  --permissions        Run permissions setup job"
            echo "  --debug              Enable debug mode"
            echo "  --create-test-data   Generate test data"
            echo "  --help               Show this help"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done


set -e

if ! command -v databricks &> /dev/null; then
    echo "Error: Databricks CLI not found. Please install it first."
    exit 1
fi

if [ ! -f "databricks.yml" ]; then
    echo "Error: databricks.yml not found. Run from the dbxmetagen directory."
    exit 1
fi

echo "Using Databricks profile: $PROFILE"
if ! databricks current-user me --profile "$PROFILE" &> /dev/null; then
    echo "Error: Not authenticated with Databricks using profile '$PROFILE'."
    echo "Run: databricks configure --profile $PROFILE"
    exit 1
fi

CURRENT_USER=$(databricks current-user me --profile "$PROFILE" --output json | jq -r '.userName')

cat > app/deploying_user.yml << EOF
# Auto-generated during deployment - contains the user who deployed this app
# This file is created by deploy.sh and should not be committed to version control
deploying_user: "$CURRENT_USER"
EOF


APP_ENV=${TARGET}

cat > app/app_env.yml << EOF
# Auto-generated during deployment - contains the user who deployed this app
# This file is created by deploy.sh and should not be committed to version control
app_env: "$APP_ENV"
EOF

if [ -f "${APP_ENV}.env" ]; then
    echo "Loading environment variables from dev.env..."
    set -a  # automatically export all variables
    source ${APP_ENV}.env
    set +a  # turn off automatic export
fi

HOST_URL=$DATABRICKS_HOST
TARGET=$TARGET

if [ -f "variables.bkp" ]; then
    echo "Restoring variables.yml from backup..."
    mv variables.bkp variables.yml
fi

if [ -f "variables_override.yml" ]; then
    echo "Cleaning up variables_override.yml..."
    rm variables_override.yml
fi


cat app/deploying_user.yml
cat app/app_env.yml

echo "Current user: $CURRENT_USER"

echo ""
echo "DBX MetaGen Deployment"
echo "Target: $TARGET"

update_variables_yml
cp configurations/domain_config.yaml app/domain_config.yaml
create_deploying_user_yml
create_app_env_yml
create_env_overrides_yml
check_for_deployed_app
cat variables_override.yml >> variables.yml

echo "=== Deploying bundle ==="
validate_bundle

# Store the SP ID before deployment (might be empty on first deploy)
OLD_APP_SP_ID="$APP_SP_ID"

deploy_bundle

# After deployment, check if we now have a service principal ID
echo "=== Checking for service principal after deployment ==="
check_for_deployed_app

# If we got a new SP ID after deployment, redeploy with permissions
if [ -z "$OLD_APP_SP_ID" ] && [ -n "$APP_SP_ID" ] && [ "$APP_SP_ID" != "null" ]; then
    echo "============================================"
    echo "New service principal detected: $APP_SP_ID"
    echo "Redeploying bundle with service principal permissions..."
    echo "============================================"
    deploy_bundle
fi

echo "=== Starting app ==="
start_app
#Run permissions if requested
if [ "$RUN_PERMISSIONS" = true ]; then
       run_permissions_setup
fi

cleanup_temp_yml_files

echo "Deployment complete!"
echo "Access your app in Databricks workspace > Apps > dbxmetagen-app"