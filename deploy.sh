#!/bin/bash

set -e

if ! command -v databricks &> /dev/null; then
    echo "Error: Databricks CLI not found. Please install it first."
    exit 1
fi

if [ ! -f "databricks.yml" ]; then
    echo "Error: databricks.yml not found. Run from the dbxmetagen directory."
    exit 1
fi

if ! databricks current-user me --profile DEFAULT &> /dev/null; then
    echo "Error: Not authenticated with Databricks. Run: databricks configure"
    exit 1
fi
 

add_service_principal_simple() {
    # Copy databricks.yml and add service principal permissions to the dev section

    SOURCE_FILE="databricks.yml"
    TARGET_FILE="databricks_final.yml"

    # Copy the original file
    cp "$SOURCE_FILE" "$TARGET_FILE"

    # Add service principal lines after the existing user permissions in dev section
    sed -i.tmp '/^  dev:/,/^  [a-z]/{
        /^      level: CAN_MANAGE/{
            a\
        - service_principal_name: ${var.app_service_principal_application_id}\
            level: CAN_MANAGE
        }
    }' "$TARGET_FILE"

    # Clean up sed backup file
    #rm "${TARGET_FILE}.tmp" 2>/dev/null

echo "Created $TARGET_FILE with service principal permissions added to dev section"
}

check_for_deployed_app() {
    SP_ID=$(databricks apps get "dbxmetagen-app" --output json | jq -r '.id')
    echo "SP_ID: $SP_ID"
    export APP_SP_ID="$SP_ID"
    if [ ! -n "$SP_ID" ]; then
        echo "App does not exist. Running initial deployment to get app SP ID..."
        validate_bundle -t ${TARGET}_spn --var "app_service_principal_application_id=None"
        deploy_bundle -t ${TARGET}_spn --var "app_service_principal_application_id=None"
    else        
        echo "App already exists. Using existing SP ID: $APP_SP_ID"
        add_service_principal_simple
        validate_bundle -t ${TARGET} -bundle_file "databricks_final.yml.tmp" --var "app_service_principal_application_id=$APP_SP_ID"
        deploy_bundle -t ${TARGET} -bundle_file "databricks_final.yml.tmp" --var "app_service_principal_application_id=$APP_SP_ID"
        rm databricks_final.yml.tmp
    fi
}

validate_bundle() {
    echo "Validating bundle..."
    if ! databricks bundle validate; then
        echo "Error: Bundle validation failed"
        exit 1
    fi
}

deploy_bundle() {
    TARGET="${TARGET:-dev}" # This is hardcoded for now, but should be passed in from env
    echo "Deploying to $TARGET..."
    
    DEPLOY_VARS=""
    if [ "$DEBUG_MODE" = true ]; then
        DEPLOY_VARS="$DEPLOY_VARS --var=debug_mode=true"
    fi
    if [ "$CREATE_TEST_DATA" = true ]; then
        DEPLOY_VARS="$DEPLOY_VARS --var=create_test_data=true"
    fi
    
    if ! databricks bundle deploy --target "$TARGET" $DEPLOY_VARS; then
        echo "Error: Bundle deployment failed"
            exit 1
        fi
            
            export DEPLOY_TARGET="$TARGET"
    echo "Bundle deployed successfully"
}

# May need to update this
run_permissions_setup() {
    echo "Setting up permissions..."
    
    local catalog_name="dbxmetagen"
    if [ -f "variables.yml" ]; then
        catalog_name=$(awk '/catalog_name:/{flag=1; next} flag && /default:/{print $2; exit}' variables.yml | xargs)
        catalog_name=${catalog_name:-dbxmetagen}
    fi
    
    local job_id
    job_id=$(databricks jobs list --output json | grep -B5 -A5 "dbxmetagen_permissions_setup" | grep '"job_id"' | head -1 | sed 's/.*"job_id": *\([0-9]*\).*/\1/' || echo "")
    
    if [ -n "$job_id" ]; then
        echo "Running permissions setup job..."
        databricks jobs run-now --json "{\"job_id\": $job_id, \"job_parameters\": {\"catalog_name\": \"$catalog_name\"}}"
        echo "Permissions job started (ID: $job_id)"
    else
        echo "Warning: Could not find permissions setup job"
    fi
}

create_deploying_user_yml() {
    echo "Creating deploying_user.yml with current user..."
    
    # Create the deploying_user.yml file in the app directory
    cat > app/deploying_user.yml << EOF
# Auto-generated during deployment - contains the user who deployed this app
# This file is created by deploy.sh and should not be committed to version control
# However, it cannot be added to gitignore because asset bundles obeys gitignore.
deploying_user: "$CURRENT_USER"
EOF

create_app_env_yml() {
    echo "Creating app_env.yml with target..."
    cat > app/app_env.yml << EOF
# Auto-generated during deployment - contains the user who deployed this app
# This file is created by deploy.sh and should not be committed to version control
app_env: "$APP_ENV"
EOF
}

    
    echo "✅ deploying_user.yml created with user: $CURRENT_USER"
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
    if [ -f app/variables.yml ]; then
        echo "Cleaning up variables.yml..."
        rm app/variables.yml
    fi
    if [ -f app/app_variables.yml ]; then
        echo "Cleaning up app_variables.yml..."
        rm app/app_variables.yml
    fi
    if [ -f databricks_final.yml ]; then
        echo "Cleaning up databricks_final.yml..."
        rm databricks_final.yml
    fi
}

start_app() {
    echo "App ID: $APP_ID"
    echo "Service Principal ID: $APP_SP_ID"

    # Create deploying_user.yml before deployment

    # Deploy and run the app - the app will read deploying_user.yml directly
    databricks bundle run -t $TARGET --var="deploying_user=$CURRENT_USER" --var="app_service_principal_application_id=$APP_SP_ID" dbxmetagen_app
}

# Parse arguments
RUN_PERMISSIONS=false
DEBUG_MODE=false
CREATE_TEST_DATA=false
TARGET="dev"
PROFILE="DEFAULT"
CURRENT_USER=$(databricks current-user me --profile DEFAULT --output json | jq -r '.userName')
HOST_URL=$(databricks current-user me --profile DEFAULT --output json | jq -r '.workspaceUrl')


while [[ $# -gt 0 ]]; do
    case $1 in
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
            echo "  --permissions     Run permissions setup job"
            echo "  --debug          Enable debug mode"
            echo "  --create-test-data Generate test data"
            echo "  --target TARGET  Deploy to specific target (dev/prod)"
            echo "  --help           Show this help"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Main execution
echo "DBX MetaGen Deployment"
echo "Target: $TARGET"
APP_ENV=${TARGET}

copy_variables_to_app() {
    #Copy variables to app folder if it exists
    if [ -f "resoures/app_variables.yml" ]; then
    cp resources/app_variables.yml app/ 2>/dev/null || true
    fi

    if [ -f "variables.yml" ]; then
    cp variables.yml app/ 2>/dev/null || true
    fi
}

# Deploy everything
#create_secret_scope
create_deploying_user_yml
create_app_env_yml
copy_variables_to_app
check_for_deployed_app
validate_bundle
deploy_bundle
start_app
cleanup_temp_yml_files

#Run permissions if requested
if [ "$RUN_PERMISSIONS" = true ]; then
       run_permissions_setup
fi

echo "Deployment complete!"
echo "Access your app in Databricks workspace > Apps > dbxmetagen-app"