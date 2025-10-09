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


echo "Created $TARGET_FILE with service principal permissions added to dev section"
}

check_for_deployed_app() {
    cd ../
    SP_ID=$(databricks apps get "dbxmetagen-app" --output json 2>/dev/null | jq -r '.id' || echo "")
    echo "SP_ID: $SP_ID"
    cd dbxmetagen
    export APP_SP_ID="$SP_ID"
    
    if [ -z "$SP_ID" ] || [ "$SP_ID" = "null" ]; then
        echo "App does not exist. Will create it in first deployment..."
        export APP_EXISTS=false
    else        
        echo "App already exists. Using existing SP ID: $APP_SP_ID"
        export APP_EXISTS=true
    fi
}

validate_bundle() {
    echo "Validating bundle..."
    if ! databricks bundle validate -t "$TARGET"; then
        echo "Error: Bundle validation failed"
        exit 1
    fi
}

deploy_bundle() {

    TARGET="${TARGET:-dev}"
    echo "Deploying to $TARGET..."
    
    # Build var flags only for debug/test modes (simpler boolean values)
    local DEPLOY_VARS=()
    if [ "$DEBUG_MODE" = true ]; then
        DEPLOY_VARS+=("--var" "debug_mode=true")
    fi
    if [ "$CREATE_TEST_DATA" = true ]; then
        DEPLOY_VARS+=("--var" "create_test_data=true")
    fi
    
    if ! databricks bundle deploy --target "$TARGET" "${DEPLOY_VARS[@]}"; then
        echo "Error: Bundle deployment failed with target $TARGET"
        exit 1
    fi
    
    export DEPLOY_TARGET="$TARGET"
    echo "Bundle deployed successfully"
}

run_permissions_setup() {
    echo "Setting up permissions..."
    
    local catalog_name="${catalog_name:-dbxmetagen}"
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
    
    echo "✅ deploying_user.yml created with user: $CURRENT_USER"
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
    
    echo "✅ env_overrides.yml created"
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
    if [ -f variables_override.yml ]; then
        echo "Cleaning up variables_override.yml..."
        rm variables_override.yml
    fi
    if [ -f databricks_final.yml ]; then
        echo "Cleaning up databricks_final.yml..."
        rm databricks_final.yml
    fi
    if [ -f databricks_final.yml.tmp ]; then
        echo "Cleaning up databricks_final.yml.tmp..."
        rm databricks_final.yml.tmp
    fi
}

start_app() {
    echo "App ID: $APP_ID"
    echo "Service Principal ID: $APP_SP_ID"

    # Create deploying_user.yml before deployment

    # Deploy and run the app
    databricks bundle run -t "$TARGET" \
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
CURRENT_USER=$(databricks current-user me --profile DEFAULT --output json | jq -r '.userName')
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

create_variables_override_yml() {
    # Create a variables override file that will be included in the bundle
    # This is cleaner than passing --var flags and handles complex values better
    echo "Creating variables override file from dev.env..."
    
    # Check if dev.env exists
    if [ ! -f "dev.env" ]; then
        echo "No dev.env found, creating empty override file"
        cat > variables_override.yml << 'EOF'
# Auto-generated during deployment
variables: {}
EOF
        return
    fi
    
    cat > variables_override.yml << 'EOF'
# Auto-generated from dev.env during deployment
# This file overrides default values in variables.yml
variables:
EOF
    
    # Add workspace_host override if set
    if [ -n "$DATABRICKS_HOST" ]; then
        cat >> variables_override.yml << EOF
  workspace_host:
    default: "$DATABRICKS_HOST"
EOF
        echo "  ✓ Setting workspace_host from DATABRICKS_HOST"
    fi
    
    # Add permission_groups override if set  
    if [ -n "$permission_groups" ]; then
        cat >> variables_override.yml << EOF
  permission_groups:
    default: "$permission_groups"
EOF
        echo "  ✓ Setting permission_groups: $permission_groups"
    fi
    
    # Add permission_users override if set
    if [ -n "$permission_users" ]; then
        cat >> variables_override.yml << EOF
  permission_users:
    default: "$permission_users"
EOF
        echo "  ✓ Setting permission_users: $permission_users"
    fi
    
    echo "✅ Created variables_override.yml"
}

# Deploy everything
#create_secret_scope

# Source dev.env properly (handles values with spaces and special characters)
if [ -f "dev.env" ]; then
    echo "Loading environment variables from dev.env..."
    set -a  # automatically export all variables
    source dev.env
    set +a  # turn off automatic export
fi

HOST_URL=$DATABRICKS_HOST
TARGET=$TARGET

# Create override files from environment variables
create_variables_override_yml
create_deploying_user_yml
create_app_env_yml
create_env_overrides_yml
check_for_deployed_app

if [ "$APP_EXISTS" = false ]; then
    echo "=== First deployment: Creating app without SP ==="
    validate_bundle
    deploy_bundle
    
    # Get the newly created SP ID
    cd ../
    APP_SP_ID=$(databricks apps get "dbxmetagen-app" --output json 2>/dev/null | jq -r '.id' || echo "")
    cd dbxmetagen
    export APP_SP_ID
    
    if [ -z "$APP_SP_ID" ] || [ "$APP_SP_ID" = "null" ]; then
        echo "Error: Failed to get SP ID after first deployment"
        exit 1
    fi
    
    echo "=== Second deployment: Updating with SP ID: $APP_SP_ID ==="
    add_service_principal_simple
    validate_bundle
    deploy_bundle
else
    echo "=== Single deployment: App exists, using SP ID: $APP_SP_ID ==="
    add_service_principal_simple
    validate_bundle
    deploy_bundle
fi

start_app
cleanup_temp_yml_files

#Run permissions if requested
if [ "$RUN_PERMISSIONS" = true ]; then
       run_permissions_setup
fi

echo "Deployment complete!"
echo "Access your app in Databricks workspace > Apps > dbxmetagen-app"