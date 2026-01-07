#!/bin/bash

# Databricks Asset Bundle Cleanup Script
# This script removes your deployed DABs project

set -e  # Exit on any error

echo "Databricks Asset Bundle Cleanup"
echo "=================================="

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to prepare variables for bundle operations
prepare_variables() {
    print_status "Preparing variables for bundle operations..."
    
    # Backup original variables.yml
    if [ -f variables.yml ]; then
        cp variables.yml variables.yml.cleanup.bkp
    fi
    
    # Load dev.env if it exists
    if [ -f dev.env ]; then
        print_status "Loading environment from dev.env..."
        set -a  # Export all variables
        source dev.env
        set +a
    fi
    
    # Create variables_override.yml with any missing variables
    > variables_override.yml  # Create empty file
    
    if [ -n "$DATABRICKS_HOST" ]; then
        cat >> variables_override.yml << EOF
  workspace_host:
    default: "$DATABRICKS_HOST"
EOF
    fi
    
    if [ -n "$permission_groups" ]; then
        cat >> variables_override.yml << EOF
  permission_groups:
    default: "$permission_groups"
EOF
    fi
    
    if [ -n "$permission_users" ]; then
        cat >> variables_override.yml << EOF
  permission_users:
    default: "$permission_users"
EOF
    fi
    
    # Append overrides to variables.yml if any were created
    if [ -s variables_override.yml ]; then
        cat variables_override.yml >> variables.yml
        print_status "Applied variable overrides"
    fi
}

# Function to restore original variables.yml
restore_variables() {
    if [ -f variables.yml.cleanup.bkp ]; then
        mv variables.yml.cleanup.bkp variables.yml
        print_status "Restored original variables.yml"
    fi
    if [ -f variables_override.yml ]; then
        rm variables_override.yml
    fi
}

# Check if databricks CLI is installed
if ! command -v databricks &> /dev/null; then
    print_error "Databricks CLI is not installed or not in PATH"
    exit 1
fi

# Prepare variables before bundle operations
prepare_variables

# Trap to ensure cleanup happens even on error
trap restore_variables EXIT

# Show what will be destroyed
print_status "Getting current deployment summary..."
databricks bundle summary

echo ""
print_warning "WARNING: This will permanently delete all deployed resources!"
print_warning "   - Jobs will be deleted"
print_warning "   - Notebooks will be removed from workspace"
print_warning "   - All bundle artifacts will be cleaned up"
echo ""

read -p "Are you sure you want to destroy the bundle? (y/N): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    print_status "Destroying bundle..."
    if databricks bundle destroy; then
        print_success "Bundle destroyed successfully!"
    else
        print_error "Bundle destruction failed"
        exit 1
    fi
else
    print_status "Cleanup cancelled"
    exit 0
fi

echo ""
print_success "Cleanup completed!" 