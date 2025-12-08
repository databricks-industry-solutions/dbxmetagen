#!/bin/bash
#
# Test Runner for dbxmetagen
#
# This script runs all unit tests in the correct order to avoid import conflicts.
# 
# Usage:
#   ./run_tests.sh              # Run all tests
#   ./run_tests.sh -v           # Verbose output
#   ./run_tests.sh -q           # Quick mode (core tests only)
#   ./run_tests.sh -s           # Summary only
#   ./run_tests.sh --help       # Show help
#

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default options
VERBOSE=""
QUICK_MODE=false
SUMMARY_ONLY=false
PYTEST_ARGS=""

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -v|--verbose)
            VERBOSE="-v"
            shift
            ;;
        -q|--quick)
            QUICK_MODE=true
            shift
            ;;
        -s|--summary)
            SUMMARY_ONLY=true
            shift
            ;;
        -h|--help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  -v, --verbose    Verbose output"
            echo "  -q, --quick      Quick mode (core tests only)"
            echo "  -s, --summary    Summary only (no detailed output)"
            echo "  -h, --help       Show this help message"
            echo ""
            echo "Examples:"
            echo "  $0              # Run all tests"
            echo "  $0 -v           # Run all tests with verbose output"
            echo "  $0 -q           # Run core tests only (fast)"
            echo "  $0 -s           # Run all tests, show summary only"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Set pytest output mode
if [ "$SUMMARY_ONLY" = true ]; then
    PYTEST_ARGS="-q"
elif [ -n "$VERBOSE" ]; then
    PYTEST_ARGS="-v"
else
    PYTEST_ARGS="-q"
fi

# Track test results
CORE_TESTS_PASSED=0
DDL_TESTS_PASSED=0
BINARY_TESTS_PASSED=0
TOTAL_TESTS=0

echo -e "${BLUE}╔════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║       dbxmetagen Unit Test Runner             ║${NC}"
echo -e "${BLUE}╔════════════════════════════════════════════════╗${NC}"
echo ""

# Function to run tests and capture results
# Returns test count via variable set by caller
run_test_suite() {
    local test_name=$1
    local test_command=$2
    local color=$3
    local result_var=$4
    
    echo -e "${color}▶ Running ${test_name}...${NC}"
    echo ""
    
    # Run tests and capture both output and exit status
    local temp_file=$(mktemp)
    local exit_code=0
    
    if [ "$SUMMARY_ONLY" = true ]; then
        # Silent mode - capture output
        eval "$test_command" > "$temp_file" 2>&1 || exit_code=$?
    else
        # Show output
        eval "$test_command" 2>&1 | tee "$temp_file" || exit_code=$?
    fi
    
    # Extract test count from output
    local test_count=$(grep -oE "[0-9]+ passed" "$temp_file" | head -1 | grep -oE "[0-9]+" || echo "0")
    
    if [ $exit_code -eq 0 ]; then
        # Tests passed
        if [ "$SUMMARY_ONLY" = false ]; then
            echo ""
            echo -e "${GREEN}✓ ${test_name} completed${NC}"
        else
            echo -e "${GREEN}✓ ${test_name}: ${test_count} tests passed${NC}"
        fi
        rm -f "$temp_file"
        eval "$result_var=$test_count"
        return 0
    else
        # Tests failed
        echo -e "${RED}✗ ${test_name} FAILED${NC}"
        if [ "$SUMMARY_ONLY" = true ]; then
            cat "$temp_file"
        fi
        rm -f "$temp_file"
        eval "$result_var=0"
        return 1
    fi
}

# 1. Run core unit tests
echo -e "${YELLOW}═══════════════════════════════════════════════${NC}"
echo -e "${YELLOW}1/3 Core Unit Tests${NC}"
echo -e "${YELLOW}═══════════════════════════════════════════════${NC}"
echo ""

CORE_CMD="poetry run pytest tests/ --ignore=tests/test_ddl_regenerator.py --ignore=tests/test_binary_variant_types.py $PYTEST_ARGS"

run_test_suite "Core Unit Tests" "$CORE_CMD" "$YELLOW" CORE_TESTS_PASSED
if [ $? -ne 0 ]; then
    echo -e "${RED}Core tests failed. Exiting.${NC}"
    exit 1
fi

if [ "$QUICK_MODE" = true ]; then
    echo ""
    echo -e "${GREEN}╔════════════════════════════════════════════════╗${NC}"
    echo -e "${GREEN}║  Quick Mode: Core Tests Completed             ║${NC}"
    echo -e "${GREEN}╠════════════════════════════════════════════════╣${NC}"
    echo -e "${GREEN}║  Total Tests Passed: ${CORE_TESTS_PASSED}                         ║${NC}"
    echo -e "${GREEN}╚════════════════════════════════════════════════╝${NC}"
    exit 0
fi

echo ""
echo ""

# 2. Run DDL regenerator tests
echo -e "${YELLOW}═══════════════════════════════════════════════${NC}"
echo -e "${YELLOW}2/3 DDL Regenerator Tests (Separate Process)${NC}"
echo -e "${YELLOW}═══════════════════════════════════════════════${NC}"
echo ""

DDL_CMD="poetry run pytest tests/test_ddl_regenerator.py $PYTEST_ARGS"

run_test_suite "DDL Regenerator Tests" "$DDL_CMD" "$YELLOW" DDL_TESTS_PASSED
if [ $? -ne 0 ]; then
    echo -e "${RED}DDL regenerator tests failed. Continuing...${NC}"
    DDL_TESTS_PASSED=0
fi

echo ""
echo ""

# 3. Run binary/variant tests
echo -e "${YELLOW}═══════════════════════════════════════════════${NC}"
echo -e "${YELLOW}3/3 Binary/Variant Tests (Separate Process)${NC}"
echo -e "${YELLOW}═══════════════════════════════════════════════${NC}"
echo ""

BINARY_CMD="poetry run pytest tests/test_binary_variant_types.py $PYTEST_ARGS"

run_test_suite "Binary/Variant Tests" "$BINARY_CMD" "$YELLOW" BINARY_TESTS_PASSED
if [ $? -ne 0 ]; then
    echo -e "${RED}Binary/variant tests failed. Continuing...${NC}"
    BINARY_TESTS_PASSED=0
fi

# Calculate total
TOTAL_TESTS=$((CORE_TESTS_PASSED + DDL_TESTS_PASSED + BINARY_TESTS_PASSED))

echo ""
echo ""

# Print summary
echo -e "${GREEN}╔════════════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║           Test Suite Summary                   ║${NC}"
echo -e "${GREEN}╠════════════════════════════════════════════════╣${NC}"
printf "${GREEN}║  Core Tests:         %3d passing                ║${NC}\n" $CORE_TESTS_PASSED
printf "${GREEN}║  DDL Regenerator:    %3d passing                ║${NC}\n" $DDL_TESTS_PASSED
printf "${GREEN}║  Binary/Variant:     %3d passing                ║${NC}\n" $BINARY_TESTS_PASSED
echo -e "${GREEN}╠════════════════════════════════════════════════╣${NC}"
printf "${GREEN}║  TOTAL:              %3d tests passing          ║${NC}\n" $TOTAL_TESTS
echo -e "${GREEN}╚════════════════════════════════════════════════╝${NC}"
echo ""

# Check if all test suites passed
if [ $CORE_TESTS_PASSED -gt 0 ] && [ $DDL_TESTS_PASSED -gt 0 ] && [ $BINARY_TESTS_PASSED -gt 0 ]; then
    echo -e "${GREEN}✓ All test suites completed successfully!${NC}"
    exit 0
else
    echo -e "${RED}✗ Some test suites failed. Please check the output above.${NC}"
    exit 1
fi

