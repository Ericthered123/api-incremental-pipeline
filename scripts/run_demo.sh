#!/bin/bash

# run_demo.sh - Demo Script for Interview
# This script demonstrates the complete pipeline workflow

set -e  # Exit on error

echo "========================================="
echo "  GitHub Events Pipeline - Demo"
echo "========================================="
echo ""

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored output
print_step() {
    echo -e "${BLUE}>>> $1${NC}"
}

print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_info() {
    echo -e "${YELLOW}ℹ $1${NC}"
}

# Check Python version
print_step "Checking Python version..."
python --version
print_success "Python is installed"
echo ""

# Check if requirements are installed
print_step "Checking dependencies..."
if ! python -c "import requests" 2>/dev/null; then
    print_info "Installing dependencies..."
    pip install -r requirements.txt
else
    print_success "Dependencies are installed"
fi
echo ""

# Clean previous run (optional)
read -p "Clean previous data? (y/n) " -n 1 -r
echo ""
if [[ $REPLY =~ ^[Yy]$ ]]; then
    print_step "Cleaning previous run..."
    rm -rf data/
    print_success "Data cleaned"
fi
echo ""

# FIRST RUN - Bootstrap
print_step "FIRST RUN - Bootstrap Mode"
print_info "This is the initial run with no checkpoint"
print_info "The pipeline will fetch all available events"
echo ""

echo "Press Enter to start..."
read

python src/main.py --env dev

print_success "First run completed!"
echo ""

# Show results
print_step "Results from first run:"
echo ""

if [ -f "data/state/checkpoint.json" ]; then
    echo "Checkpoint:"
    cat data/state/checkpoint.json | python -m json.tool
    echo ""
fi

if [ -f "data/metrics/latest_metrics.json" ]; then
    echo "Metrics Summary:"
    cat data/metrics/latest_metrics.json | python -c "
import sys, json
data = json.load(sys.stdin)
print(f\"Total events: {data['summary']['total_events']}\")
print(f\"Event types: {', '.join(data['event_types'].keys())}\")
print(f\"Top repo: {data['rankings']['top_repos'][0]['repo']} ({data['rankings']['top_repos'][0]['total_events']} events)\")
"
    echo ""
fi

# Wait for user
echo ""
print_info "The checkpoint has been saved with the latest timestamp"
print_info "Next run will only fetch NEW events after this timestamp"
echo ""
echo "Press Enter to continue with incremental run..."
read

# SECOND RUN - Incremental
print_step "SECOND RUN - Incremental Mode"
print_info "This run will use the checkpoint from the first run"
print_info "It should fetch fewer (or zero) new events"
echo ""

python src/main.py --env dev

print_success "Second run completed!"
echo ""

# Compare results
print_step "Comparing runs:"
if [ -f "data/state/run_history.jsonl" ]; then
    echo ""
    echo "Run History:"
    cat data/state/run_history.jsonl | python -c "
import sys, json
runs = [json.loads(line) for line in sys.stdin]
for i, run in enumerate(runs, 1):
    print(f\"Run {i}:\")
    print(f\"  Status: {run['status']}\")
    print(f\"  Records: {run['records_processed']}\")
    print(f\"  Duration: {run['duration_seconds']:.2f}s\")
    print()
"
fi

# Show file structure
print_step "Generated files:"
echo ""
tree -L 2 data/ 2>/dev/null || find data/ -type f

echo ""
echo "========================================="
print_success "Demo completed successfully!"
echo "========================================="
echo ""

print_info "Key observations:"
echo "1. First run (bootstrap) processes all available events"
echo "2. Second run (incremental) processes only new events"
echo "3. Checkpoint ensures idempotent processing"
echo "4. Data is organized in medallion layers (raw/curated/metrics)"
echo ""

print_info "Next steps:"
echo "- Review data/metrics/latest_metrics.json for aggregated results"
echo "- Check data/state/checkpoint.json to see the watermark"
echo "- Run 'pytest tests/' to execute unit tests"
echo "- Explore the code in src/ directory"
echo ""

print_step "Would you like to run the tests? (y/n)"
read -p "" -n 1 -r
echo ""
if [[ $REPLY =~ ^[Yy]$ ]]; then
    print_step "Running tests..."
    pytest tests/ -v
fi

echo ""
print_success "All done! 🚀"