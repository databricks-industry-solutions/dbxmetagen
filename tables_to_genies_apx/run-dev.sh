#!/bin/bash
# APX App Development Server Launcher
# Starts both FastAPI backend and React frontend (Vite dev server)

set -e

PROJECT_ROOT=$(cd "$(dirname "$0")" && pwd)
BACKEND_DIR="$PROJECT_ROOT/src"
FRONTEND_DIR="$PROJECT_ROOT/src/tables_genies/ui"

echo "🚀 Starting Tables to Genies APX App (Development Mode)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Kill any existing processes on ports 8000 and 3000
echo ""
echo "🧹 Cleaning up existing processes..."
lsof -ti:8000 | xargs kill -9 2>/dev/null || true
lsof -ti:3000 | xargs kill -9 2>/dev/null || true
pkill -9 -f "uvicorn.*8000" 2>/dev/null || true
pkill -9 -f "vite.*3000" 2>/dev/null || true
pkill -9 -f "bun.*dev" 2>/dev/null || true
sleep 2
echo "✅ Ports 8000 and 3000 are now free"

# Clean up any existing processes on exit
cleanup() {
    echo ""
    echo "🛑 Shutting down..."
    if [ ! -z "$BACKEND_PID" ] && kill -0 $BACKEND_PID 2>/dev/null; then
        kill $BACKEND_PID 2>/dev/null || true
    fi
    if [ ! -z "$FRONTEND_PID" ] && kill -0 $FRONTEND_PID 2>/dev/null; then
        kill $FRONTEND_PID 2>/dev/null || true
    fi
    # Also kill by port as backup
    lsof -ti:8000 | xargs kill -9 2>/dev/null || true
    lsof -ti:3000 | xargs kill -9 2>/dev/null || true
    wait $BACKEND_PID $FRONTEND_PID 2>/dev/null || true
    echo "✅ Cleanup complete"
}
trap cleanup EXIT INT TERM

# Activate virtual environment for Python dependencies
echo ""
echo "🐍 Activating virtual environment..."
VENV_PATH="$PROJECT_ROOT/../.venv"
if [ -f "$VENV_PATH/bin/activate" ]; then
    source "$VENV_PATH/bin/activate"
    echo "✅ Virtual environment activated ($VENV_PATH)"

    # Verify key packages are available
    if python3 -c "import community; import networkx; print('✅ Graph analysis packages ready')" 2>/dev/null; then
        echo "✅ Graph analysis packages ready"
    else
        echo "⚠️  Warning: Graph analysis packages not available"
    fi
else
    echo "⚠️  Warning: Virtual environment not found at $VENV_PATH"
    echo "   Make sure to run setup_venv.sh from the project root first"
fi

# Start Backend (FastAPI on port 8000)
echo ""
echo "📦 Starting FastAPI backend on http://localhost:8000"
echo "   API docs: http://localhost:8000/docs"
echo "   OpenAPI:  http://localhost:8000/openapi.json"
echo ""
cd "$BACKEND_DIR"
python3 -m uvicorn tables_genies.backend.main:app \
  --host 0.0.0.0 \
  --port 8000 \
  --reload &
BACKEND_PID=$!

# Wait for backend to start and stabilize
echo "⏳ Waiting for backend to start..."
sleep 5

# Verify backend is running
if curl -s -I http://localhost:8000/openapi.json | grep -q "200 OK"; then
    echo "✅ Backend is UP and responding"
else
    echo "⚠️  Backend may still be starting..."
fi

# Start Frontend (Vite dev server on port 3000)
echo ""
echo "⚛️  Starting React frontend (Vite dev server)"
cd "$FRONTEND_DIR"
bun run dev &
FRONTEND_PID=$!

# Wait for frontend to start
echo "⏳ Waiting for frontend to start..."
sleep 6

# Verify frontend is running
if curl -s -I http://localhost:3000 | grep -q "200 OK"; then
    echo "✅ Frontend is UP and responding"
else
    echo "⚠️  Frontend may still be starting..."
fi

# Display final startup info
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "✨ Servers are running!"
echo ""
echo "📱 Frontend:   http://localhost:3000"
echo "🔌 Backend:    http://localhost:8000"
echo "📚 API Docs:   http://localhost:8000/docs"
echo "📄 OpenAPI:    http://localhost:8000/openapi.json"
echo ""
echo "🎯 To use the app:"
echo "   1. Open http://localhost:3000 in your browser"
echo "   2. Hard refresh (Cmd+Shift+R or Ctrl+Shift+R) if needed"
echo ""
echo "Press Ctrl+C to stop both servers"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

# Wait for all background jobs
wait
