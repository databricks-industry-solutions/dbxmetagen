"""
FastAPI application entry point with React frontend serving.
"""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pathlib import Path
from .router import api

app = FastAPI(
    title="Tables to Genies API",
    description="API for creating Genie rooms from UC tables",
    version="1.0.0"
)

# CORS for frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include API routes
app.include_router(api)

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy"}

# Serve React frontend
ui_dist_path = Path(__file__).parent.parent / "ui" / "dist"
if ui_dist_path.exists():
    # Mount static assets
    app.mount("/assets", StaticFiles(directory=str(ui_dist_path / "assets")), name="assets")
    
    # Serve index.html for all non-API routes
    @app.get("/{path:path}")
    async def serve_frontend(path: str):
        """Serve React frontend for all routes."""
        if path.startswith("api/"):
            return {"error": "Not found"}
        
        index_file = ui_dist_path / "index.html"
        if index_file.exists():
            return FileResponse(index_file)
        return {"error": "Frontend not built"}
