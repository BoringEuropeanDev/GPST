"""Health check"""
from fastapi import APIRouter
from datetime import datetime

router = APIRouter()

@router.get("/")
async def health():
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "service": "Global Predictive Stock Terminal"
    }
