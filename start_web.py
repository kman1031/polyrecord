#!/usr/bin/env python3
"""
Start the Polyrecord web dashboard.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

import uvicorn
from web.server import app

if __name__ == "__main__":
    print("=" * 60)
    print("Polyrecord Web Dashboard")
    print("=" * 60)
    print("\nOpen your browser and navigate to:")
    print("\n    http://localhost:8000\n")
    print("=" * 60)

    uvicorn.run(app, host="127.0.0.1", port=8000, log_level="info")
