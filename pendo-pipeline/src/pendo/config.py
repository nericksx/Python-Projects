# src/pendo/config.py
"""
Centralized configuration.

Loads environment variables (via .env) and exposes them as named constants used
by extract.py (and nowhere else). Keeps secrets/config out of the pull/endpoints
layers.
"""

import os
from dotenv import load_dotenv

load_dotenv()

BASE_URL: str = os.getenv("PENDO_BASE_URL", "")

API_KEY_SAE: str = os.getenv("PENDO_KEY_SAE", "")
API_KEY_CAE: str = os.getenv("PENDO_KEY_CAE", "")