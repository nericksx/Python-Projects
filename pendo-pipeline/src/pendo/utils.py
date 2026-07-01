# src/pendo/utils.py
from typing import Any

JOBROLE_MAP = {
    "Product Management": "PRODUCT_MANAGEMENT",
    "Product Design": "PRODUCT_DESIGN",
    "Research": "RESEARCH",
    # add as you encounter them
}

def normalize_job_role(value: Any) -> str | None:
    value = normalize_unset(value)
    if value is None:
        return None
    if isinstance(value, str):
        v = value.strip()
        return JOBROLE_MAP.get(v, v)
    return None

def normalize_unset(value: Any) -> Any:
    """
    Normalize common 'unset' / empty values to None.
    """
    if value is None:
        return None

    if isinstance(value, str):
        v = value.strip()
        if v == "" or v.lower() == "unset":
            return None
        return v

    return value