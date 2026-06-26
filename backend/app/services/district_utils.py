"""
Shared district-name normalization.

Used by both the FastAPI router (to normalize the incoming ``district`` query
parameter) and the import script (to derive ``district_key`` from GeoJSON
district names), so the API and the database stay in sync regardless of casing
or Turkish characters.

This module deliberately depends only on the standard library so it is safe to
import from a CLI script without pulling in FastAPI/asyncpg.
"""

from __future__ import annotations

import re

# Turkish-character → ASCII map for safe, deterministic district keys.
_TR_MAP = str.maketrans({
    "ş": "s", "Ş": "s",
    "ı": "i", "İ": "i",
    "ğ": "g", "Ğ": "g",
    "ü": "u", "Ü": "u",
    "ö": "o", "Ö": "o",
    "ç": "c", "Ç": "c",
    "â": "a", "Â": "a",
    "î": "i", "Î": "i",
    "û": "u", "Û": "u",
})


def normalize_district_key(name: str | None) -> str:
    """Normalize a district name to a safe key (lowercase ASCII, no spaces).

    Examples
    --------
    "Beşiktaş" -> "besiktas", "Şişli" -> "sisli", "Üsküdar" -> "uskudar",
    "Kadıköy" -> "kadikoy", "Bakırköy" -> "bakirkoy", "Ataşehir" -> "atasehir",
    "Sarıyer" -> "sariyer", "Çekmeköy" -> "cekmekoy",
    "Büyükçekmece" -> "buyukcekmece", "Küçükçekmece" -> "kucukcekmece".
    Whitespace and casing are stripped/normalized away.
    """
    s = (name or "").strip().translate(_TR_MAP).lower()
    return re.sub(r"[^a-z0-9]", "", s)
