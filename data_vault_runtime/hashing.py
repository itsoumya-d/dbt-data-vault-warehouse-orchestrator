"""Deterministic Data Vault 2.0 hashing utilities matching dbtvault specifications."""
from __future__ import annotations
import hashlib
from typing import Any, Dict, Sequence


def hash_key(*keys: Any, delimiter: str = "||") -> str:
    """
    Computes a deterministic SHA-256 hash key over one or more business keys.
    Trims whitespace, lowercases, handles nulls with standard -1 replacement.
    """
    cleaned = []
    for k in keys:
        if k is None or k == "":
            cleaned.append("-1")
        else:
            cleaned.append(str(k).strip().lower())
    payload = delimiter.join(cleaned)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def hash_diff(attributes: Dict[str, Any]) -> str:
    """
    Computes a deterministic SHA-256 hash diff for Satellite change data capture (CDC).
    Sorts dictionary keys to guarantee field ordering invariance.
    """
    parts = []
    for k in sorted(attributes.keys()):
        val = attributes[k]
        if val is None or val == "":
            parts.append(f"{k}:-1")
        else:
            parts.append(f"{k}:{str(val).strip()}")
    payload = ";;".join(parts)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()
