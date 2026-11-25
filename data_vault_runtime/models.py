"""Data Vault 2.0 entity representations."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, Any, Tuple


@dataclass(frozen=True)
class HubRecord:
    hk_hub: str
    business_key: str
    load_dts: str
    rec_src: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "hk_hub": self.hk_hub,
            "business_key": self.business_key,
            "load_dts": self.load_dts,
            "rec_src": self.rec_src,
        }


@dataclass(frozen=True)
class LinkRecord:
    hk_link: str
    hk_parents: Tuple[str, ...]
    relationship_name: str
    load_dts: str
    rec_src: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "hk_link": self.hk_link,
            "hk_parents": list(self.hk_parents),
            "relationship_name": self.relationship_name,
            "load_dts": self.load_dts,
            "rec_src": self.rec_src,
        }


@dataclass(frozen=True)
class SatRecord:
    hk_parent: str
    hash_diff: str
    attributes: Dict[str, Any]
    load_dts: str
    rec_src: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "hk_parent": self.hk_parent,
            "hash_diff": self.hash_diff,
            "attributes": self.attributes,
            "load_dts": self.load_dts,
            "rec_src": self.rec_src,
        }
