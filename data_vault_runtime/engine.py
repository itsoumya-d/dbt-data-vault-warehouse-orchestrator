"""Transformation and staging engine for Data Vault 2.0 tables."""
from __future__ import annotations
from typing import List, Dict, Any, Sequence, Optional, Set
from data_vault_runtime.hashing import hash_key, hash_diff
from data_vault_runtime.models import HubRecord, LinkRecord, SatRecord


def stage_hub(
    records: Sequence[Dict[str, Any]],
    bk_field: str,
    rec_src: str,
    load_dts: str,
    existing_hubs: Optional[Set[str]] = None,
) -> List[HubRecord]:
    """Compiles distinct Hub records from source records, ignoring already loaded hash keys."""
    seen = set(existing_hubs or set())
    hubs: List[HubRecord] = []

    for r in records:
        raw_bk = r.get(bk_field)
        if raw_bk is None or str(raw_bk).strip() == "":
            continue
        bk = str(raw_bk).strip()
        hk = hash_key(bk)
        if hk not in seen:
            seen.add(hk)
            hubs.append(HubRecord(hk_hub=hk, business_key=bk, load_dts=load_dts, rec_src=rec_src))

    return hubs


def stage_link(
    records: Sequence[Dict[str, Any]],
    relationship_name: str,
    parent_bk_fields: Sequence[str],
    rec_src: str,
    load_dts: str,
    existing_links: Optional[Set[str]] = None,
) -> List[LinkRecord]:
    """Compiles distinct Link records representing relationships between business keys."""
    seen = set(existing_links or set())
    links: List[LinkRecord] = []

    for r in records:
        bks = [str(r.get(f) or "").strip() for f in parent_bk_fields]
        if any(not b for b in bks):
            continue
        parent_hks = tuple(hash_key(b) for b in bks)
        link_hk = hash_key(*parent_hks)
        if link_hk not in seen:
            seen.add(link_hk)
            links.append(
                LinkRecord(
                    hk_link=link_hk,
                    hk_parents=parent_hks,
                    relationship_name=relationship_name,
                    load_dts=load_dts,
                    rec_src=rec_src,
                )
            )

    return links


def stage_satellite(
    records: Sequence[Dict[str, Any]],
    bk_field: str,
    attribute_fields: Sequence[str],
    rec_src: str,
    load_dts: str,
    latest_hash_diffs: Optional[Dict[str, str]] = None,
) -> List[SatRecord]:
    """
    Compiles Satellite records applying change data capture (CDC).
    Only records where hash_diff changed relative to latest_hash_diffs are staged.
    """
    diff_state = dict(latest_hash_diffs or {})
    sats: List[SatRecord] = []

    for r in records:
        raw_bk = r.get(bk_field)
        if raw_bk is None or str(raw_bk).strip() == "":
            continue
        bk = str(raw_bk).strip()
        hk = hash_key(bk)

        attrs = {k: r.get(k) for k in attribute_fields if k in r}
        hdiff = hash_diff(attrs)

        # Only stage if new entity or attributes have changed
        if diff_state.get(hk) != hdiff:
            diff_state[hk] = hdiff
            sats.append(
                SatRecord(
                    hk_parent=hk,
                    hash_diff=hdiff,
                    attributes=attrs,
                    load_dts=load_dts,
                    rec_src=rec_src,
                )
            )

    return sats


def validate_vault_integrity(
    hubs: Sequence[HubRecord],
    links: Sequence[LinkRecord],
    sats: Sequence[SatRecord],
) -> Dict[str, Any]:
    """Validates referential integrity and consistency across staged vault entities."""
    hub_keys = {h.hk_hub for h in hubs}
    errors = []

    # Verify link parent foreign keys
    for l in links:
        for p_hk in l.hk_parents:
            if p_hk not in hub_keys:
                errors.append(f"Link {l.hk_link} ({l.relationship_name}) references unknown parent hub {p_hk}")

    # Verify satellite parent foreign keys
    for s in sats:
        if s.hk_parent not in hub_keys:
            errors.append(f"Satellite with diff {s.hash_diff} references unknown parent hub {s.hk_parent}")

    return {
        "is_valid": len(errors) == 0,
        "total_hubs": len(hubs),
        "total_links": len(links),
        "total_satellites": len(sats),
        "errors": errors,
    }
