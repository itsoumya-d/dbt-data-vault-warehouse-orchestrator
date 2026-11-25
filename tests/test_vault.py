"""Unit tests for Data Vault 2.0 hashing, staging, and CDC validation."""
import pytest
from data_vault_runtime.hashing import hash_key, hash_diff
from data_vault_runtime.engine import stage_hub, stage_link, stage_satellite, validate_vault_integrity


def test_hash_key_determinism_and_formatting():
    hk1 = hash_key("BOOKING-1001")
    hk2 = hash_key("  booking-1001  ")
    assert hk1 == hk2
    assert len(hk1) == 64  # SHA-256 hex string

    # Null handling
    hk_null1 = hash_key(None)
    hk_null2 = hash_key("")
    assert hk_null1 == hk_null2


def test_hash_diff_invariance():
    # Attribute dict key order should not change the hash diff
    diff1 = hash_diff({"amount": 420.50, "currency": "NZD", "status": "CONFIRMED"})
    diff2 = hash_diff({"status": "CONFIRMED", "currency": "NZD", "amount": 420.50})
    assert diff1 == diff2

    # Modified value should change hash diff
    diff3 = hash_diff({"status": "CANCELLED", "currency": "NZD", "amount": 420.50})
    assert diff1 != diff3


def test_stage_hub_deduplication():
    records = [
        {"booking_id": "B-01", "pax": "John"},
        {"booking_id": "B-02", "pax": "Jane"},
        {"booking_id": "B-01", "pax": "John Duplicate"},
    ]
    hubs = stage_hub(records, bk_field="booking_id", rec_src="booking_engine", load_dts="2026-11-25T10:00:00Z")
    assert len(hubs) == 2
    bks = {h.business_key for h in hubs}
    assert bks == {"B-01", "B-02"}


def test_stage_link_relationships():
    records = [
        {"booking_id": "B-01", "traveler_id": "T-100"},
        {"booking_id": "B-02", "traveler_id": "T-200"},
    ]
    links = stage_link(
        records,
        relationship_name="lnk_booking_traveler",
        parent_bk_fields=["booking_id", "traveler_id"],
        rec_src="erp_travel",
        load_dts="2026-11-25T10:00:00Z",
    )
    assert len(links) == 2
    assert links[0].relationship_name == "lnk_booking_traveler"
    assert len(links[0].hk_parents) == 2


def test_stage_satellite_cdc_tracking():
    records_batch1 = [
        {"booking_id": "B-01", "fare_class": "Y", "cost": 300},
        {"booking_id": "B-02", "fare_class": "J", "cost": 1200},
    ]
    # Initial load: 2 new satellite records
    sats1 = stage_satellite(
        records_batch1,
        bk_field="booking_id",
        attribute_fields=["fare_class", "cost"],
        rec_src="booking_engine",
        load_dts="2026-11-25T10:00:00Z",
    )
    assert len(sats1) == 2

    # Second load: B-01 is unchanged, B-02 cost changes to 1100
    state = {s.hk_parent: s.hash_diff for s in sats1}
    records_batch2 = [
        {"booking_id": "B-01", "fare_class": "Y", "cost": 300},   # unchanged
        {"booking_id": "B-02", "fare_class": "J", "cost": 1100},  # changed
    ]
    sats2 = stage_satellite(
        records_batch2,
        bk_field="booking_id",
        attribute_fields=["fare_class", "cost"],
        rec_src="booking_engine",
        load_dts="2026-11-25T11:00:00Z",
        latest_hash_diffs=state,
    )
    # Only B-02 should produce a new satellite record
    assert len(sats2) == 1
    assert sats2[0].attributes["cost"] == 1100


def test_vault_referential_integrity():
    records = [{"booking_id": "B-01", "cust_id": "C-99"}]
    hubs = stage_hub(records, "booking_id", "source", "dts")
    links = stage_link(records, "lnk_b_c", ["booking_id", "cust_id"], "source", "dts")
    sats = stage_satellite(records, "booking_id", ["cust_id"], "source", "dts")

    # Links references C-99 which is not in hubs -> should fail integrity
    res = validate_vault_integrity(hubs, links, sats)
    assert res["is_valid"] is False
    assert len(res["errors"]) > 0

    # Add customer hub
    cust_hubs = stage_hub(records, "cust_id", "source", "dts")
    all_hubs = hubs + cust_hubs
    res_valid = validate_vault_integrity(all_hubs, links, sats)
    assert res_valid["is_valid"] is True
    assert len(res_valid["errors"]) == 0
