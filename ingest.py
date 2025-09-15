#!/usr/bin/env python3
"""
CMA Data Engineer ETL technical test - reference implementation
- Ingest listings.csv and category_lookup.csv
- Validate & clean (missing/invalid, types, ranges)
- Deduplicate
- Enrich via lookup
- Write Parquet
- Emit JSON data quality report

Usage:
  python ingest.py --listings data_listings.csv --lookup data_category_lookup.csv --outdir ./output
"""
import argparse, json, re, sys
from pathlib import Path
import pandas as pd
import numpy as np
import datetime as dt

REQUIRED_COLS = ["date","seller_id","region","category","item_id","price_gbp","condition"]
VALID_REGIONS = {"North","South","East","West"}
VALID_CONDITION = {"new","used","refurbished"}  # allow a small controlled vocabulary

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--listings", required=True)
    ap.add_argument("--lookup", required=True)
    ap.add_argument("--outdir", required=True)
    return ap.parse_args()

def load_csvs(listings_path, lookup_path):
    df = pd.read_csv(listings_path)  # read as strings; cast later
    lk = pd.read_csv(lookup_path)
    return df, lk

def standardise_columns(df):
    df = df.copy()
    df.columns = [c.strip().lower() for c in df.columns]
    return df

def coerce_types(df):
    df = df.copy()
    # date -> date
    df["date_parsed"] = pd.to_datetime(df["date"], errors="coerce", format="%Y-%m-%d")
    # numeric price
    df["price_gbp_num"] = pd.to_numeric(df["price_gbp"], errors="coerce")
    # normalise strings
    for c in ["seller_id","region","category","item_id","condition"]:
        df[c] = df[c].astype("string").str.strip()
    df["region"] = df["region"].str.title()
    df["category"] = df["category"].str.title()
    df["condition"] = df["condition"].str.lower()
    return df

def validate(df):
    """Return (mask_valid, issues dict)"""
    issues = {}
    n = len(df)
    # required non-null
    null_counts = {c: int(df[c].isna().sum()) for c in REQUIRED_COLS if c in df.columns}
    # missing due to empty strings
    empty_counts = {c: int((df[c].astype(str).str.strip()=="").sum()) for c in REQUIRED_COLS if c in df.columns}

    # invalid date / price
    invalid_date = df["date_parsed"].isna()
    invalid_price = df["price_gbp_num"].isna() | (df["price_gbp_num"] <= 0)

    # invalid enums
    invalid_region = ~df["region"].isin(VALID_REGIONS)
    invalid_condition = ~df["condition"].isin(VALID_CONDITION)

    issues["missing_nulls"] = null_counts
    issues["missing_empty"] = empty_counts
    issues["invalid_date"] = int(invalid_date.sum())
    issues["invalid_price"] = int(invalid_price.sum())
    issues["invalid_region"] = int(invalid_region.sum())
    issues["invalid_condition"] = int(invalid_condition.sum())

    # validity mask (fail if any required invalid)
    mask_valid = (~invalid_date &
                  ~invalid_price &
                  ~invalid_region &
                  ~invalid_condition)

    # also require non-empty essential fields
    essential = ["date","seller_id","category","item_id","price_gbp"]
    for c in essential:
        mask_valid &= ~df[c].isna()
        mask_valid &= (df[c].astype("string").str.strip().notna())
        mask_valid &= (df[c].astype("string").str.strip() != "")

    return mask_valid, issues

def dedupe(df):
    # choose natural key: date + seller + item
    before = len(df)
    df = df.sort_values(["date_parsed","seller_id","item_id","price_gbp_num"], kind="stable")
    df = df.drop_duplicates(subset=["date_parsed","seller_id","item_id"], keep="first")
    removed = before - len(df)
    return df, int(removed)

def enrich(df, lookup):
    lookup = lookup.rename(columns={"category":"category","segment":"segment"})
    lookup["category"] = lookup["category"].str.title()
    df = df.merge(lookup, how="left", on="category")
    return df

def quality_stats(df):
    # simple descriptive stats used in DQ report
    price = df["price_gbp"].dropna() if "price_gbp" in df.columns else df["price_gbp_num"].dropna()
    return {
        "row_count": int(len(df)),
        "price": {
            "min": float(price.min()) if len(price) else None,
            "max": float(price.max()) if len(price) else None,
            "avg": float(price.mean()) if len(price) else None,
            "p50": float(price.quantile(0.5)) if len(price) else None,
            "p95": float(price.quantile(0.95)) if len(price) else None
        },
        "by_region": df["region"].value_counts(dropna=False).to_dict(),
        "by_category": df["category"].value_counts(dropna=False).to_dict(),
        "by_segment": df["segment"].value_counts(dropna=False).to_dict() if "segment" in df.columns else {}
    }

def main():
    args = parse_args()
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    df_raw, lk = load_csvs(args.listings, args.lookup)
    df_raw = standardise_columns(df_raw)

    # schema presence
    missing_cols = [c for c in REQUIRED_COLS if c not in df_raw.columns]
    if missing_cols:
        raise SystemExit(f"ERROR: required columns missing: {missing_cols}")

    # coerce / validate
    df_typed = coerce_types(df_raw)
    valid_mask, issues = validate(df_typed)
    df_invalid = df_typed[~valid_mask].copy()
    df_valid = df_typed[valid_mask].copy()

    # dedupe
    df_valid, dup_removed = dedupe(df_valid)

    # enrich
    df_enriched = enrich(df_valid, lk)

    # output parquet
    out_parquet = outdir / "clean_listings.parquet"
    # Choose tidy output columns
    out_cols = ["date_parsed","seller_id","region","category","segment","item_id","price_gbp_num","condition"]
    rename = {"date_parsed":"date","price_gbp_num":"price_gbp"}
    df_out = df_enriched[out_cols].rename(columns=rename)
    # write parquet (fallback to CSV if parquet engine is unavailable)
    try:
        df_out.to_parquet(out_parquet, index=False)
    except Exception as e:
        alt_csv = outdir / "clean_listings.csv"
        df_out.to_csv(alt_csv, index=False)
        print(f"WARNING: parquet engine not available: {e}. Wrote CSV fallback to {alt_csv}")

    # compile DQ report
    dq = {
        "input": {
            "rows": int(len(df_raw))
        },
        "validation_issues": issues,
        "rows_removed_invalid": int(len(df_invalid)),
        "rows_removed_duplicates": dup_removed,
        "output": quality_stats(df_out)
    }

    # write JSON report
    out_json = outdir / "data_quality_report.json"
    out_json.write_text(json.dumps(dq, indent=2))

    # also emit a CSV of invalid rows for transparency
    if len(df_invalid):
        df_invalid.to_csv(outdir / "invalid_rows.csv", index=False)

    # Emit a small README to explain usage
    (outdir / "README.txt").write_text(
        "Artifacts generated by ingest.py\n"
        f"- clean_listings.parquet: {len(df_out)} clean, deduplicated, enriched rows\n"
        f"- data_quality_report.json: metrics and validation issues\n"
        f"- invalid_rows.csv: rows dropped during validation ({len(df_invalid)})\n"
    )

    print(f"Wrote {out_parquet} and {out_json}. Clean rows: {len(df_out)} | Invalid dropped: {len(df_invalid)} | Duplicates: {dup_removed}")

if __name__ == "__main__":
    main()
