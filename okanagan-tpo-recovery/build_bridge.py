#!/usr/bin/env python3
"""Regenerate config/unit_tile_bridge.csv from config/stream_units.csv.

Replaces the manual "query F3" step the README described. For each OKAN
assessment-watershed tile, the owning unit is the one whose ``aw_include_prefix``
is the LONGEST prefix of the tile's FWA_WATERSHED_CODE. Longest-prefix-wins
implements the nesting rule ("a downstream unit excludes any listed upstream
unit") automatically: a Coldstream tile matches both Vernon's prefix and
Coldstream's longer one, so it lands in Coldstream without a hand-maintained
exclusion. Validated 2026-07-21 to reproduce the original 71-tile bridge
exactly (zero unit_id changes, per-tile area diff <0.05 ha).

Tile source:
  --oracle              pull OKAN tiles from BCGW (needs BCGW_* creds); default
  --tiles-csv PATH      use a cached inventory (cols WATERSHED_FEATURE_ID,
                        FWA_WATERSHED_CODE, AREA_HA) - offline, reproducible

Checks (all fatal): no tile claimed by two units at equal prefix length;
every unit in stream_units.csv wins at least one tile; and for any unit that
documents an aw_exclude_prefix (';'-separated), no tile assigned to it starts
with an excluded prefix - keeping the documentation honest against the result.
"""
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

TILE_COLS = ["WATERSHED_FEATURE_ID", "FWA_WATERSHED_CODE", "AREA_HA", "GNIS_NAME_1"]


def groups_for(units: pd.DataFrame) -> tuple[str, ...]:
    """Watershed groups to pull, from the watershed_group column (default OKAN).

    Nearly every unit is OKAN; a cross-group unit (one with a non-OKAN
    watershed_group) sets its own group so the tile pull reaches into it too.
    """
    if "watershed_group" in units.columns:
        return tuple(sorted(set(units["watershed_group"].fillna("OKAN"))))
    return ("OKAN",)


def load_tiles_oracle(groups: tuple[str, ...] = ("OKAN",)) -> pd.DataFrame:
    from tpo import io_sources as io
    con = io.oracle_connect()
    placeholders = ", ".join(f":g{i}" for i in range(len(groups)))
    params = {f"g{i}": g for i, g in enumerate(groups)}
    try:
        df = io._query(con, f"""
            SELECT WATERSHED_FEATURE_ID, FWA_WATERSHED_CODE, AREA_HA, GNIS_NAME_1
            FROM WHSE_BASEMAPPING.FWA_ASSESSMENT_WATERSHEDS_POLY
            WHERE WATERSHED_GROUP_CODE IN ({placeholders})
        """, params)
    finally:
        con.close()
    return df


def load_tiles_csv(path: str | Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    return df[[c for c in TILE_COLS if c in df.columns]]


def assign(tiles: pd.DataFrame, units: pd.DataFrame) -> pd.DataFrame:
    prefixes = [(u.aw_include_prefix, u.unit_id) for u in units.itertuples()
                if isinstance(u.aw_include_prefix, str) and u.aw_include_prefix]
    rows = []
    for t in tiles.itertuples():
        code = str(t.FWA_WATERSHED_CODE)
        hits = [(len(p), uid) for p, uid in prefixes if code.startswith(p)]
        if not hits:
            continue
        top = max(h[0] for h in hits)
        winners = {uid for ln, uid in hits if ln == top}
        if len(winners) > 1:
            raise SystemExit(
                f"FATAL: tile {t.WATERSHED_FEATURE_ID} ({code}) claimed at equal "
                f"prefix length by {sorted(winners)} - fix stream_units.csv")
        rows.append({"unit_id": next(uid for ln, uid in hits if ln == top),
                     "watershed_feature_id": int(t.WATERSHED_FEATURE_ID),
                     "fwa_watershed_code": code,
                     "gnis_name": getattr(t, "GNIS_NAME_1", None),
                     "area_ha": round(float(t.AREA_HA), 1)})
    return pd.DataFrame(rows).sort_values(["unit_id", "watershed_feature_id"])


def validate(bridge: pd.DataFrame, units: pd.DataFrame) -> None:
    assigned = set(bridge["unit_id"])
    missing = [u for u in units["unit_id"] if u not in assigned]
    if missing:
        raise SystemExit(f"FATAL: units won no tiles (bad prefix?): {missing}")
    # honesty check: documented excludes must not appear in a unit's own tiles
    for u in units.itertuples():
        exc = getattr(u, "aw_exclude_prefix", None)
        if not isinstance(exc, str) or not exc:
            continue
        own = bridge[bridge["unit_id"] == u.unit_id]["fwa_watershed_code"]
        for pfx in [p.strip() for p in exc.split(";") if p.strip()]:
            bad = own[own.str.startswith(pfx)]
            if len(bad):
                raise SystemExit(
                    f"FATAL: {u.unit_id} keeps {len(bad)} tile(s) under its "
                    f"documented exclude prefix {pfx}")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--config", default="config")
    ap.add_argument("--oracle", action="store_true", help="pull OKAN tiles (default)")
    ap.add_argument("--tiles-csv", help="cached tile inventory instead of Oracle")
    ap.add_argument("--out", default=None, help="default: <config>/unit_tile_bridge.csv")
    args = ap.parse_args()

    cfgdir = Path(args.config)
    units = pd.read_csv(cfgdir / "stream_units.csv")
    groups = groups_for(units)
    tiles = (load_tiles_csv(args.tiles_csv) if args.tiles_csv
             else load_tiles_oracle(groups))
    print(f"groups {groups}: {len(tiles)} tiles | units: {len(units)}")

    bridge = assign(tiles, units)
    validate(bridge, units)

    out = Path(args.out) if args.out else cfgdir / "unit_tile_bridge.csv"
    bridge.to_csv(out, index=False)
    print(f"bridge written: {out}  ({len(bridge)} tiles)\n")

    # per-unit summary with the named-watershed area cross-check
    summ = (bridge.groupby("unit_id")
            .agg(tiles=("watershed_feature_id", "size"),
                 selected_area_ha=("area_ha", "sum")).reset_index()
            .merge(units[["unit_id", "named_watershed_area_ha"]], on="unit_id",
                   how="left"))
    summ["area_diff_ha"] = (summ["selected_area_ha"]
                            - summ["named_watershed_area_ha"]).round(1)
    print(summ.to_string(index=False))


if __name__ == "__main__":
    main()
