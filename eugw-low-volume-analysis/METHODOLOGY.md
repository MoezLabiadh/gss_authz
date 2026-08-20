# EUGW Low Volume Threshold Analysis — Methodology and Handover

**Status:** All five steps run end to end against BCGW and the real client file. Results in §6.
**Last updated:** 2026-08-12

---

## 1. What this analysis answers

The Province is considering a legislative change that would exempt very low volume
groundwater users from the requirement to hold a licence. the client needs to know
roughly how many of the outstanding Existing Use Groundwater (EUGW) applications
would fall under that exemption, and where they are.

The spatial layer shows the regional
distribution, which matters for workload planning and for consultation.

**This is a screening analysis, not a determination.** Nothing here authorises or
refuses anything. It estimates how many applications would likely qualify.

---

## 2. Client decisions

Everything in this section came from the client and should not be changed without
going back to her. Dates are when the decision was received.

| # | Decision | Detail | Date |
|---|---|---|---|
| 1 | Threshold | 2 m³/day or less, groundwater wells specifically | 2026-08 |
| 2 | Assessment unit | Per purpose, not per application | 2026-08 |
| 3 | Volume conversion | Already done by client, using days in the applied season (not 365) | 2026-08 |
| 4 | Population | The 712 applications in her spreadsheet. She had already removed: volumes over 2 m³/day; completed, abandoned or regionally reassigned files; applications with a second purpose over 2 m³/day ("licence still required"); dugout-only applications | 2026-08 |
| 5 | Spatial basis | Work from aquifers and application PIDs. Wells are not matched or registered yet | 2026-08 |
| 6 | Aquifer allocation status | Include aquifers flagged Fully Recorded | 2026-08 |
| 7 | TPO scope | **Any watershed that has experienced an order**, not only those currently in effect | 2026-08 |
| 8 | Region | West Coast and South Coast; the spreadsheet is the scope | 2026-08 |
| 8b | **Location source** | **Non-negotiable.** The PID is the primary location. The well is used only where the application has no PID, or the PID overlaps several aquifers — in which case the well determines which one. See §5.1 | 2026-08-10 |
| 9 | Undetermined aquifer | Where an application is matched to more than one aquifer it cannot be evaluated against the aquifer criteria, so do **not** pass or fail it. Material is irrelevant. Refined 2026-08-11: the status applies where the candidate aquifers *disagree*; where they all give the same answer the result stands. See §5.5 | 2026-08-11 |
| 10 | FR-EXC | No further consideration needed for this work — confirmed as not applying to this population | 2026-08-10 |

### 2.1 Population definition — read this before quoting any number

The spreadsheet is **one row per application** (712 rows, 712 unique tracking
numbers), not one row per purpose.

the client's stated rule is that the exemption applies **per purpose** — an
application with several purposes would have only the qualifying purpose
exempted. But her filtering removed applications where a second purpose exceeded
2 m³/day, on the basis that a licence is still required.

So the population is **applications where every purpose falls at or below
2 m³/day** — that is, applications that would drop out of the licensing queue
entirely. Applications that would be *partially* exempt are not included.

This was a deliberate client decision and was not challenged. But any number
produced here is a count of **fully exempt** applications, and should be labelled
as such. If someone later asks for applications *touched by* the exemption, that
is a larger number and requires a different source population.

### 2.2 Exemption criteria

Eight conditions were given. Four can be assessed with available data; four
cannot. The four that cannot are carried through the deliverable as
`not_assessed` columns so the gap is visible rather than silently absent.

| # | Criterion | Assessable | Source |
|---|---|---|---|
| 1 | Aquifer connected to a sensitive stream | Yes | Client list, 76 aquifers |
| 2 | Aquifer Fully Recorded | Yes | `WLS_WATER_NOTATION_AQUIFERS_SP` |
| 3 | Aquifer reserved under s.39–41 | Yes | `WLS_WATER_RESERVES_AQUIFERS_SP` |
| 4 | Watershed subject to s.88 order | Yes | `WLS_TEMP_PROT_ORDER_SV` |
| 5 | Streams subject to s.86/s.87 order | **No** | No order has ever been issued under either section |
| 6 | s.82 dedicated agricultural water | **No** | No spatial source found; unclear whether any regulation exists |
| 7 | Already serviced by a waterworks licensee | **No** | See 2.3 |
| 8 | No storage except closed systems | **No** | See 2.4 |

### 2.3 Why waterworks servicing cannot be derived

Water rights licences identify purveyors, their points of diversion and licensed
quantities. They do **not** record service areas — which properties are actually
connected. `WLS_WATER_LICENCED_WRK_LINE_SP` maps conveyance infrastructure but
generally covers the diversion side rather than the full distribution network.

There is also a wording issue: the criterion reads "serviced by a waterworks
licensee **for the intended non-domestic water use**." A property can be on a
community system for household supply while that system does not deliver
irrigation water. Even a complete service-area layer would not settle it.

This is an operational check. The most a spatial analysis could contribute is
flagging applications near a waterworks system as candidates for manual review.

### 2.4 Why the storage condition cannot be derived

A licence purpose of "Storage" means impounding water is the authorised purpose
of a licence — usually surface water in a dam or reservoir. The exemption
condition is about the **applicant's works**: whether pumped groundwater goes
into an open dugout or pond rather than a closed tank. An irrigation applicant
with an open storage pond holds no storage licence anywhere.

The information lives in the application, not in water rights. `Works` is
populated on only 48 of 712 rows. `Work Comments` is populated on 487, which may
support a text screen if the client wants one — worth raising, as it was not
explored.

Note that the client has already partly applied this criterion by removing
dugout-only applications, using a source we have not seen.

---

## 3. Data sources

### 3.1 Client spreadsheet

`EUGW Volumes_In Progress Low Volume.xlsx` — note the spaces; the config points
at the delivered filename rather than renaming the client's file.

| Sheet | Contents |
|---|---|
| `EUGW Low Volume` | 712 applications. Header on row 2 (`header=1`) |
| `EUGW Application Data` | Same 712 tracking numbers, without the conversion columns. Appears redundant |
| `Aquifers Overlapping Sensitive ` | 76 aquifers. **Note the trailing space in the sheet name** |

The `Aquifer` column is **empty on all 712 rows**. Deriving aquifer identity is
therefore the core of this analysis, not a lookup.

### 3.2 BCGW layers

All five verified against `ALL_TAB_COLUMNS` on 2026-08-10 and pulled
successfully.

| Purpose | Object | Verified |
|---|---|---|
| Parcels | `WHSE_CADASTRE.PMBC_PARCEL_FABRIC_POLY_SVW` | Yes |
| Aquifers | `WHSE_WATER_MANAGEMENT.GW_AQUIFERS_CLASSIFICATION_SVW` | Yes |
| Aquifer notations | `WHSE_WATER_MANAGEMENT.WLS_WATER_NOTATION_AQUIFERS_SP` | Yes |
| Water reserves | `WHSE_WATER_MANAGEMENT.WLS_WATER_RESERVES_AQUIFERS_SP` | Yes |
| Protection orders | `WHSE_WATER_MANAGEMENT.WLS_TEMP_PROT_ORDER_SV` | Yes |

Three field names in the original scripts were wrong and each stopped a run:

| Assumed | Actual | Object |
|---|---|---|
| `AQUIFER_MATERIALS` | `MATERIAL` | aquifers |
| `GEOMETRY` | `SHAPE` | protection orders |
| `NOTATION_TYPE` | `NOTATION_DESCRIPTION` | aquifer notations |

Geometry column naming is **not** consistent across objects: parcels and
protection orders use `SHAPE`, aquifers use `GEOMETRY`. All three are now config
values rather than literals in the scripts.

> `WHSE_WATER_MANAGEMENT.WLS_STREAM_RESTRICTIONS_SP` was cited early in scoping
> and does not exist. The correct object for stream notations is
> `WLS_WATER_NOTATION_STREAMS_SP`. Re-verify every name above on any schema
> refresh — the failures above were all silent-looking at config level and only
> surfaced as `ORA-00904` at run time.

### 3.3 What the BCGW exports showed

**Water Reserves – Aquifers.** 19 records, **18 distinct aquifers** (606 appears
twice under two reserve IDs), all under a single Order in Council 0347/2024 dated
2024-06-17, all active. There is **no aquifer number field** — it is embedded in
`WATER_RESOURCE_MGMT_DESC` as free text and must be parsed with a regex. Verify
the parse count against the record count on every refresh.

**14 of the 18 are already on the client's sensitive-stream list.** Only 205, 213,
215 and 1285 are new, so this criterion adds very little.

**Aquifer notations.** 171 records, one row per notation. The table has a single
`NOTATION_DESCRIPTION` field — there is **no primary/secondary notation pair** —
and the code is embedded in free text: `"143 - FR-EXC - 2023/10/10"`. The format
is uniform across all 171 rows, so codes are matched as whole `" - "`-delimited
segments. Distinct codes present:

| Code | Rows | Distinct aquifers |
|---|---|---|
| `PWS` | 134 | |
| `FR` | 17 | 14 |
| `OR` | 10 | |
| `FR-EXC` | 9 | 9 |
| `AR` | 1 | |

Matching on segments rather than substrings matters: `"FR"` is a substring of
`"FR-EXC"`, so a `contains` test would make the two codes inseparable.

**Temporary Protection Orders.** 16 records as of 2026-08-10: 13 under WSA s.88,
3 under the older Fish Protection Act s.9. **No order has ever been issued under
s.86 or s.87.** 14 are historical, 2 current (Salmon M285/2026 and M292/2026,
both Interior — outside scope). The layer gained M292/2026 between the original
export and this run; the count is live and should be re-checked, not quoted from
here.

Within West Coast and South Coast the only watersheds ever subject to an order
are **Koksilah** (2019, 2021, 2023) and **Tsolum** (2023). The client's spreadsheet
contains 2 Koksilah and 5 Tsolum applications, consistent with her comment that
most applications in those areas have already been processed.

The layer also carries `NUM_EUGW_APPS_AFFCTD`, a pre-existing count of affected
EUGW applications, on records from 2023 onward. Not currently used; potentially a
cross-check.

> **CSV export warning.** Both exports contain multi-line WKT geometry that breaks
> naive CSV row counting — the reserves file looks like 33 rows but holds 19, the
> TPO file looks like 30 but holds 15. Pull these as feature classes, not CSV.

---

## 4. Data quality findings

All figures below are from step 01 run against the real client file.

### 4.1 Coordinates — around 40% carried a sign error

Of 907 well records extracted from the 712 applications:

| Status | Count |
|---|---|
| `ok` | 577 |
| `sign_corrected` | 283 |
| `missing` | 19 |
| `outside_bc` | 18 |
| `low_precision` | 10 |

**289 longitudes were positive** — the negative sign was lost somewhere upstream.
They would plot in China. The fix flips the sign **only where doing so places the
point inside BC**; a blind flip would corrupt correct values.

The sign check runs **per coordinate, not per row**. Some multi-well applications
mix correct and incorrect signs within the same cell — one row has 17 coordinates
of which exactly one is positive.

**18 records remain outside BC even after correction** and are excluded from
spatial work. These need a manual look.

**10 coordinates are integer-only** (latitude 48, longitude -123). These are
truncated placeholders, not locations, and are excluded.

### 4.2 Delimited lists are positionally aligned

Latitude, longitude and well tag lists use empty slots as placeholders:
`104617; ` means well 1 has a tag and well 2 does not.

**Split without dropping empty tokens.** Dropping them silently mis-pairs tags to
locations. Lat/lon counts match on every row in the current file, so the pairing
is reliable when parsed correctly.

### 4.3 PIDs

Arrive hyphenated (`018-266-433`); PMBC stores a 9-character zero-padded string
with no hyphens. 781 tokens across 712 applications, 762 distinct parcels
(19 parcels are shared between applications).

| Status | Count |
|---|---|
| `ok` | 766 |
| `invalid` (no digits, zero, or over 9 digits) | 8 |
| `padded` (fewer than 9 digits, zero-filled) | 7 |

Expect some valid PIDs not to resolve in PMBC — retired or subdivided parcels.
Step 00 logs these to `qa_missing_pids.csv`.

### 4.4 Locatability

| Source | Count | % |
|---|---|---|
| Both parcel and well | 571 | 80.2% |
| Parcel only | 73 | 10.3% |
| Well only | 60 | 8.4% |
| **Neither — cannot be located** | **8** | **1.1%** |

The 60 well-only applications are the argument for not discarding coordinates.
Following the PID-only direction literally would lose them.

### 4.5 Out-of-scope regions

The spreadsheet contains 22 applications outside West Coast and South Coast:
Kootenay 15, Thompson Okanagan 5, Omineca 2. the client said the scope is West
Coast and South Coast but also said to work from the spreadsheet, so **all 712
are processed** and `region` is carried through for filtering downstream. Do not
silently drop them.

---

## 5. Method

### 5.1 Aquifer assignment — two independent passes

**Pass 1, parcel.** PID → PMBC polygon → intersect aquifers. Multi-parcel
applications are dissolved first so overlap shares are per application. Returns
**all** intersecting aquifers with `overlap_share`, the proportion of parcel area
in each.

**Pass 2, well point.** Cleaned coordinate → point-in-polygon. Returns every
aquifer polygon containing the point — which is **not necessarily one**. Where
aquifers are layered the point falls inside all of them, and the well pass is
no more able to separate them than the parcel pass is. See §5.5.

**Which one decides — client rule, non-negotiable (2026-08).** The PID is the
primary location. The well is used **only** where the PID cannot answer:

| `assignment_basis` | Rule | Applications |
|---|---|---|
| `parcel` | Parcel resolves to exactly one aquifer. The well is ignored, even where it disagrees | 320 |
| `well_parcel_ambiguous` | Parcel straddles several aquifers; the well says which one | 215 |
| `well_no_pid` | No usable PID; the well is the only location | 68 |
| `parcel_ambiguous_no_well` | Parcel straddles several and there is no usable well. All are carried — the ambiguity is kept, not guessed away | 37 |

The second case is why both passes are still computed: a parcel straddling a
boundary genuinely cannot say which aquifer the water comes from, and a well
point can. Parcel-only assignment **over-flags** — an application would be
excluded because its parcel clips a restricted aquifer the well is nowhere near.

Both passes are written to `By_aquifer` in full; `assigned` marks the rows the
rule selected (980 of 1,782). Nothing is discarded, so the rule can change
without re-running any spatial work, and any assignment can be audited back to
the two passes that produced it.

`agreement` (`parcel_only`, `well_only`, `agree`, `partial_agree`, `disagree`)
is now **diagnostic only** — it does not affect any status. It does not mean the
aquifer is correct either: where aquifers are layered both passes return the same
aquifers and the application is flagged `agree`, corroborating while remaining
aquifers, corroborating while equally ambiguous. §5.5 is what acts on that.

**Full polygons, not centroids.** A centroid can fall outside an L-shaped or
multipart parcel, and it discards the multi-aquifer case. Centroids appear only
in step 04 for the output point layer, and there via `representative_point()`,
which is guaranteed to fall inside the geometry.

**Overlap kept as a proportion**, not a boolean, so the inclusion threshold
(`aquifer_assignment.min_parcel_overlap`) stays a parameter. Default 0.0 — any
intersection counts.

### 5.2 Consistency check

Does each well coordinate fall inside its own application's parcel? Where it
does, both sources corroborate. Where it does not, one is wrong — bad coordinate,
wrong PID, or a well genuinely sited off-parcel. Written to
`qa_well_in_parcel.csv`. Cheap, and catches errors neither source reveals alone.

### 5.3 Criteria

Criteria 1–3 apply at aquifer level; criterion 4 at application level. TPO
membership is determined **spatially** rather than by matching the client's
`Watershed` text field — watershed names are ambiguous (there is a "Salmon" in
both the Interior and on the coast) and the field is blank on 72 rows. Name
matching exists only as a fallback.

Fully Recorded checks **every notation row for the aquifer**. A single aquifer
can carry several notations, and the table holds one row per notation rather than
a primary/secondary column pair, so filtering must be by membership across rows.
Codes are parsed out of `NOTATION_DESCRIPTION` as described in §3.3.

On this population the criterion is **inert**: all 18 applications it flags are
already excluded by the sensitive-stream or water-reserve criteria, so removing
it entirely would not change a single verdict. It is retained because that is a
property of the current data, not of the method.

### 5.4 Collapse

The long table becomes one row per application only in step 04, governed by
`criteria.collapse_rule`:

- `any` (default) — fails if any associated aquifer is restricted. Conservative.
- `all` — fails only if all associated aquifers are restricted.

Keeping the collapse at the last step means the rule can change, or both can be
shown, without re-running any spatial work.

`qualifies` is therefore **four-valued**, not boolean:

| Value | Meaning |
|---|---|
| `True` | passes all four assessable criteria |
| `False` | fails at least one |
| `further_assessment_needed` | aquifer undetermined and the candidates disagree, see §5.5 |
| `not_assessable` | no aquifer resolved at all |

Precedence: `not_assessable` first (an application with no aquifer has nothing to
be ambiguous about), then the undetermined-aquifer status, then pass/fail.

### 5.5 Undetermined aquifers

BC aquifer mapping is 2D and carries no vertical extents. Where an application is
matched to **more than one aquifer**, nothing in the data says which one the well
draws from, so the three aquifer-level criteria cannot be evaluated against "the"
aquifer. **233 applications** of the 640 resolved are in that position.

**Material is not the test, and an earlier version of this analysis was wrong to
use it.** That version flagged only sand and gravel lying over bedrock. But
application 100219357 is matched to four *confined sand and gravel* aquifers — 32
Beaver River, 51 Murrayville AC, 58 Nicomekl-Serpentine, 1194 Salmon River — which
are layered at one location just as genuinely, and a parcel spanning two
neighbouring aquifers is equally undetermined. The material-based rule caught 152
of the 233 and missed 81 for no principled reason.

**Being undetermined does not always change the answer.** Splitting the 233:

| | Applications |
|---|---|
| Every candidate aquifer fails — Excluded is certain | 97 |
| No candidate fails — Qualifies is certain | 90 |
| Candidates disagree — genuinely undetermined | **46** |

100219357 is in the first group: all four of its aquifers are on the
sensitive-stream list, so it is excluded whichever one the well draws from.
Reviewing that well cannot change the outcome.

**The rule:** `further_assessment_needed` applies where the aquifer is
undetermined **and** the candidates disagree — 46 applications. Everything else
keeps a definite result, and `aquifer_undetermined` marks all 233 so the
ambiguity stays visible even where it costs nothing.

The watershed criterion is judged on the application rather than the aquifer
(`criteria.application_level`) and so stays decisive throughout. It is excluded
from the disagreement test; none of the 46 fail it, so nothing is masked.

**The criterion columns are three-valued for the same reason.** `collapse_rule:
any` reduces each criterion across the candidate aquifers, which reports a
restriction as fact when only one candidate carries it. For application
100198431 — aquifer 197 (Sand and Gravel, clean) and 203 (Bedrock, sensitive
stream and water reserve) — the row asserted two failures while its own status
said the aquifer was unknown.

Where a criterion's answer differs between the candidate aquifers the cell now
reads `Depends on aquifer` rather than TRUE or FALSE, and `n_criteria_failed` is
left blank. **82 applications** are affected: the 46 held for review, plus 36
that are Excluded whichever aquifer is correct while an individual condition
still turns on the choice. Per criterion: sensitive stream 49, water reserve 41,
Fully Recorded 17.

This makes the criterion columns non-boolean for those rows, which is
deliberate — it is the same reason `qualifies` is not boolean — but anyone
counting with a formula needs to filter on the value rather than assume TRUE or
FALSE. It also tells a reviewer exactly which condition a well inspection would
settle.

`aquifer_assignment.undetermined_aquifer.scope` switches the behaviour:

- `disagreeing` (default) — the 46 above.
- `any` — all 233, never asserting a result on an unconfirmed aquifer. This sends
  187 applications for well review where the review cannot change the outcome.

This distinction matters only if the deliverable is asked *which aquifer* an
application sits on. For "would this application qualify", the 46 is the honest
answer; for per-aquifer reporting, all 233 are unknown.

---

## 6. Results

Run of 2026-08-10, 712 applications.

| Status | Applications | % |
|---|---|---|
| Qualifying | 408 | 57.3% |
| Excluded on at least one criterion | 186 | 26.1% |
| Further assessment needed (aquifer undetermined and it matters) | 46 | 6.5% |
| Not assessable (no aquifer resolved) | 72 | 10.1% |

**How many applications each criterion reaches** (these overlap, and exceed the
Excluded total because applications held for well review are counted here):

| Criterion | Applications |
|---|---|
| Sensitive stream | 232 |
| Water reserve | 128 |
| Fully Recorded | 18 |
| TPO watershed | 7 |

**Aquifer resolution.** 640 of 712 resolved (89.9%). Of the 72 that did not, 8
cannot be located at all (§4.4); the other 64 have a good parcel or well that
simply is not over any mapped aquifer.

**Method agreement**, over the 640: `agree` 403, `parcel_only` 96, `well_only`
68, `partial_agree` 58, `disagree` 15. Read §5.1 before quoting `agree` — it does
not mean the aquifer is confirmed.

**Aquifer undetermined: 233** (more than one aquifer matched), of which **46**
have a result that turns on which aquifer is correct. The other 187 keep a
definite result — see §5.5. On the Applications sheet 100 of the 233 read
Excluded and 87 read Qualifies; `aquifer_undetermined` is TRUE on all of them.

---

## 7. Known limitations

**The aquifer is undetermined for 233 applications.** Flagged, not resolved —
see §5.5. `Well Depth` is present in the source, but BC aquifer mapping carries
no vertical extents to test against, so depth cannot settle it from data alone,
hence individual well review. 187 of the 233 keep a definite result because
their candidate aquifers agree; that is sound for the question "would this
qualify" but means **the deliverable cannot say which aquifer 233 applications
sit on**. State this in anything client-facing that reports per-aquifer.

**Four of eight criteria unassessed.** See §2.2. Applications passing the four
testable criteria might still fail one of the four that cannot be tested, which
pushes the qualifying count **down** from 328.

Note the two error directions do not cancel and should not be described as a
simple bound: the unassessed criteria can only reduce the qualifying count, while
46 applications sit outside the count entirely until their wells are reviewed.
408 is neither a floor nor a ceiling — it is the count that passes the four
testable criteria on the aquifers currently assigned.

**Parcel-based assignment over-flags where the parcel is ambiguous.** Addressed
rather than merely quantified: under the client rule (§5.1) the well decides for
the 215 applications whose parcel straddles several aquifers. It remains
unaddressed for the 37 whose parcel is ambiguous and which have no usable well —
those carry every parcel aquifer and so can still be excluded by an aquifer the
well might not be in. `method_disagreement` (73) is retained as a diagnostic and
no longer affects any status.

**Volumes are taken as supplied.** The client's conversion to m³/day is not
recomputed and no volume threshold is applied anywhere in the pipeline; the
population is accepted as filtered (§2.1). One application (100373651) carries
the literal text `Total Flow?` in place of a converted quantity and has no
volume established.

**Population currency.** The 712 come from a client extract. Applications
continue to be processed, so the number is true as of a moment. Date every
deliverable. The `Status` field in the underlying EUGW master data is known to
be stale and was not used.

**Sensitive-stream aquifer list.** The client's tab is named "Aquifers
Overlapping Sensitive" while the criterion is *hydraulic connection*. Whether the
76 are a final determination or a first-pass overlap has not been confirmed.
Label output accordingly.

---

## 8. Running it

Dependencies are declared in `pyproject.toml` and locked in `uv.lock`. `uv run`
creates and updates `.venv` on its own; there is no activation step.

```bash
uv sync                        # once, to build .venv from the lock file

export BCGW_USER=your_idir     # PowerShell: $env:BCGW_USER = "your_idir"
export BCGW_PASSWORD=your_password

uv run scripts/01_prepare_applications.py   # clean the spreadsheet
uv run scripts/00_extract_bcgw.py           # needs PIDs from step 01
uv run scripts/02_resolve_aquifers.py
uv run scripts/03_apply_criteria.py
uv run scripts/04_outputs.py
```

pandas is held below 3.0 deliberately — see the note in `pyproject.toml`.

Step 01 runs before step 00 because the BCGW parcel query is filtered by the
cleaned PID list.

```
config.yaml                    all parameters; nothing hard-coded in scripts
data/raw/                      client spreadsheet
data/interim/                  cleaned tables, BCGW cache, QA logs
data/outputs/                  deliverables
```

Re-running steps 02–04 does not re-query Oracle. Only re-run step 00 when source
data is refreshed.

### Design principles

- **Oracle for extraction only.** No spatial operators. Parcels are filtered
  server-side because PMBC holds millions of rows and we need ~760; batch around
  the 1000-item `IN` limit. Everything else is small enough to pull whole, and
  all spatial logic runs locally in geopandas where it is debuggable.
- **Geometry as WKB**, not WKT — more compact, no CLOB round-trip.
- **Long tables until the last step**, so combination rules stay parameters.
- **BC Albers (EPSG:3005) throughout.** Well coordinates are geographic (4326)
  and are reprojected.

---

## 9. Open items

### Open

| # | Item | Owner |
|---|---|---|
| 2 | 18 coordinates outside BC after sign correction. These look like **misplaced decimals** (`0.49328`, `4.941259`, `8.817368` against BC latitudes of 48–49), not sign errors. Step 01 does not attempt that correction | Analyst |
| 5 | Would a text screen on `Work Comments` help the storage criterion? | Client |
| 6 | Who maintains `NUM_EUGW_APPS_AFFCTD` in the TPO layer? | Deferred |
| 11 | 46 of 759 PIDs are not in PMBC (retired or subdivided), listed in `qa_missing_pids.csv`. Those applications fall back to well coordinates where they have them | Analyst |
| 12 | 129 applications have at least one well outside their own parcel (§5.2). Sample before trusting either method on those | Analyst |
| 13 | 64 applications are locatable but over no mapped aquifer, reported `not_assessable`. Confirm that is the right treatment rather than presumed qualifying | Client |
| 14 | Application 100373651 has `Total Flow?` in place of a converted quantity. Client direction is to exclude it from the analysis and report it; **not yet implemented** | Analyst |
| 15 | Confirm `undetermined_aquifer.scope` — `disagreeing` (46, current) or `any` (233). Only matters if the client needs the aquifer *identified* rather than the result. See §5.5 | Client |

### Closed

| # | Item | Outcome |
|---|---|---|
| 1 | Verify all BCGW object and field names | Done 2026-08-10. Three defects found and fixed; see §3.2 |
| 3 | Is the 76-aquifer list a determination or a first-pass overlap? | Use as supplied; client data not challenged |
| 4 | Should out-of-scope regions (22 applications) be reported separately? | No — include them, `region` is carried for downstream filtering |
| 7 | Confirm FR-EXC counts as fully recorded | Client 2026-08-10: no further consideration needed. Inert on this data — no application sits on any of the nine FR-EXC aquifers |

---

## 10. Change log

| Date | Change |
|---|---|
| 2026-08-06 | Initial pipeline. Step 01 tested against client file; steps 00, 02–04 written, untested against BCGW |
| 2026-08-12 | Criterion columns made three-valued. `collapse_rule: any` was reporting a restriction as fact where only one candidate aquifer carried it, contradicting the status on the same row; those cells now read `Depends on aquifer` and `n_criteria_failed` is blank. 82 applications affected. Workbook formatting moved into the script |
| 2026-08-11 | Undetermined-aquifer status redefined. Material was the wrong test: it flagged 152 and missed 81 with no principled basis (application 100219357 sits on four layered sand and gravel aquifers). Now keyed on more than one assigned aquifer, with the status applied only where the candidates disagree — 46. Results 408 / 186 / 46 / 72 |
| 2026-08-10 | PID-primary assignment rule implemented (§5.1). `primary_method` had been declared in config but read by no script; both passes were being unioned, so a well point could exclude an application whose PID resolved cleanly. Results 355 / 133 / 152 / 72 |
| 2026-08-10 | First full run of 00–04 against BCGW. All object and field names verified; three field-name defects fixed (§3.2). Fully Recorded criterion repaired — it had been matching a column that does not exist and silently returning nothing. Stacked-aquifer status added at client direction (§5.5). Results recorded in §6. Project moved to a `uv` environment under git |
