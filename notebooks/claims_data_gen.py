"""Claims Intelligence Workbench — synthetic data generation (Phase 0).

Bricksurance SE / Guidewire ClaimCenter Cloud Data Access (CDA) *simulation*.
Everything here is synthetic — there is no real Guidewire integration and no
real customer data.

Design rules (do not break these — later phases depend on them):
  * Reproducible:    dbldatagen seed = 42 everywhere.
  * Rolling dates:   every loss/report/settlement date is derived from
                     `current_date()` (see `roll_dates`). No hardcoded years,
                     so the demo never goes stale.
  * Idempotent:      all writes are mode="overwrite". Re-running never dupes.
  * Money:           whole-pound integers / decimals. No stray floats.
  * Vivid claim:     cc:900001 is SACRED — fixed attributes, survives every
                     reset, appended explicitly (never randomly generated).

Deliberately-seeded business signals (for later-phase stories):
  * 80% of claims report < £5k, with a long tail to £250k.
  * Home escape-of-water is systematically UNDER-RESERVED (reserve ≈ 0.72x of
    what is needed) -> the "+28% under-reserving" story in Phase 3.
  * North-west districts (M, BL, OL, WN) get ~3x escape-of-water frequency.
  * ~2% intentionally malformed rows (bad policy_number / out-of-range
    fraud_score) -> DLT quarantine demo in Phase 1.
"""

from pyspark.sql import functions as F


# --------------------------------------------------------------------------
# Constants
# --------------------------------------------------------------------------
N_CLAIMS = 120_000
CLAIM_SEQ_START = 100_001          # bulk claim_public_ids: cc:100001 .. cc:220000
N_HANDLERS = 80
SEED = 42

VIVID_CLAIM_ID = "cc:900001"       # SACRED — see module docstring
VIVID_POLICY_NUMBER = "BSE-P-9009001"

# Motor policy ids occupy 1..40000, home 40001..70000, so a claim's product is
# recoverable from its policy id range and FK integrity is guaranteed.
N_MOTOR_POLICIES = 40_000
N_HOME_POLICIES = 30_000

# Real UK postcode districts. NW set (M / BL / OL / WN) is weighted to ~22% of
# claims so the escape-of-water skew has enough mass to be visible.
DISTRICTS = [
    # (district, weight) — NW districts first
    ("M1", 30), ("M14", 22), ("M20", 20), ("BL1", 16), ("BL3", 14),
    ("OL1", 12), ("OL9", 12), ("WN3", 11), ("WN5", 10),
    # non-NW
    ("EC1", 14), ("SW1", 14), ("E1", 14), ("N1", 13), ("SE1", 14),
    ("B1", 16), ("B15", 12), ("LS1", 14), ("LS6", 11), ("BS1", 13),
    ("NG1", 12), ("LE1", 11), ("CF10", 12), ("NE1", 13), ("S1", 13),
    ("CV1", 10), ("RG1", 10), ("OX1", 10), ("CB1", 9), ("CO1", 8),
    ("GL1", 8),
]

# 5–10 incident description templates per peril (loss_cause typecode).
DESC_TEMPLATES = {
    "vehcollision": [
        "Insured vehicle struck from behind at a junction.",
        "Side-impact collision while changing lanes on the motorway.",
        "Collision with third-party vehicle at a roundabout.",
        "Insured reversed into a parked third-party vehicle.",
        "Front-to-front collision at a give-way junction.",
        "Vehicle skidded on wet road and hit central reservation.",
        "Low-speed shunt in stationary traffic.",
        "Collision with cyclist at an urban crossing.",
    ],
    "waterdamage": [
        "Escape of water from a burst pipe under the kitchen sink.",
        "Failed washing-machine hose flooded the ground floor.",
        "Frozen pipe burst in the loft causing ceiling collapse.",
        "Leaking radiator valve damaged flooring and skirting.",
        "Overflowing bath flooded the bathroom and room below.",
        "Slow leak from shower tray rotted the subfloor.",
        "Mains supply pipe fractured, flooding the hallway.",
        "Dishwasher seal failure flooded the kitchen overnight.",
    ],
    "windhail": [
        "Storm-force winds dislodged roof tiles and damaged guttering.",
        "Fallen tree branch damaged the conservatory roof.",
        "Hail damaged roof skylights and external rendering.",
        "Wind-driven rain ingress through damaged flashing.",
        "Chimney pot blown down, cracking the roof slope.",
        "Garden fence and outbuilding roof destroyed by gale.",
        "Wind lifted a section of flat-roof felt covering.",
    ],
    "fire": [
        "Kitchen fire originating from an unattended hob.",
        "Electrical fault ignited fire in the consumer unit.",
        "Chimney fire caused smoke and heat damage throughout.",
        "Tumble-dryer fire spread to the utility room.",
        "Candle ignited curtains causing localised fire damage.",
        "Faulty extension lead caused a smouldering fire.",
        "Garden bonfire spread to the timber garage.",
    ],
}

_FIRST_NAMES = [
    "Amara", "Ben", "Chloe", "Dev", "Ewan", "Farah", "Grace", "Harvey",
    "Imran", "Jade", "Kofi", "Leila", "Marcus", "Nadia", "Omar", "Priya",
    "Quinn", "Rosa", "Sam", "Tara", "Umar", "Vera", "Wesley", "Yasmin", "Zane",
]
_LAST_NAMES = [
    "Ahmed", "Brooks", "Chen", "Doyle", "Evans", "Fischer", "Ghosh", "Hughes",
    "Iqbal", "Jensen", "Kaur", "Lowe", "Mensah", "Novak", "Owusu", "Patel",
    "Quigley", "Reed", "Singh", "Turner", "Usman", "Vance", "Walsh", "Yates",
]


# --------------------------------------------------------------------------
# Rolling-date helper (reused by Phase 9 reset to re-anchor the whole demo)
# --------------------------------------------------------------------------
def roll_dates(df, days_ago_col, out_col, anchor=None):
    """Add `out_col` = anchor - `days_ago_col` days.

    `anchor` defaults to current_date() so the demo rolls forward in time and
    never goes stale. Pass an explicit 'YYYY-MM-DD' string to re-anchor.
    """
    anchor_sql = "current_date()" if anchor is None else f"to_date('{anchor}')"
    return df.withColumn(out_col, F.expr(f"date_sub({anchor_sql}, {days_ago_col})"))


def _anchor_sql(anchor=None):
    return "current_date()" if anchor is None else f"to_date('{anchor}')"


# --------------------------------------------------------------------------
# Core claims table (dbldatagen for primitives, Spark expr for business logic)
# --------------------------------------------------------------------------
def build_claims(spark, anchor=None):
    """Return the bulk `bronze_gw_cc_claim` DataFrame (vivid claim added later).

    Carries a few helper columns (policy_seq, product, postcode_district,
    report_lag_days, is_bad_*) consumed by the child-table builders. Callers
    should select the public schema before persisting `bronze_gw_cc_claim`.
    """
    import dbldatagen as dg

    district_vals = [d for d, _ in DISTRICTS]
    district_wts = [w for _, w in DISTRICTS]

    spec = (
        dg.DataGenerator(spark, name="cc_claim", rows=N_CLAIMS,
                         partitions=8, randomSeed=SEED)
        .withColumn("claim_seq", "long",
                    minValue=CLAIM_SEQ_START, maxValue=CLAIM_SEQ_START + N_CLAIMS - 1,
                    uniqueValues=N_CLAIMS, random=False)
        .withColumn("postcode_district", "string",
                    values=district_vals, weights=district_wts, random=True)
        .withColumn("u_amount", "double", minValue=0.0, maxValue=1.0, random=True)
        .withColumn("u_cause", "double", minValue=0.0, maxValue=1.0, random=True)
        .withColumn("u_channel", "double", minValue=0.0, maxValue=1.0, random=True)
        .withColumn("u_status", "double", minValue=0.0, maxValue=1.0, random=True)
        .withColumn("days_ago_loss", "int", minValue=1, maxValue=1080, random=True)
        .withColumn("report_lag_days", "int", minValue=0, maxValue=40, random=True)
        .withColumn("policy_pick", "double", minValue=0.0, maxValue=1.0, random=True)
        .withColumn("u_prior", "double", minValue=0.0, maxValue=1.0, random=True)
        .withColumn("u_fraud", "double", minValue=0.0, maxValue=1.0, random=True)
        .withColumn("u_role", "double", minValue=0.0, maxValue=1.0, random=True)
        .withColumn("u_paid", "double", minValue=0.0, maxValue=1.0, random=True)
        .withColumn("u_sig", "double", minValue=0.0, maxValue=1.0, random=True)
        .withColumn("desc_pick", "int", minValue=0, maxValue=7, random=True)
        .withColumn("u_badpol", "double", minValue=0.0, maxValue=1.0, random=True)
    )
    df = spec.build()

    is_nw = "postcode_district rlike '^(M|BL|OL|WN)[0-9]'"

    # loss_cause — NW districts get ~3x escape-of-water (waterdamage) frequency.
    # Non-NW thresholds:  veh .45 | water .20 | windhail .20 | fire .15
    # NW thresholds:      veh .28 | water .48 | windhail .14 | fire .10  (~3x water)
    df = df.withColumn(
        "loss_cause",
        F.expr(f"""
            CASE WHEN {is_nw} THEN
                CASE
                    WHEN u_cause < 0.28 THEN 'vehcollision'
                    WHEN u_cause < 0.76 THEN 'waterdamage'
                    WHEN u_cause < 0.90 THEN 'windhail'
                    ELSE 'fire'
                END
            ELSE
                CASE
                    WHEN u_cause < 0.45 THEN 'vehcollision'
                    WHEN u_cause < 0.65 THEN 'waterdamage'
                    WHEN u_cause < 0.85 THEN 'windhail'
                    ELSE 'fire'
                END
            END
        """),
    )

    df = df.withColumn(
        "product",
        F.expr("CASE WHEN loss_cause = 'vehcollision' THEN 'motor' ELSE 'home' END"),
    )

    # report_channel
    df = df.withColumn(
        "report_channel",
        F.expr("""
            CASE
                WHEN u_channel < 0.45 THEN 'digital'
                WHEN u_channel < 0.80 THEN 'phone'
                ELSE 'broker_email'
            END
        """),
    )

    # claim_status
    df = df.withColumn(
        "claim_status",
        F.expr("""
            CASE
                WHEN u_status < 0.55 THEN 'closed'
                WHEN u_status < 0.92 THEN 'open'
                ELSE 'reopened'
            END
        """),
    )

    # total_incurred (£, whole pounds). Exactly 80% < £5k, long tail to £250k.
    df = df.withColumn(
        "total_incurred",
        F.expr("""
            CAST(round(
                CASE WHEN u_amount < 0.80
                    THEN 150 + 4850 * pow(u_amount / 0.80, 1.5)
                    ELSE 5000 + 245000 * pow((u_amount - 0.80) / 0.20, 3.0)
                END
            ) AS INT)
        """),
    )

    # Dates — rolling relative to the anchor.
    df = roll_dates(df, "days_ago_loss", "loss_date", anchor=anchor)
    a = _anchor_sql(anchor)
    # report_date = loss_date + lag, clamped to not exceed the anchor (today).
    df = df.withColumn(
        "report_date",
        F.expr(f"least(date_add(loss_date, report_lag_days), {a})"),
    )
    df = df.withColumn(
        "cda_batch_ts",
        F.expr("cast(report_date as timestamp) + make_interval(0,0,0,0, pmod(claim_seq, 12), 0, 0)"),
    )

    # policy_seq within the product's id range (guarantees FK + product match).
    df = df.withColumn(
        "policy_seq",
        F.expr(f"""
            CASE WHEN product = 'motor'
                THEN CAST(1 + floor(policy_pick * {N_MOTOR_POLICIES}) AS LONG)
                ELSE CAST({N_MOTOR_POLICIES} + 1 + floor(policy_pick * {N_HOME_POLICIES}) AS LONG)
            END
        """),
    )

    # ~2% malformed: ~1% bad policy_number (FK break -> quarantine in Phase 1).
    df = df.withColumn("is_bad_policy", F.expr("u_badpol < 0.01"))
    df = df.withColumn(
        "policy_number",
        F.expr("""
            CASE WHEN is_bad_policy THEN 'BAD-POLICY'
                 ELSE concat('BSE-P-', lpad(policy_seq, 7, '0'))
            END
        """),
    )

    df = df.withColumn("claim_public_id", F.expr("concat('cc:', claim_seq)"))
    df = df.withColumn(
        "claim_number",
        F.expr("concat('BSE-CC-', cast(year(loss_date) as string), '-', lpad(claim_seq, 6, '0'))"),
    )

    return df


def vivid_claim_row(spark, anchor=None):
    """The SACRED vivid claim cc:900001 — fixed, reproducible, exact."""
    a = _anchor_sql(anchor)
    return spark.sql(f"""
        SELECT
            cast('{VIVID_CLAIM_ID}' as string)              AS claim_public_id,
            900001L                                          AS claim_seq,
            concat('BSE-CC-', cast(year(date_sub({a},18)) as string), '-900001') AS claim_number,
            cast('{VIVID_POLICY_NUMBER}' as string)          AS policy_number,
            900001L                                          AS policy_seq,
            cast('motor' as string)                          AS product,
            cast('M1' as string)                             AS postcode_district,
            date_sub({a}, 18)                                AS loss_date,
            {a}                                              AS report_date,
            cast('phone' as string)                          AS report_channel,
            cast('vehcollision' as string)                   AS loss_cause,
            cast('open' as string)                           AS claim_status,
            8500                                             AS total_incurred,
            cast(date_sub({a},18) as timestamp) + make_interval(0,0,0,0,9,0,0) AS cda_batch_ts,
            false                                            AS is_bad_policy,
            0                                                AS report_lag_days
    """)


# --------------------------------------------------------------------------
# Child / related tables — derived from the claims DataFrame (FK-safe)
# --------------------------------------------------------------------------
def build_exposures(claims):
    """`bronze_gw_cc_exposure` (1 exposure per claim).

    Home escape-of-water is under-reserved: reserve_amount ≈ 0.72x of the
    outstanding it should carry -> "+28% under-reserving" story in Phase 3.
    """
    df = claims.withColumn(
        "paid_frac",
        F.expr("""
            CASE claim_status
                WHEN 'closed' THEN 0.95
                WHEN 'reopened' THEN 0.60
                ELSE 0.30
            END
        """),
    )
    df = df.withColumn("paid_amount", F.expr("CAST(round(total_incurred * paid_frac) AS INT)"))
    df = df.withColumn("correct_reserve", F.expr("greatest(total_incurred - paid_amount, 0)"))
    df = df.withColumn(
        "is_eow", F.expr("loss_cause = 'waterdamage' AND product = 'home'"))
    df = df.withColumn(
        "reserve_amount",
        F.expr("CAST(round(CASE WHEN is_eow THEN correct_reserve * 0.72 ELSE correct_reserve END) AS INT)"),
    )
    df = df.withColumn(
        "coverage_type",
        F.expr("""
            CASE loss_cause
                WHEN 'vehcollision' THEN 'motor_third_party'
                WHEN 'fire' THEN 'home_buildings'
                ELSE 'home_buildings'
            END
        """),
    )
    df = df.withColumn("exposure_type", F.lit("indemnity"))
    df = df.withColumn("exposure_public_id", F.expr("concat('ec:', claim_seq)"))
    return df.select(
        "exposure_public_id", "claim_public_id", "coverage_type",
        "exposure_type", "reserve_amount", "paid_amount",
    )


def build_incidents(claims):
    """`bronze_gw_cc_incident` (1 incident per claim) with templated text."""
    df = claims.withColumn(
        "incident_type",
        F.expr("""
            CASE loss_cause
                WHEN 'vehcollision' THEN 'vehicle_collision'
                WHEN 'waterdamage' THEN 'escape_of_water'
                WHEN 'windhail' THEN 'storm_damage'
                ELSE 'fire_damage'
            END
        """),
    )
    df = df.withColumn(
        "vehicle_or_property_ref",
        F.expr("""
            CASE WHEN product = 'motor'
                THEN concat(
                    chr(65 + cast(pmod(claim_seq, 26) as int)),
                    chr(65 + cast(pmod(claim_seq * 7, 26) as int)),
                    lpad(cast(pmod(claim_seq, 100) as string), 2, '0'), ' ',
                    chr(65 + cast(pmod(claim_seq * 3, 26) as int)),
                    chr(65 + cast(pmod(claim_seq * 11, 26) as int)),
                    chr(65 + cast(pmod(claim_seq * 13, 26) as int)))
                ELSE concat('PROP-', lpad(claim_seq, 7, '0'))
            END
        """),
    )
    df = df.withColumn("incident_public_id", F.expr("concat('in:', claim_seq)"))

    # description_text via a CASE over (loss_cause, desc_pick); templates inline.
    branches = []
    for cause, templates in DESC_TEMPLATES.items():
        n = len(templates)
        inner = " ".join(
            f"WHEN pmod(desc_pick, {n}) = {i} THEN '{t.replace(chr(39), chr(39)*2)}'"
            for i, t in enumerate(templates)
        )
        branches.append(f"WHEN loss_cause = '{cause}' THEN (CASE {inner} END)")
    desc_sql = "CASE " + " ".join(branches) + " END"
    df = df.withColumn("description_text", F.expr(desc_sql))

    return df.select(
        "incident_public_id", "claim_public_id", "incident_type",
        "vehicle_or_property_ref", "description_text",
    )


def build_contacts(claims):
    """`bronze_gw_cc_contact` (1 primary contact per claim)."""
    df = claims.withColumn(
        "contact_role",
        F.expr("""
            CASE
                WHEN u_role < 0.70 THEN 'claimant'
                WHEN u_role < 0.90 THEN 'third_party'
                ELSE 'witness'
            END
        """),
    )
    df = df.withColumn("contact_public_id", F.expr("concat('ct:', claim_seq, ':1')"))
    return df.select(
        "contact_public_id", "claim_public_id", "contact_role", "postcode_district",
    )


def build_fraud_signals(claims):
    """`bronze_fraud_signals_raw` — rule-seeded fraud_score.

    Elevated by: late report (lag > 21d, +20), high amount (>£20k, +20), and
    prior claims (+8 each). ~1% get an OUT-OF-RANGE score (quarantine demo).
    """
    df = claims.withColumn(
        "prior_claims_12m",
        F.expr("""
            CASE
                WHEN u_prior < 0.70 THEN 0
                WHEN u_prior < 0.88 THEN 1
                WHEN u_prior < 0.96 THEN 2
                ELSE 3
            END
        """),
    )
    df = df.withColumn("days_since_incident", F.col("report_lag_days"))
    df = df.withColumn(
        "raw_score",
        F.expr("""
            (u_fraud * 40)
            + CASE WHEN report_lag_days > 21 THEN 20 ELSE 0 END
            + CASE WHEN total_incurred > 20000 THEN 20 ELSE 0 END
            + (prior_claims_12m * 8)
        """),
    )
    # ~1% malformed: out-of-range fraud_score (negative or > 100).
    df = df.withColumn(
        "fraud_score",
        F.expr("""
            CASE
                WHEN u_sig < 0.005 THEN -5
                WHEN u_sig >= 0.995 THEN 150
                ELSE CAST(round(least(greatest(raw_score, 0), 100)) AS INT)
            END
        """),
    )
    df = df.withColumn("fraud_flag", F.expr("fraud_score > 70 AND fraud_score <= 100"))
    df = df.withColumn(
        "signal_source",
        F.expr("CASE WHEN u_sig < 0.6 THEN 'internal_rules' ELSE 'third_party_bureau' END"),
    )
    return df.select(
        "claim_public_id", "fraud_score", "fraud_flag", "prior_claims_12m",
        "days_since_incident", "signal_source",
    )


def vivid_children(spark, anchor=None):
    """Exposure / incident / contact / fraud rows for the vivid claim."""
    cid = VIVID_CLAIM_ID
    exposure = spark.sql(f"""
        SELECT 'ec:900001' AS exposure_public_id, '{cid}' AS claim_public_id,
               'motor_third_party' AS coverage_type, 'indemnity' AS exposure_type,
               6500 AS reserve_amount, 2000 AS paid_amount
    """)
    incident = spark.sql(f"""
        SELECT 'in:900001' AS incident_public_id, '{cid}' AS claim_public_id,
               'vehicle_collision' AS incident_type, 'VV19 BSE' AS vehicle_or_property_ref,
               'Collision with third-party vehicle at a roundabout.' AS description_text
    """)
    contact = spark.sql(f"""
        SELECT 'ct:900001:1' AS contact_public_id, '{cid}' AS claim_public_id,
               'claimant' AS contact_role, 'M1' AS postcode_district
    """)
    fraud = spark.sql(f"""
        SELECT '{cid}' AS claim_public_id, 74 AS fraud_score, true AS fraud_flag,
               2 AS prior_claims_12m, 18 AS days_since_incident,
               'internal_rules' AS signal_source
    """)
    return exposure, incident, contact, fraud


# --------------------------------------------------------------------------
# Policy / handler / weather tables
# --------------------------------------------------------------------------
def build_policies(spark, claims, anchor=None):
    """`bronze_gw_pc_policy` — derived from referenced (valid) policies + vivid.

    Only non-malformed claims contribute policy ids, so every policy here is
    real; malformed claims dangle intentionally for the quarantine demo.
    """
    a = _anchor_sql(anchor)
    base = (
        claims.filter("NOT is_bad_policy")
        .select("policy_seq", "policy_number", "product")
        .distinct()
    )
    # Append the vivid claim's policy.
    vivid_pol = spark.sql(f"""
        SELECT 900001L AS policy_seq, '{VIVID_POLICY_NUMBER}' AS policy_number,
               'motor' AS product
    """)
    base = base.unionByName(vivid_pol)

    df = roll_dates(
        base.withColumn("eff_days_ago", F.expr("365 + pmod(policy_seq, 1460)")),
        "eff_days_ago", "effective_date", anchor=anchor,
    )
    df = df.withColumn("expiry_date", F.expr("date_add(effective_date, 365)"))
    df = df.withColumn(
        "sum_insured",
        F.expr("""
            CASE WHEN product = 'motor'
                THEN CAST(5000 + pmod(policy_seq * 37, 45000) AS INT)
                ELSE CAST(150000 + pmod(policy_seq * 53, 600000) AS INT)
            END
        """),
    )
    df = df.withColumn(
        "annual_premium",
        F.expr("""
            CASE WHEN product = 'motor'
                THEN CAST(round(sum_insured * 0.045) AS INT)
                ELSE CAST(round(sum_insured * 0.004) AS INT)
            END
        """),
    )
    return df.select(
        "policy_number", "product", "effective_date", "expiry_date",
        "sum_insured", "annual_premium",
    )


def build_handlers(spark, anchor=None):
    """`ref_handlers` — ~80 synthetic claim handlers."""
    import dbldatagen as dg

    spec = (
        dg.DataGenerator(spark, name="handlers", rows=N_HANDLERS,
                         partitions=1, randomSeed=SEED)
        .withColumn("h_seq", "int", minValue=1, maxValue=N_HANDLERS,
                    uniqueValues=N_HANDLERS, random=False)
        .withColumn("fn_idx", "int", minValue=0, maxValue=len(_FIRST_NAMES) - 1, random=True)
        .withColumn("ln_idx", "int", minValue=0, maxValue=len(_LAST_NAMES) - 1, random=True)
        .withColumn("grade", "string",
                    values=["junior", "senior", "specialist"],
                    weights=[5, 3, 2], random=True)
        .withColumn("team", "string",
                    values=["motor_fast_track", "motor_complex", "home_property", "siu"],
                    weights=[4, 3, 4, 1], random=True)
        .withColumn("bu", "string",
                    values=["uk_personal", "uk_commercial"], weights=[7, 3], random=True)
        .withColumn("tenure_days_ago", "int", minValue=90, maxValue=3000, random=True)
    )
    df = spec.build()

    fn = F.array(*[F.lit(x) for x in _FIRST_NAMES])
    ln = F.array(*[F.lit(x) for x in _LAST_NAMES])
    df = df.withColumn("handler_id", F.expr("concat('H', lpad(h_seq, 4, '0'))"))
    df = df.withColumn(
        "handler_name",
        F.concat(fn.getItem(F.col("fn_idx")), F.lit(" "), ln.getItem(F.col("ln_idx"))),
    )
    df = roll_dates(df, "tenure_days_ago", "start_date", anchor=anchor)
    return df.select("handler_id", "handler_name", "grade", "team", "bu", "start_date")


def build_weather(spark, anchor=None):
    """`bronze_weather_raw` — one row per postcode district.

    NW districts carry elevated flood risk (consistent with the EoW skew).
    Scores are deterministic functions of the district string.
    """
    rows = ", ".join(f"('{d}')" for d, _ in DISTRICTS)
    base = spark.sql(f"SELECT col1 AS postcode_district FROM VALUES {rows} AS t(col1)")
    is_nw = "postcode_district rlike '^(M|BL|OL|WN)[0-9]'"
    a = _anchor_sql(anchor)
    df = base.withColumn(
        "flood_risk_score",
        F.expr(f"CASE WHEN {is_nw} THEN greatest(pmod(crc32(postcode_district), 5) + 6, 6) "
               f"ELSE pmod(crc32(postcode_district), 7) END"),
    )
    df = df.withColumn(
        "wind_risk_score", F.expr("pmod(crc32(concat(postcode_district, 'w')), 11)"))
    df = df.withColumn(
        "freeze_risk_score", F.expr("pmod(crc32(concat(postcode_district, 'f')), 11)"))
    df = df.withColumn("data_vintage", F.expr(f"date_sub({a}, 7)"))
    return df.select(
        "postcode_district", "flood_risk_score", "wind_risk_score",
        "freeze_risk_score", "data_vintage",
    )


# --------------------------------------------------------------------------
# Orchestration
# --------------------------------------------------------------------------
CLAIM_PUBLIC_COLS = [
    "claim_public_id", "claim_number", "policy_number", "loss_date",
    "report_date", "report_channel", "loss_cause", "claim_status",
    "total_incurred", "cda_batch_ts",
]

# table -> layer, for UC tagging
TABLE_LAYERS = {
    "bronze_gw_cc_claim": "bronze",
    "bronze_gw_cc_exposure": "bronze",
    "bronze_gw_cc_incident": "bronze",
    "bronze_gw_cc_contact": "bronze",
    "bronze_gw_pc_policy": "bronze",
    "bronze_fraud_signals_raw": "bronze",
    "bronze_weather_raw": "bronze",
    "ref_handlers": "ref",
    "ref_weather_index": "ref",
}


def write_and_tag(spark, df, catalog, schema, table, layer):
    """Overwrite a Delta table (idempotent) and apply UC tags."""
    fqn = f"`{catalog}`.`{schema}`.`{table}`"
    (df.write.format("delta").mode("overwrite")
       .option("overwriteSchema", "true").saveAsTable(fqn))
    spark.sql(
        f"ALTER TABLE {fqn} SET TAGS "
        f"('project' = 'claims_workbench', 'layer' = '{layer}', 'owner' = 'wryszka')"
    )
    return spark.table(fqn).count()


def generate_all(spark, catalog, schema, anchor=None):
    """Build, persist (overwrite) and tag every Phase 0 table.

    Returns a dict of {table_name: row_count}.
    """
    counts = {}

    # --- claims (bulk + vivid) ---
    # The bulk frame carries helper columns (u_*, policy_seq, is_bad_policy, ...)
    # that the child builders consume; the vivid row carries only the public
    # schema + the few helper cols the policy builder needs. Keep them separate
    # and union just the public projection for the persisted claim table.
    bulk = build_claims(spark, anchor=anchor)
    bulk.cache()
    vivid = vivid_claim_row(spark, anchor=anchor)

    claims_public = (
        bulk.select(*CLAIM_PUBLIC_COLS)
            .unionByName(vivid.select(*CLAIM_PUBLIC_COLS))
    )
    counts["bronze_gw_cc_claim"] = write_and_tag(
        spark, claims_public, catalog, schema, "bronze_gw_cc_claim", "bronze")

    # --- child tables (bulk derived + vivid rows) ---
    v_exp, v_inc, v_con, v_fraud = vivid_children(spark, anchor=anchor)

    # vivid carries no u_*/report_lag helper cols, so derive children from the
    # bulk frame only, then union the explicit vivid child rows.
    exposures = build_exposures(bulk).unionByName(v_exp)
    counts["bronze_gw_cc_exposure"] = write_and_tag(
        spark, exposures, catalog, schema, "bronze_gw_cc_exposure", "bronze")

    incidents = build_incidents(bulk).unionByName(v_inc)
    counts["bronze_gw_cc_incident"] = write_and_tag(
        spark, incidents, catalog, schema, "bronze_gw_cc_incident", "bronze")

    contacts = build_contacts(bulk).unionByName(v_con)
    counts["bronze_gw_cc_contact"] = write_and_tag(
        spark, contacts, catalog, schema, "bronze_gw_cc_contact", "bronze")

    fraud = build_fraud_signals(bulk).unionByName(v_fraud)
    counts["bronze_fraud_signals_raw"] = write_and_tag(
        spark, fraud, catalog, schema, "bronze_fraud_signals_raw", "bronze")

    # --- policy / handlers / weather ---
    # Derive policies from the bulk frame only; the vivid policy is appended
    # explicitly inside build_policies (avoids a double-count).
    policies = build_policies(spark, bulk, anchor=anchor)
    counts["bronze_gw_pc_policy"] = write_and_tag(
        spark, policies, catalog, schema, "bronze_gw_pc_policy", "bronze")

    handlers = build_handlers(spark, anchor=anchor)
    counts["ref_handlers"] = write_and_tag(
        spark, handlers, catalog, schema, "ref_handlers", "ref")

    weather = build_weather(spark, anchor=anchor)
    counts["bronze_weather_raw"] = write_and_tag(
        spark, weather, catalog, schema, "bronze_weather_raw", "bronze")

    # ref_weather_index — materialised copy of the weather feed for joins.
    counts["ref_weather_index"] = write_and_tag(
        spark, weather, catalog, schema, "ref_weather_index", "ref")

    bulk.unpersist()
    return counts
