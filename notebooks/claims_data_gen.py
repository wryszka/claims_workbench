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
  * Vivid claims:    cc:900001 (escalated hero) and cc:900002 (auto-close hero)
                     are SACRED — fixed attributes, survive every reset, appended
                     explicitly (never randomly generated).

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

VIVID_CLAIM_ID = "cc:900001"       # SACRED — the escalated hero (fraud 74, REFER SIU)
VIVID_POLICY_NUMBER = "BSE-P-9009001"
VIVID_CLAIM_ID_2 = "cc:900002"     # SACRED — the auto-close hero (clean, pay_direct)
VIVID_POLICY_NUMBER_2 = "BSE-P-9009002"
VIVID_CLAIM_ID_3 = "cc:900003"     # SACRED — the discrepancy hero (Phase 12): reported tiny,
VIVID_POLICY_NUMBER_3 = "BSE-P-9009003"   # but the photo shows SEVERE damage -> rule R7 fires

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
    # Cast the offset to INT — date_sub() rejects BIGINT, and some offset
    # expressions derive from LONG columns (e.g. policy_seq).
    return df.withColumn(out_col, F.expr(f"date_sub({anchor_sql}, CAST({days_ago_col} AS INT))"))


def _anchor_sql(anchor=None):
    return "current_date()" if anchor is None else f"to_date('{anchor}')"


# --------------------------------------------------------------------------
# Core claims table (dbldatagen for primitives, Spark expr for business logic)
# --------------------------------------------------------------------------
def build_claims(spark, anchor=None):
    """Return the bulk `landing_gw_cc_claim` DataFrame (vivid claim added later).

    Carries a few helper columns (policy_seq, product, postcode_district,
    report_lag_days, is_bad_*) consumed by the child-table builders. Callers
    should select the public schema before persisting `landing_gw_cc_claim`.
    """
    import dbldatagen as dg

    district_vals = [d for d, _ in DISTRICTS]
    district_wts = [w for _, w in DISTRICTS]

    spec = (
        # randomSeedMethod="hash_fieldname" seeds each column independently
        # (reproducible but decorrelated). Without it, a set randomSeed uses the
        # "fixed" method -> every column shares one stream, so e.g. u_cause ends
        # up correlated with postcode_district. We need independent draws.
        dg.DataGenerator(spark, name="cc_claim", rows=N_CLAIMS, partitions=8,
                         randomSeed=SEED, randomSeedMethod="hash_fieldname")
        .withColumn("claim_seq", "long",
                    minValue=CLAIM_SEQ_START, maxValue=CLAIM_SEQ_START + N_CLAIMS - 1,
                    uniqueValues=N_CLAIMS, random=False)
        .withColumn("postcode_district", "string",
                    values=district_vals, weights=district_wts, random=True)
        .withColumn("u_amount", "double", minValue=0.0, maxValue=1.0, continuous=True, random=True)
        .withColumn("u_cause", "double", minValue=0.0, maxValue=1.0, continuous=True, random=True)
        .withColumn("u_channel", "double", minValue=0.0, maxValue=1.0, continuous=True, random=True)
        .withColumn("u_status", "double", minValue=0.0, maxValue=1.0, continuous=True, random=True)
        .withColumn("days_ago_loss", "int", minValue=1, maxValue=1080, random=True)
        .withColumn("report_lag_days", "int", minValue=0, maxValue=40, random=True)
        .withColumn("policy_pick", "double", minValue=0.0, maxValue=1.0, continuous=True, random=True)
        .withColumn("u_prior", "double", minValue=0.0, maxValue=1.0, continuous=True, random=True)
        .withColumn("u_fraud", "double", minValue=0.0, maxValue=1.0, continuous=True, random=True)
        .withColumn("u_role", "double", minValue=0.0, maxValue=1.0, continuous=True, random=True)
        .withColumn("u_paid", "double", minValue=0.0, maxValue=1.0, continuous=True, random=True)
        .withColumn("u_sig", "double", minValue=0.0, maxValue=1.0, continuous=True, random=True)
        .withColumn("desc_pick", "int", minValue=0, maxValue=7, random=True)
        .withColumn("u_badpol", "double", minValue=0.0, maxValue=1.0, continuous=True, random=True)
        .withColumn("u_badcause", "double", minValue=0.0, maxValue=1.0, continuous=True, random=True)
    )
    df = spec.build()

    is_nw = "postcode_district rlike '^(M|BL|OL|WN)[0-9]'"

    # loss_cause_clean — the valid typecode used to derive child entities
    # (product, incident_type, coverage, under-reserving). NW districts get ~3x
    # escape-of-water (waterdamage) frequency. NW water band width 0.48,
    # non-NW 0.16 -> ~3x (verified ~0.48 vs ~0.16).
    df = df.withColumn(
        "loss_cause_clean",
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
                    WHEN u_cause < 0.61 THEN 'waterdamage'
                    WHEN u_cause < 0.85 THEN 'windhail'
                    ELSE 'fire'
                END
            END
        """),
    )

    # loss_cause as it lands in the raw claim record: ~1% carry a garbage
    # typecode ('unknown') to simulate a dirty Guidewire CDA drop. These survive
    # into the landing zone and are quarantined by the Phase 1 bronze DLT
    # (expect_or_drop on valid_loss_cause). Child tables key off the *clean*
    # cause, so only the claim header is corrupted.
    df = df.withColumn(
        "loss_cause",
        F.expr("CASE WHEN u_badcause < 0.01 THEN 'unknown' ELSE loss_cause_clean END"),
    )

    df = df.withColumn(
        "product",
        F.expr("CASE WHEN loss_cause_clean = 'vehcollision' THEN 'motor' ELSE 'home' END"),
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


def vivid_claim_row_2(spark, anchor=None):
    """The SACRED auto-close hero cc:900002 — small, clean home claim that the
    workflow straight-through-processes: home escape-of-water, GBP 420, reported
    next-day, low fraud, no prior claims -> triage pay_direct -> auto-closed & paid."""
    a = _anchor_sql(anchor)
    return spark.sql(f"""
        SELECT
            cast('{VIVID_CLAIM_ID_2}' as string)             AS claim_public_id,
            900002L                                          AS claim_seq,
            concat('BSE-CC-', cast(year(date_sub({a},1)) as string), '-900002') AS claim_number,
            cast('{VIVID_POLICY_NUMBER_2}' as string)        AS policy_number,
            900002L                                          AS policy_seq,
            cast('home' as string)                           AS product,
            cast('RG1' as string)                            AS postcode_district,
            date_sub({a}, 1)                                 AS loss_date,
            {a}                                              AS report_date,
            cast('digital' as string)                        AS report_channel,
            cast('waterdamage' as string)                    AS loss_cause,
            cast('open' as string)                           AS claim_status,
            420                                              AS total_incurred,
            cast(date_sub({a},1) as timestamp) + make_interval(0,0,0,0,9,0,0) AS cda_batch_ts,
            false                                            AS is_bad_policy,
            0                                                AS report_lag_days
    """)


def vivid_claim_row_3(spark, anchor=None):
    """The SACRED discrepancy hero cc:900003 (Phase 12) — a small, clean-looking MOTOR
    claim (GBP 600, low fraud, no prior, complete, reported next-day) that everything says
    auto-close... except the damage photo shows SEVERE damage. Rule R7 (image severity vs
    reported amount) catches the discrepancy and escalates. The Smart-Claims lightbulb."""
    a = _anchor_sql(anchor)
    return spark.sql(f"""
        SELECT
            cast('{VIVID_CLAIM_ID_3}' as string)             AS claim_public_id,
            900003L                                          AS claim_seq,
            concat('BSE-CC-', cast(year(date_sub({a},1)) as string), '-900003') AS claim_number,
            cast('{VIVID_POLICY_NUMBER_3}' as string)        AS policy_number,
            900003L                                          AS policy_seq,
            cast('motor' as string)                          AS product,
            cast('M20' as string)                            AS postcode_district,
            date_sub({a}, 1)                                 AS loss_date,
            {a}                                              AS report_date,
            cast('digital' as string)                        AS report_channel,
            cast('vehcollision' as string)                   AS loss_cause,
            cast('open' as string)                           AS claim_status,
            600                                              AS total_incurred,
            cast(date_sub({a},1) as timestamp) + make_interval(0,0,0,0,9,0,0) AS cda_batch_ts,
            false                                            AS is_bad_policy,
            0                                                AS report_lag_days
    """)


# --------------------------------------------------------------------------
# Child / related tables — derived from the claims DataFrame (FK-safe)
# --------------------------------------------------------------------------
def build_exposures(claims):
    """`landing_gw_cc_exposure` (1 exposure per claim).

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
        "is_eow", F.expr("loss_cause_clean = 'waterdamage' AND product = 'home'"))
    df = df.withColumn(
        "reserve_amount",
        F.expr("CAST(round(CASE WHEN is_eow THEN correct_reserve * 0.72 ELSE correct_reserve END) AS INT)"),
    )
    df = df.withColumn(
        "coverage_type",
        F.expr("""
            CASE loss_cause_clean
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
    """`landing_gw_cc_incident` (1 incident per claim) with templated text."""
    df = claims.withColumn(
        "incident_type",
        F.expr("""
            CASE loss_cause_clean
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

    # description_text via a CASE over (loss_cause_clean, desc_pick); inline.
    branches = []
    for cause, templates in DESC_TEMPLATES.items():
        n = len(templates)
        inner = " ".join(
            f"WHEN pmod(desc_pick, {n}) = {i} THEN '{t.replace(chr(39), chr(39)*2)}'"
            for i, t in enumerate(templates)
        )
        branches.append(f"WHEN loss_cause_clean = '{cause}' THEN (CASE {inner} END)")
    desc_sql = "CASE " + " ".join(branches) + " END"
    df = df.withColumn("description_text", F.expr(desc_sql))

    return df.select(
        "incident_public_id", "claim_public_id", "incident_type",
        "vehicle_or_property_ref", "description_text",
    )


def build_contacts(claims):
    """`landing_gw_cc_contact` (1 primary contact per claim)."""
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
    """`landing_fraud_signals` — rule-seeded fraud_score.

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


def vivid_children_2(spark, anchor=None):
    """Exposure / incident / contact / fraud rows for the auto-close hero cc:900002."""
    cid = VIVID_CLAIM_ID_2
    exposure = spark.sql(f"""
        SELECT 'ec:900002' AS exposure_public_id, '{cid}' AS claim_public_id,
               'home_buildings' AS coverage_type, 'indemnity' AS exposure_type,
               420 AS reserve_amount, 0 AS paid_amount
    """)
    incident = spark.sql(f"""
        SELECT 'in:900002' AS incident_public_id, '{cid}' AS claim_public_id,
               'escape_of_water' AS incident_type, 'RG1 2024 detached' AS vehicle_or_property_ref,
               'Minor escape of water from a kitchen appliance; limited floor damage.' AS description_text
    """)
    contact = spark.sql(f"""
        SELECT 'ct:900002:1' AS contact_public_id, '{cid}' AS claim_public_id,
               'claimant' AS contact_role, 'RG1' AS postcode_district
    """)
    fraud = spark.sql(f"""
        SELECT '{cid}' AS claim_public_id, 8 AS fraud_score, false AS fraud_flag,
               0 AS prior_claims_12m, 1 AS days_since_incident,
               'internal_rules' AS signal_source
    """)
    return exposure, incident, contact, fraud


def vivid_children_3(spark, anchor=None):
    """Children for the discrepancy hero cc:900003 — clean signals; the only red flag is the
    photo (seeded in claim_image_severity). Reported as a minor bump; image shows severe."""
    cid = VIVID_CLAIM_ID_3
    exposure = spark.sql(f"""
        SELECT 'ec:900003' AS exposure_public_id, '{cid}' AS claim_public_id,
               'motor_third_party' AS coverage_type, 'indemnity' AS exposure_type,
               600 AS reserve_amount, 0 AS paid_amount
    """)
    incident = spark.sql(f"""
        SELECT 'in:900003' AS incident_public_id, '{cid}' AS claim_public_id,
               'vehicle_collision' AS incident_type, 'MM20 BSE' AS vehicle_or_property_ref,
               'Reported as a minor parking knock; limited cosmetic damage claimed.' AS description_text
    """)
    contact = spark.sql(f"""
        SELECT 'ct:900003:1' AS contact_public_id, '{cid}' AS claim_public_id,
               'claimant' AS contact_role, 'M20' AS postcode_district
    """)
    fraud = spark.sql(f"""
        SELECT '{cid}' AS claim_public_id, 12 AS fraud_score, false AS fraud_flag,
               0 AS prior_claims_12m, 1 AS days_since_incident,
               'internal_rules' AS signal_source
    """)
    return exposure, incident, contact, fraud


# --------------------------------------------------------------------------
# Telematics (motor) — light synthetic feed aligned to the Smart Claims
# accelerator's `telematic` entity (vehicle_id, latitude, longitude,
# event_timestamp, speed), extended with posted_speed_limit + harsh_braking.
# One row per MOTOR claim; feeds rule R6 (speed-vs-limit). Home claims: none.
# --------------------------------------------------------------------------
def build_telematics(spark, claims, anchor=None):
    a = _anchor_sql(anchor)
    motor = claims.filter("product = 'motor'")
    df = (motor.select("claim_public_id", "claim_seq", "postcode_district", "loss_date")
          # posted limit cycles through UK limits deterministically by claim_seq
          .withColumn("posted_speed_limit",
                      F.expr("element_at(array(30,40,50,60,70), CAST(pmod(claim_seq,5) AS INT)+1)"))
          # most drivers are at/under; ~12% materially over (deterministic) -> R6 fires
          .withColumn("_over", F.expr("pmod(crc32(concat(claim_public_id,'|tel')),100) < 12"))
          .withColumn("speed_at_incident",
                      F.expr("CAST(CASE WHEN _over THEN posted_speed_limit + 18 + pmod(crc32(claim_public_id),15) "
                             "ELSE greatest(posted_speed_limit - pmod(crc32(claim_public_id),12), 8) END AS INT)"))
          .withColumn("harsh_braking", F.expr("_over OR pmod(crc32(concat(claim_public_id,'|hb')),100) < 20"))
          .withColumn("vehicle_id", F.expr("concat('VH-', lpad(CAST(pmod(claim_seq,99999) AS string),5,'0'))"))
          .withColumn("latitude", F.expr("round(51.5 + (pmod(crc32(claim_public_id),1000)/1000.0 - 0.5), 5)"))
          .withColumn("longitude", F.expr("round(-1.5 + (pmod(crc32(concat(claim_public_id,'|lon')),1000)/1000.0 - 0.5), 5)"))
          .withColumn("event_timestamp", F.expr(f"cast(loss_date as timestamp) + make_interval(0,0,0,0,8,30,0)"))
          .select("claim_public_id", "vehicle_id", "latitude", "longitude", "event_timestamp",
                  "speed_at_incident", "posted_speed_limit", "harsh_braking"))
    # Vivid telematics: cc:900001 speeding (R6 fires); cc:900003 within limit (only R7 fires).
    vivid = spark.sql(f"""
        SELECT '{VIVID_CLAIM_ID}' claim_public_id, 'VH-90001' vehicle_id, 53.4808 latitude, -2.2426 longitude,
               cast(date_sub({a},18) as timestamp) + make_interval(0,0,0,0,8,30,0) event_timestamp,
               95 speed_at_incident, 50 posted_speed_limit, true harsh_braking
        UNION ALL
        SELECT '{VIVID_CLAIM_ID_3}', 'VH-90003', 53.4084, -2.2342,
               cast(date_sub({a},1) as timestamp) + make_interval(0,0,0,0,8,30,0),
               28, 30, false
    """)
    return df.unionByName(vivid)


# --------------------------------------------------------------------------
# Policy / handler / weather tables
# --------------------------------------------------------------------------
def build_policies(spark, claims, anchor=None):
    """`landing_gw_pc_policy` — derived from referenced (valid) policies + vivid.

    Only non-malformed claims contribute policy ids, so every policy here is
    real; malformed claims dangle intentionally for the quarantine demo.
    """
    a = _anchor_sql(anchor)
    base = (
        claims.filter("NOT is_bad_policy")
        .select("policy_seq", "policy_number", "product")
        .distinct()
    )
    # Append both vivid claims' policies (cc:900001 motor, cc:900002 home).
    vivid_pol = spark.sql(f"""
        SELECT 900001L AS policy_seq, '{VIVID_POLICY_NUMBER}' AS policy_number, 'motor' AS product
        UNION ALL
        SELECT 900002L AS policy_seq, '{VIVID_POLICY_NUMBER_2}' AS policy_number, 'home' AS product
        UNION ALL
        SELECT 900003L AS policy_seq, '{VIVID_POLICY_NUMBER_3}' AS policy_number, 'motor' AS product
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
        dg.DataGenerator(spark, name="handlers", rows=N_HANDLERS, partitions=1,
                         randomSeed=SEED, randomSeedMethod="hash_fieldname")
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
    """`landing_weather` — one row per postcode district.

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

# table -> layer, for UC tagging. Phase 0 produces the LANDING ZONE (a simulated
# Guidewire ClaimCenter CDA drop). The Phase 1 bronze DLT pipeline reads these
# landing_* tables and produces governed bronze_* tables.
TABLE_LAYERS = {
    "landing_gw_cc_claim": "landing",
    "landing_gw_cc_exposure": "landing",
    "landing_gw_cc_incident": "landing",
    "landing_gw_cc_contact": "landing",
    "landing_gw_pc_policy": "landing",
    "landing_fraud_signals": "landing",
    "landing_weather": "landing",
    "ref_handlers": "ref",
    "ref_weather_index": "ref",
}


def set_tags_safe(spark, target_sql, tags):
    """Apply UC tags one key at a time, skipping any rejected by a governed tag
    policy on the workspace (some workspaces restrict the allowed values for a
    tag key). Returns (applied, skipped) dicts.

    `target_sql` is the ALTER target, e.g. "TABLE `cat`.`sch`.`tbl`" or
    "SCHEMA `cat`.`sch`". Keeps the demo portable: the full tag scheme applies
    on ungoverned workspaces; governed keys are simply logged and skipped.
    """
    applied, skipped = {}, {}
    for k, v in tags.items():
        try:
            spark.sql(f"ALTER {target_sql} SET TAGS ('{k}' = '{v}')")
            applied[k] = v
        except Exception as e:  # noqa: BLE001 — narrow on governed-tag message
            msg = str(e)
            if any(s in msg for s in
                   ("not an allowed value", "tag policy", "INVALID_PARAMETER_VALUE")):
                skipped[k] = v
                print(f"[tags] skipped governed tag {k}={v} on {target_sql}: "
                      f"{msg.splitlines()[0][:160]}")
            else:
                raise
    return applied, skipped


def write_and_tag(spark, df, catalog, schema, table, layer):
    """Overwrite a Delta table (idempotent) and apply UC tags."""
    fqn = f"`{catalog}`.`{schema}`.`{table}`"
    (df.write.format("delta").mode("overwrite")
       .option("overwriteSchema", "true").saveAsTable(fqn))
    set_tags_safe(spark, f"TABLE {fqn}", {
        "project": "claims_workbench", "layer": layer, "owner": "wryszka",
    })
    return spark.table(fqn).count()


def generate_all(spark, catalog, schema, anchor=None):
    """Build, persist (overwrite) and tag every Phase 0 table.

    Returns a dict of {table_name: row_count}.
    """
    counts = {}

    # --- claims (bulk + vivid) ---
    # The bulk frame carries helper columns (u_*, policy_seq, is_bad_policy, ...)
    # that the child builders consume; the vivid row carries only the public
    # schema + the few helper cols the policy builder needs.
    #
    # dbldatagen's random columns are NOT stable across separate Spark actions
    # on serverless (and .cache()/.persist() is unsupported there). Deriving the
    # child tables from a single lazy `bulk` frame would therefore decorrelate
    # them — e.g. claim.loss_cause vs contact.postcode_district would be
    # different random draws for the same claim. So materialise the full
    # enriched frame ONCE to a scratch Delta table and read it back; every child
    # table then derives from identical, stable rows (correct cross-table
    # relationships + FK integrity). The scratch table is dropped at the end.
    scratch = f"`{catalog}`.`{schema}`._tmp_bulk_claims"
    (build_claims(spark, anchor=anchor).write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true").saveAsTable(scratch))
    bulk = spark.table(scratch)
    vivid = vivid_claim_row(spark, anchor=anchor)
    vivid2 = vivid_claim_row_2(spark, anchor=anchor)
    vivid3 = vivid_claim_row_3(spark, anchor=anchor)

    claims_public = (
        bulk.select(*CLAIM_PUBLIC_COLS)
            .unionByName(vivid.select(*CLAIM_PUBLIC_COLS))
            .unionByName(vivid2.select(*CLAIM_PUBLIC_COLS))
            .unionByName(vivid3.select(*CLAIM_PUBLIC_COLS))
    )
    counts["landing_gw_cc_claim"] = write_and_tag(
        spark, claims_public, catalog, schema, "landing_gw_cc_claim", "landing")

    # --- child tables (bulk derived + vivid rows) ---
    v_exp, v_inc, v_con, v_fraud = vivid_children(spark, anchor=anchor)
    v2_exp, v2_inc, v2_con, v2_fraud = vivid_children_2(spark, anchor=anchor)
    v3_exp, v3_inc, v3_con, v3_fraud = vivid_children_3(spark, anchor=anchor)

    # vivid claims carry no u_*/report_lag helper cols, so derive children from
    # the bulk frame only, then union the explicit vivid child rows.
    exposures = build_exposures(bulk).unionByName(v_exp).unionByName(v2_exp).unionByName(v3_exp)
    counts["landing_gw_cc_exposure"] = write_and_tag(
        spark, exposures, catalog, schema, "landing_gw_cc_exposure", "landing")

    incidents = build_incidents(bulk).unionByName(v_inc).unionByName(v2_inc).unionByName(v3_inc)
    counts["landing_gw_cc_incident"] = write_and_tag(
        spark, incidents, catalog, schema, "landing_gw_cc_incident", "landing")

    contacts = build_contacts(bulk).unionByName(v_con).unionByName(v2_con).unionByName(v3_con)
    counts["landing_gw_cc_contact"] = write_and_tag(
        spark, contacts, catalog, schema, "landing_gw_cc_contact", "landing")

    fraud = build_fraud_signals(bulk).unionByName(v_fraud).unionByName(v2_fraud).unionByName(v3_fraud)
    counts["landing_fraud_signals"] = write_and_tag(
        spark, fraud, catalog, schema, "landing_fraud_signals", "landing")

    # --- telematics (motor only) ---
    # build_telematics filters motor from the BULK frame and unions the explicit vivid
    # telematics for cc:900001 (speeding -> R6) and cc:900003 (within limit -> only R7).
    telematics = build_telematics(spark, bulk, anchor=anchor)
    counts["landing_telematics"] = write_and_tag(
        spark, telematics, catalog, schema, "landing_telematics", "landing")

    # --- policy / handlers / weather ---
    # Derive policies from the bulk frame only; the vivid policy is appended
    # explicitly inside build_policies (avoids a double-count).
    policies = build_policies(spark, bulk, anchor=anchor)
    counts["landing_gw_pc_policy"] = write_and_tag(
        spark, policies, catalog, schema, "landing_gw_pc_policy", "landing")

    handlers = build_handlers(spark, anchor=anchor)
    counts["ref_handlers"] = write_and_tag(
        spark, handlers, catalog, schema, "ref_handlers", "ref")

    weather = build_weather(spark, anchor=anchor)
    counts["landing_weather"] = write_and_tag(
        spark, weather, catalog, schema, "landing_weather", "landing")

    # ref_weather_index — materialised copy of the weather feed for joins.
    counts["ref_weather_index"] = write_and_tag(
        spark, weather, catalog, schema, "ref_weather_index", "ref")

    # Drop the scratch materialisation — it is an internal staging table only.
    spark.sql(f"DROP TABLE IF EXISTS {scratch}")

    return counts
