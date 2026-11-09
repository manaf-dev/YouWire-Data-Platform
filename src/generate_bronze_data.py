"""Generate the synthetic YouWire Bronze dataset.

Deterministic (seeded) so every run produces the same files and row counts:

    <out>/customers/customers.csv          750 rows
    <out>/fx_rates/fx_rates.csv            6,579 rows  (9 currencies x 731 days)
    <out>/fee_schedule/fee_schedule.csv    16 rows     (2 segments x 4 methods x 2 corridor types)
    <out>/payments/payments_<period>.json  48,788 rows across 4 half-year files

The layout mirrors the Lakehouse, so the output folder can be uploaded as-is to
``Files/bronze`` in ``lh_youwire``. Inside a Fabric notebook you can also write
straight to OneLake:  ``--out /lakehouse/default/Files/bronze``.

Some string columns are deliberately "messy" (stray whitespace, mixed case) so
the Silver notebook's cleansing steps have real work to do.

Usage:
    python src/generate_bronze_data.py [--out data/bronze] [--seed 42]
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import random
from datetime import date, datetime, timedelta
from pathlib import Path

N_CUSTOMERS = 750
N_PAYMENTS = 48_788
START_DATE = date(2024, 1, 1)
END_DATE = date(2025, 12, 31)  # 731 days (2024 is a leap year)

# country_code -> (country_name, currency, customer share, cross-border failure rate, settlement median hrs)
COUNTRIES = {
    "NG": ("Nigeria", "NGN", 18, 0.090, 36),
    "GB": ("United Kingdom", "GBP", 15, 0.020, 10),
    "US": ("United States", "USD", 15, 0.020, 10),
    "GH": ("Ghana", "GHS", 10, 0.070, 30),
    "KE": ("Kenya", "KES", 10, 0.050, 24),
    "ZA": ("South Africa", "ZAR", 8, 0.035, 18),
    "DE": ("Germany", "EUR", 8, 0.020, 12),
    "FR": ("France", "EUR", 6, 0.020, 12),
    "AE": ("United Arab Emirates", "AED", 5, 0.025, 14),
    "CN": ("China", "CNY", 5, 0.030, 20),
}

# currency -> (starting USD rate, daily drift, daily volatility)
FX = {
    "USD": (1.0, 0.0, 0.0),
    "EUR": (1.09, 0.0, 0.004),
    "GBP": (1.27, 0.0, 0.004),
    "CNY": (0.141, 0.0, 0.0015),
    "AED": (0.2723, 0.0, 0.0),  # USD peg
    "ZAR": (0.054, 0.0, 0.007),
    "KES": (0.0064, 0.0001, 0.005),
    "GHS": (0.084, -0.0005, 0.006),
    "NGN": (0.00125, -0.0008, 0.008),
}

SEGMENTS = {"SME": 0.75, "Enterprise": 0.25}
PAYMENT_METHODS = {"bank_transfer": 0.45, "wallet": 0.20, "card": 0.20, "mobile_money": 0.15}
INDUSTRIES = [
    "E-commerce", "Logistics", "Manufacturing", "Software", "Travel",
    "Healthcare", "Education", "Agriculture", "Media", "Professional Services",
]
KYC_STATUSES = {"approved": 0.85, "pending": 0.10, "rejected": 0.05}

# fee % of amount_usd, by (segment, method) -> (domestic, cross_border)
FEE_PERCENT = {
    ("SME", "bank_transfer"): (0.50, 1.20),
    ("SME", "card"): (1.80, 2.90),
    ("SME", "mobile_money"): (1.00, 2.00),
    ("SME", "wallet"): (0.30, 0.90),
    ("Enterprise", "bank_transfer"): (0.25, 0.70),
    ("Enterprise", "card"): (1.40, 2.40),
    ("Enterprise", "mobile_money"): (0.70, 1.50),
    ("Enterprise", "wallet"): (0.15, 0.50),
}

NAME_PREFIX = [
    "Atlas", "Baobab", "Cedar", "Delta", "Equator", "Falcon", "Granite", "Harbor",
    "Indigo", "Juniper", "Kestrel", "Lumen", "Meridian", "Nimbus", "Orbit", "Pioneer",
    "Quartz", "Rift", "Savanna", "Tidal", "Umoja", "Vertex", "Willow", "Zenith",
]
NAME_CORE = [
    "Trading", "Freight", "Foods", "Textiles", "Digital", "Health", "Logistics",
    "Energy", "Imports", "Labs", "Holdings", "Agro", "Systems", "Ventures",
]
NAME_SUFFIX = ["Ltd", "LLC", "GmbH", "SARL", "PLC", "Inc", "Co"]


def weighted(rng: random.Random, options: dict[str, float]) -> str:
    return rng.choices(list(options), weights=list(options.values()), k=1)[0]


def mess(rng: random.Random, value: str, *, case: bool, p: float = 0.06) -> str:
    """Randomly add whitespace (and optionally change case) to exercise Silver cleansing."""
    if rng.random() >= p:
        return value
    if case and rng.random() < 0.5:
        value = rng.choice([value.lower(), value.upper(), value.title()])
    return rng.choice([" ", "  ", ""]) + value + rng.choice([" ", "  ", ""])


def daterange(start: date, end: date):
    for i in range((end - start).days + 1):
        yield start + timedelta(days=i)


def iso(ts: datetime) -> str:
    return ts.strftime("%Y-%m-%dT%H:%M:%SZ")


def build_fx(rng: random.Random) -> dict[tuple[date, str], float]:
    rates: dict[tuple[date, str], float] = {}
    for currency, (rate, drift, vol) in FX.items():
        for d in daterange(START_DATE, END_DATE):
            rates[(d, currency)] = round(rate, 8)
            rate *= math.exp(rng.gauss(drift, vol))
    return rates


def build_fee_schedule() -> list[dict]:
    rows = []
    for i, ((segment, method), (domestic, cross_border)) in enumerate(FEE_PERCENT.items()):
        for j, (corridor_type, pct) in enumerate((("domestic", domestic), ("cross_border", cross_border))):
            rows.append({
                "fee_schedule_id": f"FEE-{i * 2 + j + 1:03d}",
                "segment": segment,
                "payment_method": method,
                "corridor_type": corridor_type,
                "min_amount_usd": 0,
                "max_amount_usd": 1_000_000,
                "fee_type": "percent",
                "fee_value": pct,
                "effective_from": START_DATE.isoformat(),
                "effective_to": END_DATE.isoformat(),
                "is_active": "true",
            })
    return rows


def build_customers(rng: random.Random) -> list[dict]:
    codes = list(COUNTRIES)
    shares = [COUNTRIES[c][2] for c in codes]
    customers = []
    for i in range(1, N_CUSTOMERS + 1):
        country = rng.choices(codes, weights=shares, k=1)[0]
        country_name, currency = COUNTRIES[country][0], COUNTRIES[country][1]
        segment = weighted(rng, SEGMENTS)
        kyc = weighted(rng, KYC_STATUSES)
        onboarded = date(2021, 1, 1) + timedelta(days=rng.randrange(0, 1095))
        approved = onboarded + timedelta(days=rng.randint(1, 30)) if kyc == "approved" else None
        median_volume = 400_000 if segment == "SME" else 12_000_000
        customers.append({
            "customer_id": f"CUST-{i:05d}",
            "company_name": mess(
                rng,
                f"{rng.choice(NAME_PREFIX)} {rng.choice(NAME_CORE)} {rng.choice(NAME_SUFFIX)}",
                case=False,
            ),
            "country_code": mess(rng, country, case=True),
            "country_name": country_name,
            "segment": mess(rng, segment, case=False),
            "industry": rng.choice(INDUSTRIES),
            "kyc_status": mess(rng, kyc, case=True),
            "primary_currency": mess(rng, currency, case=True),
            "onboarded_date": onboarded.isoformat(),
            "kyc_approved_date": approved.isoformat() if approved else "",
            "is_active": "true" if rng.random() < 0.92 else "false",
            "annual_payment_volume_usd": round(rng.lognormvariate(math.log(median_volume), 0.8), 2),
            # internal fields (not written)
            "_country": country,
            "_currency": currency,
            "_segment": segment,
            "_eligible": kyc == "approved",
        })
    return customers


def build_payments(rng: random.Random, customers: list[dict], fx: dict) -> list[dict]:
    payers = [c for c in customers if c["_eligible"] and c["is_active"] == "true"]
    payer_weights = [4.0 if c["_segment"] == "Enterprise" else 1.0 for c in payers]

    days = list(daterange(START_DATE, END_DATE))
    span = len(days) - 1
    # steady growth over two years, quieter weekends
    day_weights = [(1 + 1.2 * i / span) * (0.55 if d.weekday() >= 5 else 1.0) for i, d in enumerate(days)]
    pending_from = END_DATE - timedelta(days=1)

    codes = list(COUNTRIES)
    payments = []
    for day in rng.choices(days, weights=day_weights, k=N_PAYMENTS):
        c = rng.choices(payers, weights=payer_weights, k=1)[0]
        source = c["_country"]
        target = source if rng.random() < 0.15 else rng.choice([x for x in codes if x != source])
        corridor_type = "domestic" if target == source else "cross_border"
        method = weighted(rng, PAYMENT_METHODS)

        hour = min(23, max(0, int(rng.gauss(13, 3.5))))
        initiated = datetime(day.year, day.month, day.day, hour, rng.randrange(60), rng.randrange(60))

        median_amount = 2_500 if c["_segment"] == "SME" else 25_000
        amount_usd = round(min(950_000, max(10, rng.lognormvariate(math.log(median_amount), 1.0))), 2)
        source_currency = c["_currency"]
        rate = fx[(day, source_currency)]

        fail_rate = 0.015 if corridor_type == "domestic" else COUNTRIES[target][3]
        if method == "card":
            fail_rate += 0.02
        if day >= pending_from and rng.random() < 0.6:
            status = "pending"
        elif rng.random() < fail_rate:
            status = "failed"
        else:
            status = "completed"

        settlement_hours = completed_at = None
        fee_usd = 0.0
        if status == "completed":
            median_hours = 1.5 if corridor_type == "domestic" else COUNTRIES[target][4]
            settlement_hours = round(max(0.05, rng.lognormvariate(math.log(median_hours), 0.45)), 2)
            completed_at = iso(initiated + timedelta(hours=settlement_hours))
            pct = FEE_PERCENT[(c["_segment"], method)][0 if corridor_type == "domestic" else 1]
            fee_usd = round(amount_usd * pct / 100, 2)

        payments.append({
            "_initiated": initiated,
            "customer_id": c["customer_id"],
            "source_country_code": mess(rng, source, case=True, p=0.03),
            "target_country_code": mess(rng, target, case=True, p=0.03),
            "source_currency": mess(rng, source_currency, case=True, p=0.03),
            "target_currency": mess(rng, COUNTRIES[target][1], case=True, p=0.03),
            "payment_method": mess(rng, method, case=True, p=0.03),
            "corridor_type": corridor_type,
            "status": mess(rng, status, case=True, p=0.03),
            "amount_source": round(amount_usd / rate, 2),
            "amount_usd": amount_usd,
            "fx_rate_to_usd": rate,
            "fee_usd": fee_usd,
            "initiated_at": iso(initiated),
            "completed_at": completed_at,
            "settlement_hours": settlement_hours,
        })

    payments.sort(key=lambda p: p["_initiated"])
    for i, p in enumerate(payments, start=1):
        p["payment_id"] = f"PAY-{i:07d}"
    return payments


def write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = [k for k in rows[0] if not k.startswith("_")]
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def write_payments(out: Path, payments: list[dict]) -> dict[str, int]:
    folder = out / "payments"
    folder.mkdir(parents=True, exist_ok=True)
    order = ["payment_id"] + [k for k in payments[0] if not k.startswith("_") and k != "payment_id"]
    files: dict[str, list[dict]] = {}
    for p in payments:
        ts = p["_initiated"]
        period = f"{ts.year}_h{1 if ts.month <= 6 else 2}"
        files.setdefault(period, []).append({k: p[k] for k in order})
    counts = {}
    for period, rows in sorted(files.items()):
        name = f"payments_{period}.json"
        (folder / name).write_text(json.dumps(rows), encoding="utf-8")
        counts[name] = len(rows)
    return counts


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--out", default="data/bronze", help="output folder (default: data/bronze)")
    parser.add_argument("--seed", type=int, default=42, help="random seed (default: 42)")
    args = parser.parse_args()

    rng = random.Random(args.seed)
    out = Path(args.out)

    fx = build_fx(rng)
    fee_schedule = build_fee_schedule()
    customers = build_customers(rng)
    payments = build_payments(rng, customers, fx)

    write_csv(out / "customers" / "customers.csv", customers)
    write_csv(
        out / "fx_rates" / "fx_rates.csv",
        [{"rate_date": d.isoformat(), "currency_code": c, "rate_to_usd": r} for (d, c), r in sorted(fx.items())],
    )
    write_csv(out / "fee_schedule" / "fee_schedule.csv", fee_schedule)
    payment_files = write_payments(out, payments)

    assert len(customers) == N_CUSTOMERS
    assert len(fx) == len(FX) * ((END_DATE - START_DATE).days + 1) == 6_579
    assert len(fee_schedule) == 16
    assert sum(payment_files.values()) == N_PAYMENTS and len(payment_files) == 4

    print(f"Bronze data written to {out.resolve()}")
    print(f"  customers     {len(customers):>7,}")
    print(f"  fx_rates      {len(fx):>7,}")
    print(f"  fee_schedule  {len(fee_schedule):>7,}")
    for name, n in payment_files.items():
        print(f"  {name:<24}{n:>7,}")
    print(f"  payments total{N_PAYMENTS:>7,}")


if __name__ == "__main__":
    main()
