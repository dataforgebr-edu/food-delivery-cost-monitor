import argparse
import math
import random
import uuid
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
from infra.logi import get_logger
from pipeline_config import DEFAULT_LOCAL_DIR

logger = get_logger(__name__)

DOMAINS = ["orders", "payments", "fintech", "marketplace", "logistics", "restaurants"]

JOBS_BY_DOMAIN = {
    "orders": [
        "process_order_events",
        "aggregate_order_metrics",
        "validate_order_status",
    ],
    "payments": [
        "process_payment_events",
        "reconcile_transactions",
        "detect_fraud_patterns",
    ],
    "fintech": ["calculate_fees", "process_refunds", "generate_statements"],
    "marketplace": ["update_catalog", "sync_inventory", "calculate_commissions"],
    "logistics": ["track_deliveries", "optimize_routes", "calculate_eta"],
    "restaurants": ["update_menu", "process_ratings", "sync_restaurant_data"],
}

BASE_COST_BY_DOMAIN = {
    "orders": 1.2,
    "payments": 1.5,
    "fintech": 2.0,
    "marketplace": 0.8,
    "logistics": 1.0,
    "restaurants": 0.6,
}

# fintech e payments têm maior probabilidade de gerar outliers
OUTLIER_WEIGHT_BY_DOMAIN = {
    "orders": 1.0,
    "payments": 2.5,
    "fintech": 3.0,
    "marketplace": 0.8,
    "logistics": 1.0,
    "restaurants": 0.7,
}

CLUSTER_TYPES = ["job_cluster", "all_purpose"]
STATUSES = ["success", "failed", "timeout"]
STATUS_WEIGHTS = [0.90, 0.07, 0.03]

JOBS_PER_DOMAIN_PER_DAY = 20
OUTLIER_RATE = 0.05


def _hour_cost_multiplier(hour: int) -> float:
    """Pico de custo no almoço (13h) e jantar (20h) via gaussiana."""
    lunch = math.exp(-0.5 * ((hour - 13) / 1.5) ** 2)
    dinner = math.exp(-0.5 * ((hour - 20) / 1.5) ** 2)
    return 1.0 + 1.5 * max(lunch, dinner)


def _generate_record(execution_date: date, domain: str, is_outlier: bool) -> dict:
    job_name = random.choice(JOBS_BY_DOMAIN[domain])
    cluster_type = random.choice(CLUSTER_TYPES)

    hour = random.randint(0, 23)
    created_at = datetime(
        execution_date.year,
        execution_date.month,
        execution_date.day,
        hour,
        random.randint(0, 59),
        random.randint(0, 59),
    )

    duration_min = round(random.uniform(2.0, 45.0), 2)
    dbu_consumed = round(duration_min * random.uniform(0.2, 0.6), 2)

    base = BASE_COST_BY_DOMAIN[domain]
    cluster_multiplier = 1.5 if cluster_type == "all_purpose" else 1.0
    cost = (
        base
        * (duration_min / 10)
        * _hour_cost_multiplier(hour)
        * cluster_multiplier
        * random.uniform(0.8, 1.2)
    )

    if is_outlier:
        cost *= random.uniform(3.0, 8.0)

    return {
        "job_id": f"job_{uuid.uuid4().hex[:8]}",
        "execution_date": execution_date,
        "domain": domain,
        "job_name": job_name,
        "duration_min": duration_min,
        "dbu_consumed": dbu_consumed,
        "estimated_cost_usd": round(cost, 4),
        "status": random.choices(STATUSES, weights=STATUS_WEIGHTS)[0],
        "cluster_type": cluster_type,
        "created_at": created_at,
        "_is_outlier": is_outlier,
    }


def generate_for_date(
    execution_date: date, output_dir: Path = DEFAULT_LOCAL_DIR
) -> Path:
    """Gera logs de jobs para uma data e salva em Parquet. Retorna o caminho do arquivo."""
    total = len(DOMAINS) * JOBS_PER_DOMAIN_PER_DAY
    outlier_budget = int(total * OUTLIER_RATE)

    # Distribui outliers pelos domínios com pesos diferentes
    domain_pool = [
        d for d in DOMAINS for _ in range(int(OUTLIER_WEIGHT_BY_DOMAIN[d] * 10))
    ]
    outlier_domain_draws = random.choices(domain_pool, k=outlier_budget)

    records = []
    total_outliers = 0

    for domain in DOMAINS:
        domain_outlier_count = outlier_domain_draws.count(domain)
        outlier_indices = set(
            random.sample(
                range(JOBS_PER_DOMAIN_PER_DAY),
                min(domain_outlier_count, JOBS_PER_DOMAIN_PER_DAY),
            )
        )

        for i in range(JOBS_PER_DOMAIN_PER_DAY):
            records.append(
                _generate_record(
                    execution_date, domain, is_outlier=i in outlier_indices
                )
            )

    df = pd.DataFrame(records)

    for _, row in df[df["_is_outlier"]].iterrows():
        logger.info(
            f"OUTLIER | job_id={row['job_id']} domain={row['domain']} "
            f"job={row['job_name']} cost=${row['estimated_cost_usd']:.4f}"
        )
        total_outliers += 1

    df = df.drop(columns=["_is_outlier"])

    partition_path = output_dir / f"date={execution_date.isoformat()}"
    partition_path.mkdir(parents=True, exist_ok=True)
    file_path = partition_path / "data.parquet"
    df.to_parquet(file_path, index=False, engine="pyarrow")

    logger.info(
        f"date={execution_date} | {len(df)} registros | {total_outliers} outliers | {file_path}"
    )
    return file_path


def generate_for_period(
    start_date: date, end_date: date, output_dir: Path = DEFAULT_LOCAL_DIR
) -> list[Path]:
    """Gera logs para um intervalo de datas."""
    paths = []
    current = start_date
    while current <= end_date:
        paths.append(generate_for_date(current, output_dir))
        current += timedelta(days=1)
    return paths


def main():
    parser = argparse.ArgumentParser(
        description="Gera logs sintéticos de execução de jobs"
    )
    parser.add_argument(
        "--days", type=int, default=30, help="Número de dias a gerar (padrão: 30)"
    )
    parser.add_argument(
        "--start-date", type=date.fromisoformat, help="Data inicial (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--end-date", type=date.fromisoformat, help="Data final (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--output-dir", type=Path, default=DEFAULT_LOCAL_DIR, help="Diretório de saída"
    )
    args = parser.parse_args()

    if args.start_date and args.end_date:
        start, end = args.start_date, args.end_date
    else:
        end = date.today() - timedelta(days=1)
        start = end - timedelta(days=args.days - 1)

    logger.info(f"Gerando dados de {start} a {end}")
    paths = generate_for_period(start, end, args.output_dir)
    logger.info(f"Concluído: {len(paths)} arquivos gerados")


if __name__ == "__main__":
    # main()
    generate_for_date(date(2026, 6, 30))
