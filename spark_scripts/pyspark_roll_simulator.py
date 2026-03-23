#!/usr/bin/python3
"""
Distributed RISK battle simulation using PySpark.
Simulates attacker win rates across army size combinations.

Usage:
    python risk_spark_sim.py --min_att 2 --max_att 24 --min_def 2 --max_def 24 --trials 100 --batches 100
"""

import argparse
import itertools
import pandas as pd
from pyspark.sql import SparkSession


def run_parallel_sim(params):
    """
    Worker function distributed across Spark cluster nodes.
    Simulates RISK battles for a single (attacker, defender) combination.

    :param params: Tuple of (attacker_count, defender_count, n_trials, n_batches)
    :type params: tuple
    :returns: (defender_count, attacker_count, mean_win_rate, std_win_rate)
    :rtype: tuple
    """
    attacker_count, defender_count, n_trials, n_batches = params

    import random
    import statistics
    from collections import defaultdict

    def simulate_attacks_local(n_attackers, n_defenders, trials):
        """
        Simulates RISK battles with the threshold stopping rule:
        Stop attacking when attacker army falls below (defender count - 1).

        :param n_attackers: Starting attacker army size
        :param n_defenders: Starting defender army size
        :param trials: Number of trials per batch
        :returns: dict with attacker win%, defender win%, cease_fire%
        """
        win_loss_ratio = defaultdict(int)

        for _ in range(trials):
            trial_attackers = n_attackers
            trial_defenders = n_defenders

            # Threshold stopping rule: consistent with original analysis
            # Stop when attacker would fall more than 1 below defender count
            while (trial_attackers > trial_defenders - 2) and \
                  (trial_defenders > 0) and \
                  (trial_attackers > 1):

                att_dice = min(3, trial_attackers - 1)
                def_dice = min(2, trial_defenders)

                att_res = sorted(
                    [random.randint(1, 6) for _ in range(att_dice)],
                    reverse=True
                )
                def_res = sorted(
                    [random.randint(1, 6) for _ in range(def_dice)],
                    reverse=True
                )

                for a, d in zip(att_res, def_res):
                    if d >= a:
                        trial_attackers -= 1
                    else:
                        trial_defenders -= 1

            # Classify outcome
            if trial_defenders <= 0:
                win_loss_ratio["attacker"] += 1
            elif trial_attackers < 2:
                win_loss_ratio["defender"] += 1
            else:
                win_loss_ratio["cease_fire"] += 1

        # Return both normalization methods
        total_resolved = win_loss_ratio["attacker"] + win_loss_ratio["defender"]
        
        win_rate_raw = (win_loss_ratio["attacker"] * 100) / trials
        win_rate_resolved = (
            (win_loss_ratio["attacker"] * 100) / total_resolved
            if total_resolved > 0 else 0.0
        )
        
        return win_rate_raw, win_rate_resolved

    # Run batches and collect results
    raw_rates = []
    resolved_rates = []
    
    for _ in range(n_batches):
        raw, resolved = simulate_attacks_local(attacker_count, defender_count, n_trials)
        raw_rates.append(raw)
        resolved_rates.append(resolved)

    return (
        defender_count,
        attacker_count,
        statistics.mean(raw_rates),
        statistics.stdev(raw_rates),
        statistics.mean(resolved_rates),
        statistics.stdev(resolved_rates),
    )


def parse_args():
    """
    Parses command-line arguments for simulation configuration.
    """
    parser = argparse.ArgumentParser(
        description="Distributed RISK battle simulator using PySpark"
    )
    parser.add_argument("--min_att",  type=int, default=2,   help="Minimum attacker army size")
    parser.add_argument("--max_att",  type=int, default=24,  help="Maximum attacker army size")
    parser.add_argument("--min_def",  type=int, default=2,   help="Minimum defender army size")
    parser.add_argument("--max_def",  type=int, default=24,  help="Maximum defender army size")
    parser.add_argument("--trials",   type=int, default=100, help="Trials per batch")
    parser.add_argument("--batches",  type=int, default=100, help="Batches per scenario")
    parser.add_argument("--slices",   type=int, default=100, help="Spark partition count")
    parser.add_argument("--output",   type=str, default="risk_results.txt", help="Output filename")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    att_range = range(args.min_att, args.max_att + 1)
    def_range = range(args.min_def, args.max_def + 1)

    spark = SparkSession.builder.appName("RiskParallelSim").getOrCreate()
    sc = spark.sparkContext

    # Build parameter tuples — trials and batches travel with each task
    scenarios = [
        (att, def_, args.trials, args.batches)
        for att, def_ in itertools.product(att_range, def_range)
    ]

    print(f"Simulating {len(scenarios)} scenarios across cluster...")
    print(f"  Attackers: {args.min_att}–{args.max_att}")
    print(f"  Defenders: {args.min_def}–{args.max_def}")
    print(f"  {args.batches} batches × {args.trials} trials per scenario")

    dist_scenarios = sc.parallelize(scenarios, numSlices=args.slices)
    raw_results = dist_scenarios.map(run_parallel_sim).collect()

    # Build results dataframe
    results_df = pd.DataFrame(raw_results, columns=[
        "Defenders", "Attackers",
        "Win_Rate_Raw",    # includes cease-fires in denominator
        "Std_Raw",
        "Win_Rate_Resolved",  # excludes cease-fires from denominator
        "Std_Resolved"
    ])

    # Two pivot tables — one per normalization method
    pivot_raw = results_df.pivot(
        index="Defenders", columns="Attackers", values="Win_Rate_Raw"
    )
    pivot_resolved = results_df.pivot(
        index="Defenders", columns="Attackers", values="Win_Rate_Resolved"
    )

    with open(args.output, "w") as f:
        f.write("=== Win Rate (raw: cease-fires counted as trials) ===\n")
        f.write(pivot_raw.to_string(index=True))
        f.write("\n\n=== Win Rate (resolved: cease-fires excluded) ===\n")
        f.write(pivot_resolved.to_string(index=True))

    print(f"Done. Results written to {args.output}")
    spark.stop()