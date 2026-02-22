import random
import statistics
from collections import defaultdict
from pyspark.sql import SparkSession
import pandas as pd
import itertools

# --- 1. THE WORKER FUNCTION ---
# This function is sent to every Pi in the cluster.
def run_parallel_sim(params):
    attacker_count, defender_count = params
    n_trials = 100
    batches = 100
    
    # We must import inside the function so Workers have access
    import random
    import statistics
    from collections import defaultdict

    def simulate_attacks_local(n_attackers, n_defenders, trials):
        win_loss_ratio = defaultdict(int)
        if n_attackers <= n_defenders:
            return {"attacker": 0.0}

        for _ in range(trials):
            trial_attackers = n_attackers
            trial_defenders = n_defenders
            
            while (trial_attackers > 1) and (trial_defenders > 0):
                att_res = sorted([random.randint(1,6) for _ in range(min(3, trial_attackers-1))], reverse=True)
                def_res = sorted([random.randint(1,6) for _ in range(min(2, trial_defenders))], reverse=True)

                for a, d in zip(att_res, def_res):
                    if d >= a:
                        trial_attackers -= 1
                    else:
                        trial_defenders -= 1
            
            if trial_defenders <= 0:
                win_loss_ratio["attacker"] += 1
                
        return {"attacker": (win_loss_ratio["attacker"] * 100) / trials}

    # Run the batches
    batch_results = []
    for _ in range(batches):
        res = simulate_attacks_local(attacker_count, defender_count, n_trials)
        batch_results.append(res["attacker"])
    
    # Return the summary to the Master
    return (defender_count, attacker_count, statistics.mean(batch_results))

# --- 2. THE MASTER EXECUTION ---
if __name__ == "__main__":
    # Initialize Spark
    spark = SparkSession.builder.appName("RiskParallelSim").getOrCreate()
    sc = spark.sparkContext

    # Configuration
    max_range = range(2, 41)
    
    # Create the combinations (Attacker, Defender)
    scenarios = list(itertools.product(max_range, max_range))
    
    # Distribute work (using 100 slices to keep all Pi cores busy)
    dist_scenarios = sc.parallelize(scenarios, numSlices=100)
    
    print("Starting distributed simulation across the cluster...")
    
    # Map work to workers and collect results back to Master
    raw_results = dist_scenarios.map(run_parallel_sim).collect()
    
    # --- 3. POST-PROCESSING (Master Node Only) ---
    # raw_results is a list of (defenders, attackers, mean_win)
    results_df = pd.DataFrame(raw_results, columns=['Defenders', 'Attackers', 'Win_Rate'])
    
    # Pivot the data to match your original table format
    # Rows = Defenders, Columns = Attackers
    df_pivot = results_df.pivot(index='Defenders', columns='Attackers', values='Win_Rate')

    # Write results to file using your requested logic
    output_file = "risk_attack_simulation_results.txt"
    with open(output_file, "w") as f:
        f.write(df_pivot.to_string(index=True)) # index=True to keep Defender labels

    print(f"Simulation complete. Results written to {output_file}")
    
    spark.stop()