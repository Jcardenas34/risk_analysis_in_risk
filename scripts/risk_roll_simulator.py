#!/usr/bin/python3

'''
 Will sumliate dice roles played within the game RISK
 to determine the most probable outcome of dice roles in a given situation.

 And maybe determine a gernal guideline by which one should attack or not.
 
 '''

import random
import statistics
import matplotlib.pyplot as plt
import numpy as np
from scipy.stats import norm
from collections import defaultdict
import pandas as pd

def write_results_to_file(filename:str, mean:list, std:list):
    '''
    Writes the results of the simulation to a file

    :param filename: The name of the file to write to
    :type filename: str
    :param results: The results of the simulation
    :type results: array
    '''

    with open(filename, 'w') as f:
        for m, s in zip(mean, std):
            f.write(f"{m} +/-{s} ")
        f.write("\n")   

def simulate_attacks(n_attackers:int, n_defenders:int, n_trials:int):
    '''
    Runs a simulation of continuous attacks to dertermine how often
    an attacker will will or lose, under different numbers of defenders
    
    :param n_attackers: Description
    :type n_attackers: int
    :param n_defenders: Description
    :type n_defenders: int

    '''

    # Where the number of wins an losses will be kept
    win_loss_ratio = defaultdict(int)
    # if n_attackers < n_defenders:
    #     win_loss_ratio["attacker"] = 0
    #     win_loss_ratio["defender"] = 100
    #     win_loss_ratio["draw"] = 0
    #     return win_loss_ratio

    # General Rules for attacking in RISK
    # 1. The Attacker must have 2 solders in order to attack.
    # 2. The attacker can choose to attack with at most 3 soldiers at a time
    # 3. In the case of a draw, the defender wins
    
    # print(f"Start Att, Def: {n_attackers} {n_defenders}")
    for i in range(n_trials):
        # print(f"Trial: {i+1}")
        trial_attackers = n_attackers
        trial_defenders = n_defenders
        
        # Stop attacking if you have less soldiers than the defender
        # Stop attacking if the defender has no more soldiers
        # while (trial_attackers >= trial_defenders) and (trial_defenders > 0):
        # while (trial_attackers >= trial_defenders) and (trial_defenders > 0) and (trial_attackers > 1):
        # To attack at all, the attacker must have more than 1 soldier, e.g 2+
        # while (trial_attackers > 1) and (trial_defenders > 0):

        # Stop attacking if my army count is 1 less than my opponent 
        while (trial_attackers > trial_defenders-2) and (trial_defenders > 0) and (trial_attackers > 1):


            # print(trial_attackers, trial_defenders)
            if trial_defenders > 0 :

                # Simulate the rolls according to the max number of dice allowed, or number of soldiers one has
                att_res = sorted([random.randint(1,6) for _ in range(min(3, trial_attackers-1))], reverse=True)
                def_res = sorted([random.randint(1,6) for _ in range(min(2, trial_defenders))], reverse=True)


                # determine who takes losses
                for a, d in zip(att_res, def_res):
                    if d>=a:
                        trial_attackers-=1
                    else:
                        trial_defenders-=1

                # print(f"\t Att, Def: {trial_attackers, trial_defenders}, via {att_res, def_res}" )


        
        if trial_defenders <= 0:
            # print("attacker_win")
            win_loss_ratio["attacker"] += 1
        # If the defender has more soldiers than the attacker, they win
        elif trial_attackers < 2:
            # print("defender_win")
            win_loss_ratio["defender"] += 1
        # draw if both have soldiers remaining  
        else:
            # print("draw")
            win_loss_ratio["cease_fire"] +=1



    # Normalize trial results
    for k,v in win_loss_ratio.items():
        win_loss_ratio[k] = (v*100)/n_trials

    return win_loss_ratio




# Precomputed transition probabilities for RISK battles
# (attacker_dice, defender_dice): (P(attacker loses 2), P(both lose 1), P(defender loses 2))
TRANSITIONS = {
    (3,2): (0.2925668724, 0.335756, 0.3716771276),
    (3,1): (0.255419,     0.0,      0.744581),
    (2,2): (0.4483024691, 0.324074,  0.2276235309),
    (2,1): (0.421296,     0.0,      0.578704),
    (1,2): (0.745370,     0.0,      0.254630),
    (1,1): (0.583333,     0.0,      0.416667)
}

def simulate_fast(A, D, trials):
    '''
    Fast simulation of RISK attacks using precomputed transition probabilities
    created by ChatGPT.
    Gives the probability of the attacker winning according to a Markov process.
        
    :param A: Description
    :param D: Description
    :param trials: Description
    '''
    wins = 0

    for _ in range(trials):
        a, d = A, D
        while a > 1 and d > 0:

            ad = min(3, a-1)
            dd = min(2, d)
            p0, p1, p2 = TRANSITIONS[(ad, dd)]
            r = random.random()

            if r < p0:
                a -= min(2, ad)
            elif r < p0 + p1:
                a -= 1; d -= 1
            else:
                d -= min(2, dd)

        wins += (d == 0)

    return wins / trials



def plot_results(x_vals, mean_win_arr, std_win_arr,
                    mean_loss_arr, std_loss_arr,
                    mean_draw_arr, std_draw_arr,
                    attackers, defenders, directory:str='results'):
    '''
    Plots the results of the simulation
    :param x_vals: The x values (number of attackers)

    '''
        
    # # Plotting the results
    plt.figure(figsize=(15, 6))
    plt.errorbar(x_vals, mean_win_arr, yerr=std_win_arr, label='Attacker Win %', fmt='-o')
    plt.errorbar(x_vals, mean_loss_arr, yerr=std_loss_arr, label='Defender Win %', fmt='-o')
    plt.errorbar(x_vals, mean_draw_arr, yerr=std_draw_arr, label='Cease Fire %', fmt='-o')
    plt.title(f'RISK Attack Simulation Results (Defenders: {defenders})')
    plt.xlabel('Number of Attackers')
    plt.ylabel('Percentage (%)')
    plt.xticks(x_vals)
    plt.legend()

    # Placing red vertical line at x=3
    plt.axvline(x=defenders, color='red', linestyle='--', label=f'x={defenders}')
    # placing a horizontal line at y=50
    plt.axhline(y=50, color='black', linestyle='--', label='y=50%')

    plt.grid(True, alpha=0.3, linestyle='--')
    plt.savefig(f'{directory}/risk_attack_simulation_att_{attackers}_def_{defenders}.png')






# Specifying the number of defenders
# Will loop through different numbers of attackers later
max_defenders = range(2, 25)
# max_defenders = range(18, 25)


# Create pandas dataframe to hold results
df = pd.DataFrame(columns=[x for x in max_defenders])

for defenders in max_defenders:

    mean_win_arr = []
    mean_loss_arr = []
    mean_draw_arr = []

    std_win_arr = []
    std_loss_arr = []
    std_draw_arr = []
        
    # Simulate 2 to n max_defenders attackers
    for attackers in max_defenders:
        win_arr = []
        loss_arr = []
        draw_arr = []
        

        # Simlulate 100 batches of 100 battles, to generate win loss ratio and standard deviations
        for _ in range(100):
            wlr = simulate_attacks(attackers, defenders, 100)
            # wlr = simulate_fast(attackers, defenders, 100)
            win_arr.append(wlr["attacker"])
            loss_arr.append(wlr["defender"])
            draw_arr.append(wlr["cease_fire"])
        print(f"Attackers: {attackers}, Defenders: {defenders} => Win%: {statistics.mean(win_arr):.2f} +/- {statistics.stdev(win_arr):.2f}")
        mean_win_arr.append(statistics.mean(win_arr))
        mean_loss_arr.append(statistics.mean(loss_arr))
        mean_draw_arr.append(statistics.mean(draw_arr))

        std_win_arr.append(statistics.stdev(win_arr))
        std_loss_arr.append(statistics.stdev(loss_arr))
        std_draw_arr.append(statistics.stdev(draw_arr))

    # Adding row to dataframe
    df.loc[f'Defenders_{defenders}'] = mean_win_arr

    # Plot results
    plot_results(max_defenders, mean_win_arr, std_win_arr,
                    mean_loss_arr, std_loss_arr,
                    mean_draw_arr, std_draw_arr,
                    attackers, defenders, directory='results')
    
# Write results to file
with open("risk_attack_simulation_results.txt", "w") as f:
    f.write(df.to_string(index=False))


