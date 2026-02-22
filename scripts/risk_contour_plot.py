# /usr/bin/python3

'''
 Will plot the results of the risk roll simulator
 to determine the most probable outcome of dice roles in a given situation.

'''
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns


# reading data from a txt file and plotting each row as a line on a graph
def plot_risk_contour(datafile, outputfile):
    '''
    Creating a color map of the attacker success rate
    from the risk attack simulation results.
    datafile: str : path to the txt file containing the simulation results
    outputfile: str : path to save the contour plot image
    
    '''
    # load .txt file
    data = pd.read_csv(datafile, delim_whitespace=True, header=None)
    Z = data.to_numpy()
    # print(Z)

    # for i in range(Z.shape[0]):
    #     for j in range(Z.shape[1]):
    #         Z[i][j] = Z[i][j] * 100.0  # convert to percentage      

    # create contour plot
    # plt.figure(figsize=(10, 8))
    # X = np.arange(1, Z.shape[1] + 1)
    # Y = np.arange(1, Z.shape[0] + 1)
    # X, Y = np.meshgrid(X, Y)
    # contour = plt.contourf(Y, X, Z, levels=10, cmap='viridis')
    # # Drawing a diagonal line for reference at attacker win rate = 50%
    # plt.plot([1, Z.shape[0]], [1, Z.shape[1]], color='red', linestyle='--', label='Attacker Win Rate = 50%')

    # # Drawing a reference line where attacker win rate = 75% based on data observation
    # plt.plot([1, Z.shape[0]], [3, Z.shape[1]], color='blue', linestyle='--', label='Attacker Win Rate = 75%')

    # plt.colorbar(contour, label='Attacker Win Rate (%)')
    # plt.title('Risk Attack Simulation: Attacker Win Rate Contour Plot')
    # plt.ylabel('Number of Attacker Troops')
    # plt.xlabel('Number of Defender Troops')
    # plt.grid(True)

    # # save the plot
    # plt.savefig(outputfile)
    # plt.close()


    # Making a plot showing win rate for attacker number = defender number+2
    plt.figure(figsize=(10, 6))
    attacker_troops = np.arange(1, Z.shape[0] + 1)
    defender_troops = attacker_troops 
    win_rates = []
    for i in range(len(attacker_troops)):
        atk = attacker_troops[i]
        defn = defender_troops[i]
        if defn >= 1 and atk <= Z.shape[0] and defn <= Z.shape[1]:
            win_rate = Z[atk - 1][defn - 1]
            win_rates.append(win_rate)
        else:
            win_rates.append(None)
    plt.plot(attacker_troops, win_rates, marker='o')
    plt.title('Attacker Win Rate vs Number of Attacker Troops (Defender Troops = Attacker Troops - 2)')
    plt.xlabel('Number of Attacker Troops') 
    plt.ylabel('Attacker Win Rate (%)')
    plt.grid(True)
    plt.ylim(0, 100)
    plt.savefig('attacker_win_rate_vs_troops.png')
    plt.close()










if __name__ == "__main__":
    # Example usage
    plot_risk_contour('./risk_attack_simulation_results.txt', 'risk_contour_plot.png')        
