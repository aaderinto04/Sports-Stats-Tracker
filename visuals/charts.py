# visuals/charts.py
import matplotlib.pyplot as plt

def plot_goal_difference(df):
    if df.empty:
        print("No data to plot.")
        return
    plt.figure(figsize=(8,5))
    plt.hist(df["goal_difference"], bins=8)
    plt.title("Distribution of Goal Differences")
    plt.xlabel("Goal Difference")
    plt.ylabel("Match Count")
    plt.tight_layout()
    plt.show()
