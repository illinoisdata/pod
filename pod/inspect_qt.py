import numpy as np
import matplotlib.pyplot as plt
from itertools import product
from model import QLearningPoddingModel


def idx_to_state(idx):
    # The lists from which the Cartesian product was created
    lists = [
        [True, False],
        QLearningPoddingModel.SIZES,
        QLearningPoddingModel.SIZES,
        QLearningPoddingModel.PROBABILITIES,
        QLearningPoddingModel.PROBABILITIES,
        QLearningPoddingModel.TYPES,
        QLearningPoddingModel.ACTION_CHOICES,
    ]
    
    total_combinations = [len(lst) for lst in lists]
    parameters = []
    for lst_len in reversed(total_combinations):
        value_idx = idx % lst_len
        parameters.append(lists[len(lists) - len(parameters) - 1][value_idx])
        idx //= lst_len
    parameters.reverse()
    return tuple(parameters)


def sort_by_max(arr):
    max_values = np.max(arr, axis=1)
    sorted_indices = np.argsort(max_values)[::-1]
    sorted_arr = arr[sorted_indices]
    return sorted_indices, sorted_arr


def inspect(qt_path):
    qt = np.load(qt_path)
    fresh_qt = np.load("qtables/EVAL.npy")
    # print(f"MAX {np.max(qt)}")
    # print(f"MIN {np.min(qt)}")
    # plt.hist(qt, bins=10, edgecolor='black')
    # plt.title('Histogram of Data')
    # plt.xlabel('Value')
    # plt.ylabel('Frequency')
    # plt.savefig("qt_vals.png")
    # plt.show()
    used_values = np.where((qt == 10) | (qt == 20) | (qt == 30), -10000, qt)
    sorted_used_idx, sorted_used_qt = sort_by_max(used_values)
    sorted_fresh_idx, sorted_fresh_qt = sort_by_max(fresh_qt)
    with open("differences.txt", "w") as diff_file:
        for idx in sorted_used_idx:
            if np.max(qt[idx] < 1e-8):
                continue
            if np.argmax(qt[idx]) != np.argmax(fresh_qt[idx]):
                diff_file.write(f"STATE {idx_to_state(idx)} VALUE IN USED {qt[idx]} VALUE IN FRESH {fresh_qt[idx]}\n")
    # relevant_max_id = sorted_indices[-28650:-28500]
    # print(f"Max modified idices {relevant_max_id}, values {qt.flatten()[relevant_max_id]}")
    # print(f"Min idices {sorted_indices[:20]}, values {qt.flatten()[sorted_indices[:20]]}")
    # for i in relevant_max_id:
    #     print(index_to_state(i))



if __name__ == "__main__":
    inspect("qtables/0-6&0-1.npy")