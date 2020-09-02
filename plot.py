"""
Function: plot data and stats and abnormal values
Author: Xinran Wang
Date: 09/02/2020
"""

import matplotlib.pyplot as plt
import os

plt.rc('font', family='Tahoma')


def create_folder(directory, name='ReinjectionFigures'):
    """
    Create a folder in the assigned directory with assigned name
    :param directory: the directory where the new folder will be located
    :param name: the folder's name
    :return: the absolute path of the folder (None if the folder already exists under the directory)
    """
    path = os.path.join(directory, name)
    exist = os.path.exists(path)
    if exist:
        print("The folder: " + path + " already exists, cannot create new folder")
        return
    else:
        os.mkdir(path)
        print("Created a new folder in: " + directory)
        return path


def plot_ori_and_test(dataframe, save_path, to_analysis, cam_id_name):
    """
    Plot the original and test data based on the given data column and save the figure to the given path
    :param dataframe: a pandas dataframe of the test data excel
    :param save_path: a string representing the path where the output figure should locate in
    :param to_analysis: a string corresponding to the column on the dataframe, indicating the data to look into
    :param cam_id_name: a string representing the name of the cam id parameter
    :return: None
    """
    fig = plt.figure(figsize=(20, 8), dpi=80)
    ax = plt.subplot(111)
    name_list = []
    # colors = ["r", "g", "b", "y", "c"]
    index_names = list(dataframe.columns)
    for n in index_names:
        if n.startswith(to_analysis):
            name = n.split("_")[-1]
            name_list.append(name)
            plt.plot(dataframe[cam_id_name], dataframe[n])

    plt.title("Comparison of Original and Test Data's " + to_analysis + " as a function of Camera ID", color='navy', fontsize=18, y=1.03)
    plt.xlabel(cam_id_name, color='navy', fontsize=15)
    plt.ylabel(to_analysis, color='navy', fontsize=15)
    ax.legend(name_list, loc=1, fontsize=12)

    square_bracket = to_analysis.find("[")
    if square_bracket != -1:
        file_name = to_analysis[:square_bracket]
        plt.savefig(os.path.join(os.path.abspath(save_path), file_name + "-OriTestFig" + ".png"))
    else:
        plt.savefig(os.path.join(os.path.abspath(save_path), to_analysis + "-OriTestFig" + ".png"))
    # plt.show()


def plot_data_and_stats(dataframe, save_path, has_stats, to_analysis, cam_id_name):
    """
    Plot all the test data as well as the mean and std statistics in one figure
    :param dataframe: a pandas dataframe of the test data excel
    :param save_path: a string representing the path where the output figure should locate in
    :param has_stats: a boolean value of whether this dataframe has statistics (if no test case exists, then no stats)
    :param to_analysis: a string corresponding to the column on the dataframe, indicating the data to look into
    :param cam_id_name: a string representing the name of the cam id parameter
    :return: None
    """
    fig = plt.figure(figsize=(20, 8), dpi=80)
    fig.suptitle("Comparison of Original and Test Data's " + to_analysis + " as a function of Camera ID, with mean and std presented", color='navy', fontsize=18, y=0.95)
    ax_up = plt.subplot(211)
    ax_down = plt.subplot(212)
    name_list = []
    colors = ["mediumseagreen", "orangered"]
    index_names = list(dataframe.columns)
    if has_stats:
        stats_list = index_names[-2:]
    else:
        stats_list = []
    for n in index_names:
        if n.startswith(to_analysis):
            name = n.split("_")[-1]
            name_list.append(name)
            ax_up.plot(dataframe[cam_id_name], dataframe[n])
    for idx, m_s in enumerate(stats_list):
        ax_down.plot(dataframe[cam_id_name], dataframe[m_s], color=colors[idx])

    plt.xlabel(cam_id_name, color='navy', fontsize=15)
    plt.ylabel(to_analysis, color='navy', fontsize=15, y=1.7)
    ax_up.legend(name_list, loc=1, fontsize=12)
    ax_down.legend(stats_list, loc=1, fontsize=12)

    square_bracket = to_analysis.find("[")
    if square_bracket != -1:
        file_name = to_analysis[:square_bracket]
        plt.savefig(os.path.join(os.path.abspath(save_path), file_name + "-StatsFig" + ".png"))
    else:
        plt.savefig(os.path.join(os.path.abspath(save_path), to_analysis + "-StatsFig" + ".png"))
    # plt.show()


def plot_data_and_stats_with_outliers(dataframe, save_path, has_stats, to_analysis, cam_id_name, threshold):
    """
    Plot all the test data as well as the mean, std statistics and the outlier line of data analysis in one figure
    :param dataframe: a pandas dataframe of the test data excel
    :param save_path: a string representing the path where the output figure should locate in
    :param has_stats: a boolean value of whether this dataframe has statistics (if no test case exists, then no stats)
    :param to_analysis: a string corresponding to the column on the dataframe, indicating the data to look into
    :param cam_id_name: a string representing the name of the cam id parameter
    :param threshold: the threshold indicating the outlier bottom line
    :return: None
    """
    fig = plt.figure(figsize=(20, 8), dpi=80)
    fig.suptitle("Changes in Test Data's " + to_analysis + " as a function of Camera ID, with mean and std presented", color='navy', fontsize=18, y=0.95)
    ax_up = plt.subplot(211)
    ax_down = plt.subplot(212)
    name_list = []
    colors = ["mediumseagreen", "orangered"]
    index_names = list(dataframe.columns)
    if has_stats:
        stats_list = index_names[-2:]
    else:
        stats_list = []
    for n in index_names:
        if n.startswith(to_analysis):
            name = n.split("_")[-1]
            name_list.append(name)
            ax_up.plot(dataframe[cam_id_name], dataframe[n])
    for idx, m_s in enumerate(stats_list):
        ax_down.plot(dataframe[cam_id_name], dataframe[m_s], color=colors[idx])
    plt.xlabel(cam_id_name, color='navy', fontsize=15)
    plt.ylabel(to_analysis, color='navy', fontsize=15, y=1.7)
    plt.axhline(y=threshold, ls=":", c="purple")

    x_axis_max = plt.axis()[1]
    ax_down.text(x_axis_max+50, threshold, "Abnormal data:\nstd >= {:.5f}".format(threshold), fontsize=12, color='navy', bbox=dict(facecolor='white', alpha=0.5))
    ax_up.legend(name_list, loc=1, fontsize=12)
    ax_down.legend(stats_list, loc=1, fontsize=12)

    square_bracket = to_analysis.find("[")
    if square_bracket != -1:
        file_name = to_analysis[:square_bracket]
        plt.savefig(os.path.join(os.path.abspath(save_path), file_name + "-StatsAbnormalFig" + ".png"))
    else:
        plt.savefig(os.path.join(os.path.abspath(save_path), to_analysis + "-StatsAbnormalFig" + ".png"))
    # plt.show()

