"""
Function: plot solely original data, solely Reinjection data, a combination of original data, Reinjection mean and Reinjection std
Author: Xinran Wang
Date: 08/14/2020
"""

import matplotlib.pyplot as plt
import os
plt.rc('font', family='Arial')


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


def plot_original(dataframe, save_path, index, to_analysis):
    """
    Plot only the original data based on the given data column and save the figure to the given path
    :param dataframe: a pandas dataframe of the original data excel
    :param save_path: a string representing the path where the output figure should locate in
    :param index: the number representing the id of figure
    :param to_analysis: a string corresponding to the column on the dataframe, indicating the data to look into
    :return: None
    """
    fig = plt.figure(figsize=(20,8),dpi=80)
    ax = plt.subplot(111)
    plt.plot(dataframe['Cam_id'], dataframe[to_analysis], color='r')
    plt.title("Changes in Original Data's " + to_analysis + " as a function of Camera ID", color = 'b', fontsize = 18)
    plt.xlabel('Cam_id', color='b', fontsize = 15)
    plt.ylabel(to_analysis, color='b', fontsize = 15)
    plt.savefig(os.path.join(os.path.abspath(save_path), str(index) + "-1-" + to_analysis + "-OriginalFig" + ".png"))
    #plt.show()


def plot_tests(dataframe, save_path, to_analysis):
    """
    Plot the reinjection test data based on the given data column and save the figure to the given path
    :param dataframe: a pandas dataframe of the test data excel
    :param save_path: a string representing the path where the output figure should locate in
    :param to_analysis: a string corresponding to the column on the dataframe, indicating the data to look into
    :return: None
    """
    fig = plt.figure(figsize=(20,8),dpi=80)
    ax = plt.subplot(111)
    name_list = []
    # colors = ["r", "g", "b", "y", "c"]
    index_names = list(dataframe.columns)
    for n in index_names:
        if n.startswith(to_analysis):
            test_name = n.split("_")[-1]
            name_list.append(test_name)
            plt.plot(dataframe['Camera_Frame_ID'], dataframe[n])

    plt.title("Changes in Test Data's " + to_analysis + " as a function of Camera ID", color='b', fontsize=18, y=1.03)
    plt.xlabel('Camera_Frame_ID', color='b', fontsize=15)
    plt.ylabel(to_analysis, color='b', fontsize=15)
    ax.legend(name_list, loc=1, fontsize=12)
    square_bracket = to_analysis.find("[")
    if square_bracket != -1:
        file_name = to_analysis[:square_bracket]
        plt.savefig(os.path.join(os.path.abspath(save_path), file_name + "-TestFig" + ".png"))
    else:
        plt.savefig(os.path.join(os.path.abspath(save_path), to_analysis + "-TestFig" + ".png"))
    # plt.show()
    
    
def plot_tests_and_stats(dataframe, save_path, to_analysis):
    """
    Plot all the test data as well as the mean and std statistics in one figure
    :param dataframe: a pandas dataframe of the test data excel
    :param save_path: a string representing the path where the output figure should locate in
    :param to_analysis: a string corresponding to the column on the dataframe, indicating the data to look into
    :return: None
    """
    fig = plt.figure(figsize=(20,8),dpi=80)
    fig.suptitle("Changes in Test Data's " + to_analysis + " as a function of Camera ID, with mean and std presented", color='b', fontsize=18, y=0.95)
    ax_up = plt.subplot(211)
    ax_down = plt.subplot(212)
    name_list = []
    colors = ["mediumseagreen", "orangered"]
    index_names = list(dataframe.columns)
    stats_list = index_names[-2:]
    for n in index_names:
        if n.startswith(to_analysis):
            test_name = n.split("_")[-1]
            name_list.append(test_name)
            ax_up.plot(dataframe['Camera_Frame_ID'], dataframe[n])
    for idx, m_s in enumerate(stats_list):
        ax_down.plot(dataframe['Camera_Frame_ID'], dataframe[m_s], color=colors[idx])

    plt.xlabel('Camera_Frame_ID', color='b', fontsize=15)
    plt.ylabel(to_analysis, color='b', fontsize=15, y=1.7)
    ax_up.legend(name_list, loc=1, fontsize=12)
    ax_down.legend(stats_list, loc=1, fontsize=12)
    square_bracket = to_analysis.find("[")
    if square_bracket != -1:
        file_name = to_analysis[:square_bracket]
        plt.savefig(os.path.join(os.path.abspath(save_path), file_name + "-TestStatsFig" + ".png"))
    else:
        plt.savefig(os.path.join(os.path.abspath(save_path), to_analysis + "-TestStatsFig" + ".png"))
    # plt.show()


def plot_tests_and_stats_with_outliers(dataframe, save_path, to_analysis, threshold):
    """
    Plot all the test data as well as the mean, std statistics and the outlier line of data analysis in one figure
    :param dataframe: a pandas dataframe of the test data excel
    :param save_path: a string representing the path where the output figure should locate in
    :param to_analysis: a string corresponding to the column on the dataframe, indicating the data to look into
    :param threshold: the threshold indicating the outlier bottom line
    :return: None
    """
    fig = plt.figure(figsize=(20,8),dpi=80)
    fig.suptitle("Changes in Test Data's " + to_analysis + " as a function of Camera ID, with mean and std presented", color='b', fontsize=18, y=0.95)
    ax_up = plt.subplot(211)
    ax_down = plt.subplot(212)
    name_list = []
    colors = ["mediumseagreen", "orangered"]
    index_names = list(dataframe.columns)
    stats_list = index_names[-2:]
    for n in index_names:
        if n.startswith(to_analysis):
            test_name = n.split("_")[-1]
            name_list.append(test_name)
            ax_up.plot(dataframe['Camera_Frame_ID'], dataframe[n])
    for idx, m_s in enumerate(stats_list):
        ax_down.plot(dataframe['Camera_Frame_ID'], dataframe[m_s], color=colors[idx])
    plt.xlabel('Camera_Frame_ID', color='b', fontsize=15)
    plt.ylabel(to_analysis, color='b', fontsize=15, y=1.7)
    plt.axhline(y=threshold, ls=":", c="purple")

    x_axis_max = plt.axis()[1]
    ax_down.text(x_axis_max+50, threshold, "Abnormal data:\nstd >= {:.5f}".format(threshold), fontsize=12, color="b", bbox=dict(facecolor='white', alpha=0.5))
    ax_up.legend(name_list, loc=1, fontsize=12)
    ax_down.legend(stats_list, loc=1, fontsize=12)
    square_bracket = to_analysis.find("[")
    if square_bracket != -1:
        file_name = to_analysis[:square_bracket]
        plt.savefig(os.path.join(os.path.abspath(save_path), file_name + "-TestStatsLineFig" + ".png"))
    else:
        plt.savefig(os.path.join(os.path.abspath(save_path), to_analysis + "-TestStatsLineFig" + ".png"))
    # plt.show()

