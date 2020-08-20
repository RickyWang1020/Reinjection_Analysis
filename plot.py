"""
Function: plot solely original data, solely Reinjection data, a combination of original data, Reinjection mean and Reinjection std
Author: Xinran Wang
Date: 07/22/2020
"""

import matplotlib.pyplot as plt
import os

def create_folder(directory, name):
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
    :param to_analysis: a string corresponding to the column on the dataframe, indicating the data to look into
    :return: None
    """
    fig = plt.figure(figsize=(20,8),dpi=80)
    ax = plt.subplot(111)
    plt.plot(dataframe['Cam_id'], dataframe[to_analysis], color = 'r')
    plt.title("Changes in Original Data's " + to_analysis + " as a function of Camera ID", color = 'b', fontsize = 18)
    plt.xlabel('Cam_id', color = 'b', fontsize = 15)
    plt.ylabel(to_analysis, color = 'b', fontsize = 15)
    plt.savefig(os.path.join(os.path.abspath(save_path), str(index) + "-1-" + to_analysis + "-OriginalFig" + ".png"))
    #plt.show()

def plot_tests(df_array, save_path, index, to_analysis):
    """
    Plot all the Reinjection data based on the given data column and save the figure to the given path
    :param df_array: a list containing several pandas dataframes, each dataframe holds data of corresponding Reinjection data excel
    :param save_path: a string representing the path where the output figure should locate in
    :param to_analysis: a string corresponding to the column on the dataframe, indicating the data to look into
    :return: None
    """
    fig = plt.figure(figsize=(20,8),dpi=80)
    ax = plt.subplot(111)
    name_list = []
    colors = ["r", "g", "b", "y", "c"]
    for i in range(len(df_array)):
        df = df_array[i]
        name_list.append("Test" + str(i+1))
        plt.plot(df['Cam_id'], df[to_analysis], color = colors[i])
    plt.title("Changes in Test Data's " + to_analysis + " as a function of Camera ID", color='b', fontsize=18)
    plt.xlabel('Cam_id', color = 'b', fontsize = 15)
    plt.ylabel(to_analysis, color = 'b', fontsize = 15)
    ax.legend(name_list, loc=1, fontsize=12)
    plt.savefig(os.path.join(os.path.abspath(save_path), str(index) + "-2-" + to_analysis + "-TestFig" + ".png"))
    #plt.show()

def plot_statistics(dataframe, save_path, index, to_analysis, include_original = True, include_mean = True, include_std = True):
    """
    Plot the original data, Reinjection data's mean and/or std in one figure and save the figure to the given path
    :param dataframe: the already-merged dataframe after merge_and_calculate process based on a specific data column
    :param save_path: a string representing the path where the output figure should locate in
    :param to_analysis: a string corresponding to the column on the dataframe, indicating the data to look into
    :param include_original: a boolean value of whether original data should appear in this figure
    :param include_mean: a boolean value of whether Reinjection data's mean should appear in this figure
    :param include_std: a boolean value of whether Reinjection data's std should appear in this figure
    :return:
    """
    fig = plt.figure(figsize=(20,8),dpi=80)
    ax = plt.subplot(111)
    legend_list = []
    if include_original:
        ax.plot(dataframe["Cam_id"], dataframe["Original"], color = "deepskyblue")
        legend_list.append('Original_data')
    if include_mean:
        ax.plot(dataframe["Cam_id"], dataframe["test_mean"], color = "forestgreen")
        legend_list.append('Test_mean')
    if include_std:
        ax.plot(dataframe["Cam_id"], dataframe["test_std"], color = "orange")
        legend_list.append('Test_std')
    plt.title("Changes in " + to_analysis + "'s std and mean of Test Data and " + to_analysis + " of Original Data as a function of Camera ID", color='b', fontsize=18)
    plt.xlabel('Cam_id', color='b', fontsize=15)
    plt.ylabel(to_analysis, color='b', fontsize=15)
    ax.legend(legend_list, loc=1, fontsize=12)
    plt.savefig(os.path.join(os.path.abspath(save_path), str(index) + "-3-" + to_analysis + "-Merged_" + "_".join(legend_list) + ".png"))
    #plt.show()