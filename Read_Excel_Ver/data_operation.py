"""
Function: process raw data excel (original and Reinjection data), generate aligned pandas dataframes, perform mean and standard deviation operation on Reinjection data, and can filter out potential problematic data (those with large std)
Author: Xinran Wang
Date: 08/31/2020
"""

import pandas as pd
import xlrd
import os
from plot import *

pd.set_option('display.max_columns',13)
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_columns',None)

def search_dir(directory):
    """
    In the given directory, find original data and Reinjection data, and output their names
    :param directory: the absolute path of the folder storing all the excel files
    :return: a list containing the strings of names of Reinjection test files
    """
    files = os.listdir(directory)

    test_file = []

    for f in files:
        if f.endswith(".csv"):
            if "With_FrameID" in f:
                test_file.append(f)

    return test_file

def generate_file_path(directory, name):
    """
    Generates the path of a file
    :param directory: the absolute path of the folder storing all the excel files
    :param name: the name of one file
    :return: a string of the file's absolute path
    """
    return os.path.join(directory, name)

def load_and_concat_data(path):
    """
    Load the original data excel and concat all the sheets to one pandas dataframe
    :param path: the absolute path of the original data excel
    :return: a pandas dataframe that all the sheets of the original data excel are concatenated in
    """
    header_list = ["t[s]", "Camera_Frame_ID", "EyeQ_Frame_ID", "Line01_HeadingAngle[rad]", "Line01_Dy[m]",\
                   "Line01_Curv[1/m]", "Line02_HeadingAngle[rad]", "Line02_Dy[m]", "Line02_Curv[1/m]",\
                   "obj01_abs_Ax[m/s^2]", "obj01_abs_Ay[m/s^2]", "obj01_Rel_Vx[m/s]", "obj01_Rel_Vy[m/s]",\
                   "obj01_Dx[m]", "obj01_Dy[m]", "obj01_Width[m]", "obj01_Heading[rad]", "obj01_Type"]

    dataframe = pd.read_csv(path)
    dataframe.columns = header_list

    return dataframe

def shift_columns(dataframe):
    """
    Shift the columns of dataframes to make one group of data align to one row (used to process the old version of messy column data)
    :param dataframe: a pandas dataframe
    :return: a pandas dataframe after shifting columns
    """
    dataframe_copy = dataframe.copy()
    for col in dataframe.columns[3:9]:
        dataframe_copy[col] = dataframe_copy[col].shift(1)
    return dataframe_copy

def fill_na_and_remove_dup(dataframe):
    """
    Fill NAs with bfill method and remove duplicated data
    :param dataframe: a pandas dataframe
    :return: the processed new dataframe
    """
    new_dataframe = dataframe.fillna(method='bfill')
    new_dataframe.drop_duplicates(subset='Camera_Frame_ID', keep="last", inplace=True)
    new_dataframe = new_dataframe.reset_index(drop=True)
    return new_dataframe

# subject to change
def remove_outlier(dataframe, outlier_range, to_detect):
    """
    Remove the outlier of a specific column of dataframe
    :param dataframe: a pandas dataframe
    :param outlier_range: a list containing the values that are view as outliers
    :param to_detect: a string corresponding to the column on the dataframe, indicating the data to look into
    :return: a pandas dataframe after removing outliers
    """
    filt = (dataframe[to_detect].isin(outlier_range)) # currently set to an array of outlier values
    after_remove = dataframe.drop(dataframe[filt].index)
    after_remove = after_remove.reset_index(drop=True)
    return after_remove

def generate_dataframe(directory):
    """
    Call this function to read data from directory and get the processed original and Reinjection dataframes
    :param directory: the absolute path of the folder storing all the excel files
    :return: a tuple, original_df is a pandas dataframe of original data, test_df_array is a list containing dataframes corresponding to every Reinjection data excel files
    """
    test_names = search_dir(directory)
    test_df_array = []

    for n in test_names:
        path = generate_file_path(directory, n)
        test_df = fill_na_and_remove_dup(shift_columns(load_and_concat_data(path)))
        # -200 is for Reinjection purpose
        # test_df = remove_outlier(test_df, [-200])
        # test_df = remove_outlier(test_df, [0], "Cam_id")
        test_df_array.append(test_df)
    return test_df_array


# below is further data analysis and calculation

def drop_zero_and_na(dataframe):
    """
    Drop columns that include NaN or camera id is 0
    :param dataframe: a pandas dataframe (after combining original dataframe with Reinjection dataframes)
    :return: a pandas dataframe after dropping
    """
    new = dataframe.reset_index(drop=True)
    new = new.drop(dataframe[dataframe["Camera_Frame_ID"] == 0].index)
    new = new.dropna()
    new = new.reset_index(drop=True)
    return new

def merge_and_calculate(test_list, to_analysis):
    """
    Call this function to merge original data and Reinjection data into one dataframe by the type of value to analysis, generate mean and std values of Reinjection data and add to the end of merged dataframe
    :param test_list: a list containing several pandas dataframes, each dataframe holds data of corresponding Reinjection data excel
    :param to_analysis: a string corresponding to the column on the dataframes, indicating the data to look into
    :return: a dataframe containing merged test data of a specific data value type
    """
    # stores all the test case's corresponding columns with respect to to_analysis
    test_selected = []

    for df in test_list:
        data = df.loc[:, ["Camera_Frame_ID", to_analysis]]
        test_selected.append(data)
    merged = test_selected[0]
    for index in range(1, len(test_selected)):
        merged = pd.merge(merged, test_selected[index], on="Camera_Frame_ID", how="outer")
    name_list = [to_analysis + "_test" + str(j+1) for j in range(len(test_selected))]
    merged.columns = ["Camera_Frame_ID"] + name_list

    merged = drop_zero_and_na(merged)
    merged["test_mean"] = merged[name_list].mean(axis=1)
    merged["test_std"] = merged[name_list].std(axis=1)

    return merged

def large_std_cam_id(dataframe, percentile=0.95):
    """
    Pick out the camera ids that have too large std values (larger than some pre-set percentile lower bound)
    :param dataframe: a pandas dataframe that has gone through merge_and_calculate operation
    :param percentile: the percentile of the std lower bound
    :return: a list containing numbers representing the camera ids of potential abnormal points
    """
    # currently, set the lower bound to 95% largest data std
    std_lower_bound = dataframe["test_std"].describe((1-percentile, percentile))[str(int(percentile*100))+"%"]
    filt = (dataframe["test_std"] >= std_lower_bound)
    return list(dataframe[filt]["Camera_Frame_ID"].astype(int)), std_lower_bound

def convert_to_interval(id_array):
    """
    Convert some consecutive timestamps' id to some intervals for easier retrieval
    :param id_array: a list containing all the camera ids of outliers
    :return: a list of strings representing the abnormal camera id ranges
    """
    interval = []
    current_interval = [id_array[0]]
    digit = id_array[0] // 100
    for i in range(1, len(id_array)):
        now = id_array[i]
        if now // 100 == digit:
            current_interval.append(now)
        elif now // 100 == digit + 1:
            current_interval.append(now)
            digit += 1
        else:
            if current_interval[-1] - current_interval[0] >= 5:
                interval.append(str(current_interval[0]) + "-" + str(current_interval[-1]))
            current_interval = [now]
            digit = now // 100
    if current_interval[-1] - current_interval[0] >= 5:
        interval.append(str(current_interval[0]) + "-" + str(current_interval[-1]))
    return interval

if __name__ == "__main__":
    dirs = search_dir("C:\\Users\\Z0050908\\Desktop\\to_analysis")
    dir = "C:\\Users\\Z0050908\\Desktop\\to_analysis"
    df_array = generate_dataframe(dir)
    test_df = merge_and_calculate(df_array, "obj01_Dx[m]")
    #print(test_df.iloc[:,1])
    print(test_df.head(50))
    # # plot_tests(test_df, "C:\\Users\\Z0050908\\Desktop", "obj01_Dx[m]")

    #print(large_std_cam_id(test_df, 7.99))
    print(test_df["test_std"].describe((0.05, 0.95))["95%"])
    x, y = large_std_cam_id(test_df, 0.95)
    plot_tests_and_stats_with_outliers(test_df, "C:\\Users\\Z0050908\\Desktop", "obj01_Dx[m]", y)
    print(x)
    print(convert_to_interval(x))