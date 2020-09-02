"""
Function: process raw data excel (original and Reinjection data), generate aligned pandas dataframes, perform mean and standard deviation operation on Reinjection data, and can filter out potential problematic data (those with large std)
Author: Xinran Wang
Date: 09/02/2020
"""

# suggested folder structures:
# ├──
# ├── original data folder
# ---├── original mf4 1
# ---├── original mf4 2
# ---└── ...
# ├── test data 1 folder
# ---├── test 1 mf4 1
# ---├── test 1 mf4 2
# ---└── ...
# ├── test data 2 folder
# ---├── test 2 mf4 1
# ---├── test 2 mf4 2
# ---└── ...
# └── ...


import pandas as pd
import xlrd
import os
import asammdf
from asammdf import MDF
import glob
import time
from process_data import *
from plot import *
import sys

sys.setrecursionlimit(100000)
pd.set_option('display.max_columns', 8)
pd.set_option('expand_frame_repr', False)


def search_dir(directory):
    """
    In the given directory, find original data and Reinjection data, and output their names
    :param directory: the absolute path of the folder storing all the excel files
    :return: a dictionary containing keys as the data name (original, test file No.), value as a list of files in this folder
    example output: {"original": [o_dir1, o_dir2, ...], "test1": [t1_dir1, t1_dir2, ...], "test2": [...], ...}
    """
    folders = os.listdir(directory)

    files = {}

    for f in folders:
        if "original" in f.lower():
            files["original"] = list_file_path(os.path.join(directory, f))
        else:
            files[f] = list_file_path(os.path.join(directory, f))

    return files


def list_file_path(path):
    """
    If the directory has the structure listed below, then can use this function to further extract files
        ├──
        ├── original data folder
        ---├── original mf4 1
        ---├── original mf4 2
        ---└── ...
        ├── test data 1 folder
        ---├── test 1 mf4 1
        ---├── test 1 mf4 2
        ---└── ...
        ├── test data 2 folder
        ---├── test 2 mf4 1
        ---├── test 2 mf4 2
        ---└── ...
        └── ...
    :param path: the path of one folder (ex: original data folder, test data folder...)
    :return: a list containing all the mf4 files' paths in this folder
    """
    file_paths = []
    files = os.listdir(path)
    for f in files:
        if f.endswith(".mf4"):
            file_paths.append(os.path.join(path, f))
    return file_paths


def generate_wanted_signal(signal_path):
    """
    From the excel containing wanted signals, extract the signals with top priority and split them by enumerate and value types
    :param signal_path: the path of the signals excel file
    :return: a tuple with 2 lists containing respectively enumerated signal and value signal, and the string name of camera id
    """
    start_time = time.time()

    signals = pd.read_excel(signal_path)
    first_priority = signals[(signals["Priority"] == 1) & (signals["Alignment"] == "Agree")]
    camera_id = "Camera_ID"
    for i in first_priority["Name"]:
        if "camera" in i.lower() and "id" in i.lower():
            camera_id = i
            break
    enum_list = list(first_priority[first_priority["Value Table"] == "Enumeration"]["Name"])
    val_list = list(first_priority[first_priority["Value Table"] == "None"]["Name"])
    enum_list.append(camera_id)
    end_time = time.time()
    print("Time spent on extracting wanted signals from excel: " + str(end_time - start_time) + " seconds")

    return enum_list, val_list, camera_id


def load_mf4_to_dic_for_all(data_path_dic, dbc, total_wanted):
    """
    Based on the given dictionary containing all data files' paths, extract all the dictionary-form data using loadMF4data2Dict
    :param data_path_dic: the directory of data file
    :param dbc: the total_fullpath variable generated from load_total_matrix
    :param total_wanted: the wanted signals for extracting data
    :return: a dictionary containing keys as the data name (original, test file No.), value as a list of dictionaries, each dictionary contains the data of one file in this folder
    example output: {"original": [{Idx1: df1, Idx2: df2, ...}, {Idx1': df1', Idx2': df2', ...}, ...], "test1": [{Idx1: df1, Idx2: df2, ...}, {Idx1': df1', Idx2': df2', ...}, ...], "test2": [...], ...}
    """
    data_dic = {}
    for k in data_path_dic:
        data_dic[k] = []
        for p in data_path_dic[k]:
            data_dic[k].append(loadMF4data2Dict(p, total_wanted, dbc))
    return data_dic


def merge_one_type_data(data_dictionary, to_analysis, cam_id_name):
    """
    Based on the given signal to analysis, generate the full dataframe
    :param data_dictionary: the dictionary with key as the data folders' names and the value as a list of dictionaries, each dictionary holding the data for one file in that data folder
    :param to_analysis: the signal to analysis
    :param cam_id_name: a string representing the name of the camera id's name in the data columns
    :return: the merged dataframe, and the list containing all the test data's names (for further detection of the existence of test data)
    """
    ori_name_list = [cam_id_name]
    test_name_list = []

    # first generate the dataframe for original data
    to_analysis_ori = [i[to_analysis] for i in data_dictionary["original"] if i[to_analysis] is not None]
    if len(to_analysis_ori) == 0:
        merged = pd.DataFrame()
    elif len(to_analysis_ori) > 1:
        merged = pd.concat(to_analysis_ori)
    else:
        merged = to_analysis_ori[0]
        
    cam_id_ori = [o[cam_id_name] for o in data_dictionary["original"] if o[cam_id_name] is not None]
    if len(cam_id_ori) == 0:
        original_cam_id = pd.DataFrame()
    elif len(cam_id_ori) > 1:
        original_cam_id = pd.concat(cam_id_ori)
    else:
        original_cam_id = cam_id_ori[0]
    merged = original_cam_id.join(merged, how="outer")
    # fill the NAs in the original cam ids and delete all the NAs in the signal data accordingly
    merged[cam_id_name].fillna(method="ffill", inplace=True)
    merged[cam_id_name].fillna(method="bfill", inplace=True)
    # merged.fillna(method="ffill", inplace=True)
    # merged.fillna(method="bfill", inplace=True)
    merged = merged.dropna()
    merged = remove_dup(merged, cam_id_name)

    if merged.shape[1] > 1:
        ori_name_list.append(to_analysis + "_original")

    # then generate the dataframe for test data
    for k in sorted(list(data_dictionary.keys())):
        if k != "original":
            to_analysis_test = [d[to_analysis] for d in data_dictionary[k] if d[to_analysis] is not None]
            cam_id_test = [d[cam_id_name] for d in data_dictionary[k] if d[cam_id_name] is not None]
            if len(to_analysis_test) == 0:
                dataframe = pd.DataFrame()
            elif len(to_analysis_test) > 1:
                dataframe = pd.concat(to_analysis_test)
            else:
                dataframe = to_analysis_test[0]

            if len(cam_id_test) == 0:
                cam_id_test_total = pd.DataFrame()
            elif len(cam_id_test) > 1:
                cam_id_test_total = pd.concat(cam_id_test)
            else:
                cam_id_test_total = cam_id_test[0]
            dataframe = cam_id_test_total.join(dataframe, how="outer")
            # after joining, fill in all the missing cam ids for current test case, and remove duplicated cam ids' data
            dataframe[cam_id_name].fillna(method="ffill", inplace=True)
            dataframe[cam_id_name].fillna(method="bfill", inplace=True)
            dataframe = dataframe.dropna()
            dataframe = remove_dup(dataframe, cam_id_name)

            # merge the dataframe to the result, and fill in all the missing values in other data columns
            merged = pd.merge(merged, dataframe, on=cam_id_name, how="outer")
            merged = merged.sort_values(by=cam_id_name)
            merged = merged.reset_index(drop=True)
            merged.fillna(method="ffill", inplace=True)
            merged.fillna(method="bfill", inplace=True)
            # merged = remove_dup(merged, cam_id_name)

            if dataframe.shape[1] > 1:
                test_name_list.append(to_analysis + "_" + k)

    merged.columns = ori_name_list + test_name_list
    merged = drop_zero_and_na(merged, cam_id_name)

    return merged, test_name_list


def remove_dup(dataframe, cam_id_name):
    """
    Drop rows that have duplicated camera id (keep the first duplicated camera id data)
    :param dataframe: a pandas dataframe
    :return: a pandas dataframe after removing duplicates
    """
    dataframe.drop_duplicates(subset=cam_id_name, inplace=True)
    return dataframe


# below is further data analysis and calculation methods

def drop_zero_and_na(dataframe, camera_id_name):
    """
    Drop columns that include NaN or camera id is 0
    :param dataframe: a pandas dataframe (after combining original dataframe with Reinjection dataframes)
    :param camera_id_name: a string representing the name of the camera id's name in the data columns
    :return: a pandas dataframe after dropping
    """
    new = dataframe.reset_index(drop=True)
    new = new.drop(dataframe[dataframe[camera_id_name] == 0].index)
    new = new.dropna()
    new = new.reset_index(drop=True)
    return new


def generate_stats(dataframe, test_name_list):
    """
    Generate the test data's mean and std data from the given dataframe
    :param dataframe: the dataframe containing all the merged data
    :param test_name_list: a list containing test data's names (it is used to detect whether test data exists for this signal)
    :return: a new dataframe with mean and std added, and a boolean flag value of whether there is test data for
    """
    if len(test_name_list) > 0:
        flag = True
        dataframe["test_mean"] = dataframe[test_name_list].mean(axis=1)
        dataframe["test_std"] = dataframe[test_name_list].std(axis=1)
    else:
        flag = False
    return dataframe, flag


def large_std_cam_id(dataframe, cam_id_name, percentile=0.95):
    """
    Pick out the camera ids that have too large std values (larger than some pre-set percentile lower bound)
    :param dataframe: a pandas dataframe that has gone through merge_and_calculate operation
    :param cam_id_name: a string representing the name of the cam id parameter
    :param percentile: the percentile of the std lower bound
    :return: a list containing numbers representing the camera ids of potential abnormal points, and a float number representing the lower bound of abnormal std
    """
    # set the default lower bound to 95% largest data std
    std_lower_bound = dataframe["test_std"].describe((1-percentile, percentile))[str(int(percentile*100))+"%"]
    filt = (dataframe["test_std"] >= std_lower_bound)
    return list(dataframe[filt][cam_id_name].astype(int)), std_lower_bound


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
    path = "C:\\Users\\Z0050908\\Desktop\\hil_test\\"
    dbc_path = "C:\\Users\\Z0050908\\Downloads\\"
    signal = "C:\\Users\\Z0050908\\Desktop\\FR-IFC-Private CAN_Checklist.xlsx"

    e, v, cam = generate_wanted_signal(signal)

    dbcs = {"Ch3": ['GWM V71 CAN 01C.dbc'], "Ch4": ['FR-IFC-Private CAN.dbc'], "Ch5": ['GWM V71 CAN 01C.dbc'], "Ch6": ['FR-IFC-Private CAN.dbc']}
    A, B, C = load_total_matrix(dbc_path, dbcs)

    dic = search_dir(path)
    print(dic)
    data_dic = load_mf4_to_dic_for_all(dic, A, e+v)

    df, tn = merge_one_type_data(data_dic, 'IFC_obj01_Dx', cam)
    # val: IFC_obj01_Dx
    # enum: BridgeDistance
    # none: FS_Out_Of_Calib
    df_stat, changed = generate_stats(df, tn)

    print(df_stat.head(50))
    print(df_stat.tail(50))

    abnormal, lower = large_std_cam_id(df_stat, cam, 0.95)
    plot_data_and_stats_with_outliers(df_stat, dbc_path, changed, "IFC_obj01_Dx", cam, lower)
    # plot_data_and_stats(df_stat, dbc_path, changed, "IFC_obj01_Dx", cam)
