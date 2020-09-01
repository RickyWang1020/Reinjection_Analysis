"""
Function: process raw data excel (original and Reinjection data), generate aligned pandas dataframes, perform mean and standard deviation operation on Reinjection data, and can filter out potential problematic data (those with large std)
Author: Xinran Wang
Date: 08/26/2020
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


pd.set_option('display.max_columns', 8)
pd.set_option('expand_frame_repr', False)


def search_dir(directory):
    """
    In the given directory, find original data and Reinjection data, and output their names
    :param directory: the absolute path of the folder storing all the excel files
    :return: a dictionary containing keys as the data name (original, test file No.), value as a list of files in this folder
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
    enum_list.append(camera_id)
    val_list = list(first_priority[first_priority["Value Table"] == "None"]["Name"])

    end_time = time.time()
    print("Time spent on extracting wanted signals from excel: " + str(end_time - start_time) + " seconds")

    return enum_list, val_list, camera_id


def read_all_and_combine(dir_list, dbc, wanted_enum, wanted_val, cam_id_name):

    start_time = time.time()

    enum_list = []
    val_list = []
    for directory in dir_list:
        raw = load_one_test_data(dbc, directory)
        enum_df = remove_dup(dropnan(raw.reindex(columns=wanted_enum)), cam_id_name)
        val_df = remove_dup(dropnan(raw.reindex(columns=wanted_val)), cam_id_name)
        enum_list.append(enum_df)
        val_list.append(val_df)
    enum = pd.concat(enum_list, ignore_index=True)
    val = pd.concat(val_list, ignore_index=True)

    end_time = time.time()
    print("Time spent on generating the dataframe for " + str(dir_list) + " is: " + str(end_time - start_time) + " seconds")

    return enum, val


def load_mf4_to_dic_for_all(data_path_dic, dbc, total_wanted):
    data_dic = {}
    for k in data_path_dic:
        data_dic[k] = []
        for p in data_path_dic[k]:
            data_dic[k].append(loadMF4data2Dict(p, total_wanted, dbc))
    return data_dic


def merge_one_type_data(data_dictionary, to_analysis, cam_id_name):
    
    # first generate the dataframe for original data
    to_analysis_ori = [i[to_analysis] for i in data_dictionary["original"]]
    if len(to_analysis_ori) > 1:
        merged = pd.concat(to_analysis_ori)
    else:
        merged = to_analysis_ori[0]
    cam_id_ori = [o[cam_id_name] for o in data_dictionary["original"]]
    if len(cam_id_ori) > 1:
        original_cam_id = pd.concat(cam_id_ori)
    else:
        original_cam_id = cam_id_ori[0]
    merged = original_cam_id.join(merged, how="outer")
    
    name_list = [cam_id_name, to_analysis + "_original"]

    # then generate the dataframe for test data
    for k in sorted(list(data_dictionary.keys())):
        if k != "original":
            to_analysis_test = [d[to_analysis] for d in data_dictionary[k]]
            cam_id_test = [d[cam_id_name] for d in data_dictionary[k]]
            if len(to_analysis_test) > 1:
                dataframe = pd.concat(to_analysis_test)
            else:
                dataframe = to_analysis_test[0]
            if len(cam_id_test) > 1:
                cam_id_test_total = pd.concat(cam_id_test)
            else:
                cam_id_test_total = cam_id_test[0]
            dataframe = cam_id_test_total.join(dataframe, how="outer")
            merged = pd.merge(merged, dataframe, on=cam_id_name, how="outer")
            merged.fillna(method="bfill", inplace=True)
            merged.fillna(method="ffill", inplace=True)
            merged = remove_dup(merged, cam_id_name)
            print(merged)
            name_list.append(to_analysis + "_" + k)

    merged.columns = name_list

    return merged


# def dropnan(dataframe):
#     """
#     Drop all the rows in the dataframe that has NaN camera id (the process after shift_columns)
#     :param dataframe: a pandas dataframe
#     :return: a pandas dataframe after dropping
#     """
#     return dataframe.dropna(subset = ['Camera_Frame_ID_HIL'])
#


def remove_dup(dataframe, cam_id_name):
    """
    Drop rows that have duplicated camera id (keep the first duplicated camera id data)
    :param dataframe: a pandas dataframe
    :return: a pandas dataframe after removing duplicates
    """
    dataframe.drop_duplicates(inplace=True)
    return dataframe


def generate_dataframe(directory, dbc_file_dir, dbc_channels, enum_list, val_list, cam_id_name):

    start_time = time.time()

    total_fullpath, total_messages, total_signals = load_total_matrix(dbc_file_dir, dbc_channels)

    # ori_dir, t_dir = search_dir(directory)
    #
    # original_enum, original_val = read_all_and_combine(list_file_path(ori_dir), dbc, enum_list, val_list, cam_id_name)
    # test_df_enum_arr = []
    # test_df_val_arr = []

    ori_files, t_files = search_dir(directory)

    original_enum, original_val = read_all_and_combine(ori_files, dbc, enum_list, val_list, cam_id_name)

    for t in t_files:
        test_e, test_v = read_all_and_combine(t, dbc, enum_list, val_list, cam_id_name)
        test_df_enum_arr.append(test_e)
        test_df_val_arr.append(test_v)

    end_time = time.time()
    print("Total time spent on generating original and test dataframes from mf4 files: " + str(end_time - start_time) + " seconds")

    return original_enum, original_val, test_df_enum_arr, test_df_val_arr


# below is further data analysis and calculation

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


def merge_and_calculate(original, test_list, to_analysis, cam_id_name):

    merged = original.loc[:, [cam_id_name, to_analysis]]
    test_selected = []
    for df in test_list:
        data = df.loc[:, [cam_id_name, to_analysis]]
        test_selected.append(data)
    for idx in range(len(test_selected)):
        merged = pd.merge(merged, test_selected[idx], on=cam_id_name, how="outer")
    name_list = [to_analysis + "_test" + str(j+1) for j in range(len(test_selected))]
    merged.columns = [cam_id_name, "Original"] + name_list
    merged = drop_zero_and_na(merged, cam_id_name)

    merged["test_mean"] = merged[name_list].mean(axis=1)
    merged["test_std"] = merged[name_list].std(axis=1)
    return merged

def large_std_cam_id(dataframe, std_lower_bound):
    """
    Pick out the camera ids that have too large std values (larger than some pre-set lower bound)
    :param dataframe: a pandas dataframe that has gone through merge_and_calculate operation
    :param std_lower_bound: the lower bound of "too large std", any std over this value will be considered as a potential abnormal point
    :return: a list containing numbers representing the camera ids of potential abnormal points
    """
    filt = (dataframe["test_std"] >= std_lower_bound)
    return list(dataframe[filt]["Cam_id"])

def covert_cam_id_to_time(original, cam_id):
    """
    Convert the camera ids to the corresponding frame timestamps in the original dataframe
    :param original: the original dataframe
    :param cam_id: a list of numbers
    :return: a pandas series of numbers, index is the camera id, value is the corresponding timestamp in original data
    """
    filt = (original["Cam_id"].isin(cam_id))
    filtered_t = original[filt]["T"]
    return filtered_t

def convert_t_to_interval(time_and_id):
    """
    (This is not called in the main function, subject to change.) Convert some consecutive timestamps to a time interval for easier time-retrieval
    :param time_and_id: a pandas series, index is camera id, value is the corresponding timestamp
    :return: a list of strings, each string is either a time range, or a single timestamp
    """
    interval = []
    indices = time_and_id.index
    current = 0
    start = indices[current]
    length = 1
    while True:
        try:
            end = indices[current + 1]
            if end != start + length:
                length = 1
                if start == indices[current]:
                    interval.append(str(time_and_id[start]))
                else:
                    interval.append(str(time_and_id[start])+"-"+str(time_and_id[indices[current]]))
                current += 1
                start = indices[current]
            else:
                length += 1
                current += 1
        except:
            if length != 1:
                interval.append(str(time_and_id[start])+"-"+str(time_and_id[end]))
            else:
                interval.append(str(time_and_id[end]))
            break
    return interval

if __name__ == "__main__":
    path = "C:\\Users\\Z0050908\\Desktop\\hil_test\\"
    dbc_path = "C:\\Users\\Z0050908\\Downloads\\"
    signal = "C:\\Users\\Z0050908\\Desktop\\FR-IFC-Private CAN_Checklist.xlsx"

    e, v, cam = generate_wanted_signal(signal)
    # ori, enum, val = generate_dataframe(path, dbc_path, e, v, cam)
    #
    # print(merge_and_calculate(ori, val, "BridgeDistance"))

    # dbc = glob.glob(dbc_path + "*.dbc")
    #
    # ori_dir, t_dir = search_dir(path)
    # print(list_file_path(ori_dir))
    # for t in t_dir:
    #     file_paths = list_file_path(t)
    #     print(read_all_and_combine(file_paths, dbc, e, v, cam))

    # o_e, o_v, t_e, t_v = generate_dataframe(path, dbc_path, e, v, cam)
    # print(o_e)
    # print(o_v)
    # print(t_e)
    # print(t_v)
    dbcs = {"Ch3": ['GWM V71 CAN 01C.dbc'], "Ch4": ['FR-IFC-Private CAN.dbc'], "Ch5": ['GWM V71 CAN 01C.dbc'], "Ch6": ['FR-IFC-Private CAN.dbc']}
    A, B, C = load_total_matrix(dbc_path, dbcs)

    dic = search_dir(path)
    print(dic)
    data_dic = load_mf4_to_dic_for_all(dic, A, e+v)

    df = merge_one_type_data(data_dic, "IFC_obj01_Dx", cam)
    # IFC_obj01_Dx
    print(df.head(50))
