"""
Function: process raw data excel (original and Reinjection data), generate aligned pandas dataframes, perform mean and standard deviation operation on Reinjection data, and can filter out potential problematic data (those with large std)
Author: Xinran Wang
Date: 08/26/2020
"""

import pandas as pd
import xlrd
import os
import asammdf
from asammdf import MDF
import glob

import time

pd.set_option('display.max_columns', 8)
pd.set_option('expand_frame_repr', False)

def search_dir(directory):
    """
    In the given directory, find original data and Reinjection data, and output their names
    :param directory: the absolute path of the folder storing all the excel files
    :return: a tuple, original_file is a string of original file's name (if no such file exists, it will be None), test_file is a list containing the strings of names of Reinjection files
    """
    folders = os.listdir(directory)

    original_dir = None
    test_dir = []

    for d in folders:
        if "original" in d:
            original_dir = os.path.join(directory, d)
        else:
            test_dir.append(os.path.join(directory, d))

    # original_file = None
    # test_file = []
    #
    # for f in files:
    #     if f.endswith(".mf4"):
    #         # original file name not sure...
    #         if f.startswith("Original"):
    #             original_file = f
    #         else:
    #             test_file.append(f)

    return original_dir, test_dir

def list_file_path(path):
    file_paths = []
    files = os.listdir(path)
    for f in files:
        if f.endswith(".mf4"):
            file_paths.append(os.path.join(path, f))
    return file_paths

def generate_file_path(directory, name):

    return os.path.join(directory, name)

def generate_wanted_signal(signal_path):

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
    for dir in dir_list:
        raw = load_one_test_data(dbc, dir)
        enum_df = remove_dup(dropnan(raw.reindex(columns=wanted_enum)), cam_id_name)
        val_df = remove_dup(dropnan(raw.reindex(columns=wanted_val)), cam_id_name)
        enum_list.append(enum_df)
        val_list.append(val_df)
    enum = pd.concat(enum_list, ignore_index=True)
    val = pd.concat(val_list, ignore_index=True)

    end_time = time.time()
    print("Time spent on generating the dataframe for " + str(dir_list) + " is: " + str(end_time - start_time) + " seconds")

    return enum, val


def load_and_concat_original_data(path):
    """
    Load the original data excel and concat all the sheets to one pandas dataframe
    :param path: the absolute path of the original data excel
    :return: a pandas dataframe that all the sheets of the original data excel are concatenated in
    """
    dataframe = pd.DataFrame()
    data = xlrd.open_workbook(path)
    header_list = ['T', 'Cam_id', 'EQ_id', 'Lat_D', 'Com_Sync_id', 'OBJ_Sync_id', 'OBJ_CIPV_ID', 'OBJ_CIPV_Lost',
                   'OBJ_id', 'OBJ_Re_Long_V', 'OBJ_Long_D']
    for sheet in data.sheets():
        name = sheet.name
        print("Processing:", name)

        # are you sure that every sheet of the original data has a header???
        # if not, need to resemble the code in load_and_concat_test_data
        df = pd.read_excel(path, names=header_list, sheet_name=name)
        dataframe = pd.concat([dataframe, df], ignore_index=True)
    return dataframe

def check_first_row(dataframe, header_list):
    """
    Check whether the first row of the dataframe is the header (which means we should reset the header of the dataframe)
    :param dataframe: a pandas dataframe
    :param header_list: the list containing the header names of dataframe's columns
    :return: a boolean value: True means the first row of the dataframe is the same as header
    """
    return list(dataframe.loc[0]) == header_list

def load_one_test_data(dbc, data_path):
    """
    Load one Reinjection data excel and concat all the sheets to one pandas dataframe
    :param path: the absolute path of one Reinjection data excel
    :return: a pandas dataframe that all the sheets of one Reinjection data excel are concatenated in
    """
    mdf = MDF(data_path, "r")
    information = mdf.extract_can_logging(dbc)
    dataframe = information.to_dataframe()

    return dataframe

def dropnan(dataframe):
    """
    Drop all the rows in the dataframe that has NaN camera id (the process after shift_columns)
    :param dataframe: a pandas dataframe
    :return: a pandas dataframe after dropping
    """
    return dataframe.dropna(subset = ['Camera_Frame_ID_HIL'])

def remove_dup(dataframe, cam_id_name):
    """
    Drop rows that have duplicated camera id (keep the first duplicated camera id data)
    :param dataframe: a pandas dataframe
    :return: a pandas dataframe after removing duplicates
    """
    dataframe.drop_duplicates(subset=cam_id_name, inplace=True)
    return dataframe

def generate_dataframe(directory, dbc_file_dir, enum_list, val_list, cam_id_name):

    start_time = time.time()

    dbc = glob.glob(dbc_file_dir + "*.dbc")

    ori_dir, t_dir = search_dir(directory)

    original_enum, original_val = read_all_and_combine(list_file_path(ori_dir), dbc, enum_list, val_list, cam_id_name)
    test_df_enum_arr = []
    test_df_val_arr = []

    for t in t_dir:
        file_paths = list_file_path(t)
        test_e, test_v = read_all_and_combine(file_paths, dbc, enum_list, val_list, cam_id_name)
        test_df_enum_arr.append(test_e)
        test_df_val_arr.append(test_v)

    end_time = time.time()
    print("Total time spent on generating original and test dataframes from mf4 files: " + str(end_time - start_time) + " seconds")

    return original_enum, original_val, test_df_enum_arr, test_df_val_arr


# below is further data analysis and calculation

def drop_zero_and_na(dataframe, cam_id):
    """
    Drop columns that include NaN or camera id is 0
    :param dataframe: a pandas dataframe (after combining original dataframe with Reinjection dataframes)
    :return: a pandas dataframe after dropping
    """
    new = dataframe.reset_index(drop=True)
    new = new.drop(dataframe[dataframe[cam_id] == 0].index)
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
    """
    ['C:\\Users\\Z0050908\\Desktop\\hil_test\\original\\GWM_TimeSycn_142_2020_07_11_070917_log_001.mf4', 'C:\\Users\\Z0050908\\Desktop\\hil_test\\original\\GWM_TimeSycn_142_2020_07_11_070917_log_002.mf4', 'C:\\Users\\Z0050908\\Desktop\\hil_test\\original\\GWM_TimeSycn_142_2020_07_11_070917_log_003.mf4']
    ['C:\\Users\\Z0050908\\Desktop\\hil_test\\test1\\GWM_TimeSycn_142_2020_07_11_070917_log_test_001.mf4', 'C:\\Users\\Z0050908\\Desktop\\hil_test\\test1\\GWM_TimeSycn_142_2020_07_11_070917_log_test_002.mf4', 'C:\\Users\\Z0050908\\Desktop\\hil_test\\test1\\GWM_TimeSycn_142_2020_07_11_070917_log_test_003.mf4']
    ['C:\\Users\\Z0050908\\Desktop\\hil_test\\test2\\GWM_TimeSycn_142_2020_07_11_070917_log_test_001.mf4', 'C:\\Users\\Z0050908\\Desktop\\hil_test\\test2\\GWM_TimeSycn_142_2020_07_11_070917_log_test_002.mf4', 'C:\\Users\\Z0050908\\Desktop\\hil_test\\test2\\GWM_TimeSycn_142_2020_07_11_070917_log_test_003.mf4']
    """

    o_e, o_v, t_e, t_v = generate_dataframe(path, dbc_path, e, v, cam)
    print(o_e)
    print(o_v)
    print(t_e)
    print(t_v)

# the structure of the to-analysis directory:
#     |
#     -- original data folder
#     ------ original mf4 1
#     ------ original mf4 2
#     ------ ...
#     -- test data 1 folder
#     ------ test 1 mf4 1
#     ------ test 1 mf4 2
#     ------ ...
#     -- test data 2 folder
#     ------ test 2 mf4 1
#     ------ test 2 mf4 2
#     ------ ...
#     -- ...
