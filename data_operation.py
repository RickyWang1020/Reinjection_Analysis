"""
Function: process raw data excel (original and Reinjection data), generate aligned pandas dataframes, perform mean and standard deviation operation on Reinjection data, and can filter out potential problematic data (those with large std)
Author: Xinran Wang
Date: 07/22/2020
"""

import pandas as pd
import xlrd
import os

def search_dir(directory):
    """
    In the given directory, find original data and Reinjection data, and output their names
    :param directory: the absolute path of the folder storing all the excel files
    :return: a tuple, original_file is a string of original file's name (if no such file exists, it will be None), test_file is a list containing the strings of names of Reinjection files
    """
    files = os.listdir(directory)

    original_file = None
    test_file = []

    for f in files:
        if f.endswith(".XLS"):
            if f.startswith("Reinjection"):
                test_file.append(f)
            elif f.startswith("Original"):
                original_file = f

    return original_file, test_file

def generate_file_path(directory, name):
    """
    Generates the path of a file
    :param directory: the absolute path of the folder storing all the excel files
    :param name: the name of one file
    :return: a string of the file's absolute path
    """
    return directory + "\\" + name

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

def load_and_concat_test_data(path):
    """
    Load one Reinjection data excel and concat all the sheets to one pandas dataframe
    :param path: the absolute path of one Reinjection data excel
    :return: a pandas dataframe that all the sheets of one Reinjection data excel are concatenated in
    """
    dataframe = pd.DataFrame()
    old_header_list = ["t[s]",
                       "EYEQDG_CMN_Params_s.COM_Cam_Frame_ID_b32[]",
                       "EYEQDG_CMN_Params_s.COM_EyeQ_Frame_ID_b32[]",
                       "EYEQDG_OBJT_Params_s.EYEQDG_OBJTvO_Params_as._0_.OBJ_Lat_Distance_b12[]",
                       "EYEQDG_CMN_Params_s.COM_Sync_Frame_ID_b8[]",
                       "EYEQDG_OBJT_Params_s.EYEQDG_OBJTvH_Params_s.OBJ_Sync_ID_b8[]",
                       "EYEQDG_OBJT_Params_s.EYEQDG_OBJTvH_Params_s.OBJ_VD_CIPV_ID_b8[]",
                       "EYEQDG_OBJT_Params_s.EYEQDG_OBJTvH_Params_s.OBJ_VD_CIPV_Lost_b2[]",
                       "EYEQDG_OBJT_Params_s.EYEQDG_OBJTvO_Params_as._0_.OBJ_ID_b8[]",
                       "EYEQDG_OBJT_Params_s.EYEQDG_OBJTvO_Params_as._0_.OBJ_Relative_Long_Velocity_b13[]",
                       "EYEQDG_OBJT_Params_s.EYEQDG_OBJTvO_Params_as._0_.OBJ_Long_Distance_b14[]"]
    header_list = ['T', 'Cam_id', 'EQ_id', 'Lat_D', 'Com_Sync_id', 'OBJ_Sync_id', 'OBJ_CIPV_ID', 'OBJ_CIPV_Lost',
                   'OBJ_id', 'OBJ_Re_Long_V', 'OBJ_Long_D']
    data = xlrd.open_workbook(path)
    # note down the name of the last sheet because we need to delete the final 8 rows that are not numeric data
    final_sheet_name = data.sheets()[-1].name

    for sheet in data.sheets():
        name = sheet.name
        print("Processing:", name)

        if name == final_sheet_name:
            df = pd.read_excel(path, names=header_list, header=None, sheet_name=name, skipfooter=8)
        else:
            df = pd.read_excel(path, names=header_list, header=None, sheet_name=name)

        # not sure whether which sheet will include the header row, so use this function to check
        if check_first_row(df, old_header_list):
            df = df.drop(0)

        dataframe = pd.concat([dataframe, df], ignore_index=True, sort=False)

    return dataframe

def shift_columns(dataframe):
    """
    Shift the columns of dataframes to make one group of data align to one row
    :param dataframe: a pandas dataframe
    :return: a pandas dataframe after shifting columns
    """
    dataframe_copy = dataframe.copy()
    for index, col in enumerate(dataframe.columns[2:]):
        dataframe_copy[col] = dataframe_copy[col].shift(index+1)
    return dataframe_copy

def dropnan(dataframe):
    """
    Drop all the rows in the dataframe that has NaN camera id (the process after shift_columns)
    :param dataframe: a pandas dataframe
    :return: a pandas dataframe after dropping
    """
    return dataframe.dropna(subset = ['Cam_id'])

def remove_dup(dataframe):
    """
    Drop rows that have duplicated camera id (keep the first duplicated camera id data)
    :param dataframe: a pandas dataframe
    :return: a pandas dataframe after removing duplicates
    """
    dataframe.drop_duplicates(subset='Cam_id', inplace=True)
    return dataframe

def remove_outlier(dataframe, outlier_range, to_detect = 'OBJ_Long_D'):
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
    original_name, test_names = search_dir(directory)
    test_df_array = []
    original_path = generate_file_path(directory, original_name)
    original_df = remove_dup(dropnan(shift_columns(load_and_concat_original_data(original_path))))
    # -200 is for Reinjection purpose
    original_df = remove_outlier(original_df, [-200])
    original_df = remove_outlier(original_df, [-100], "Lat_D")
    original_df = remove_outlier(original_df, [0], "Cam_id")

    for n in test_names:
        path = generate_file_path(directory, n)
        test_df = remove_dup(dropnan(shift_columns(load_and_concat_test_data(path))))
        # -200 is for Reinjection purpose
        test_df = remove_outlier(test_df, [-200])
        test_df = remove_outlier(test_df, [0], "Cam_id")
        test_df_array.append(test_df)
    return original_df, test_df_array


# below is further data analysis and calculation

def drop_zero_and_na(dataframe):
    """
    Drop columns that include NaN or camera id is 0
    :param dataframe: a pandas dataframe (after combining original dataframe with Reinjection dataframes)
    :return: a pandas dataframe after dropping
    """
    new = dataframe.reset_index(drop=True)
    new = new.drop(dataframe[dataframe["Cam_id"] == 0].index)
    new = new.dropna()
    return new

def merge_and_calculate(original, test_list, to_analysis):
    """
    Call this function to merge original data and Reinjection data into one dataframe by the type of value to analysis, generate mean and std values of Reinjection data and add to the end of merged dataframe
    :param original: a pandas dataframe containing all data in original data excel
    :param test_list: a list containing several pandas dataframes, each dataframe holds data of corresponding Reinjection data excel
    :param to_analysis: a string corresponding to the column on the dataframes, indicating the data to look into
    :return:
    """
    original_selected = original.loc[:, ["Cam_id", to_analysis]]
    test_selected = []
    for df in test_list:
        data = df.loc[:, ["Cam_id", to_analysis]]
        test_selected.append(data)
    merged = pd.merge(original_selected, test_selected[0], on = "Cam_id", how = "outer")
    for index in range(1, len(test_selected)):
        merged = pd.merge(merged, test_selected[index], on = "Cam_id", how = "outer")
    name_list = [to_analysis + "_test" + str(j+1) for j in range(len(test_selected))]
    merged.columns = ["Cam_id", "Original"] + name_list
    merged = drop_zero_and_na(merged)
    # print(merged["Cam_id"].min())
    merged = merged.reset_index(drop=True)
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
    # print(dataframe)
    # print(dataframe["test_std"])
    # print(dataframe["test_std"].max())
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

