import asammdf
from asammdf import MDF
import glob
import pandas as pd

def load_dbc(dbc_file_dir):
    dbc = glob.glob(dbc_file_dir + "*.dbc")
    return dbc

def read_mf4(file_path, dbc):
    mdf = MDF(file_path, "r")
    information = mdf.extract_can_logging(dbc)
    data = information.to_dataframe()
    return data

def extract_wanted_signal_data(dataframe, signal_excel_path):
    signals = pd.read_excel(signal_excel_path)
    names = list(signals["Name"])
    return dataframe.loc[:, names]

if __name__ == "__main__":
    path = "C:\\Users\\Z0050908\\Documents\\Reinj_data\\Raw data\\GWM_TimeSycn_142_2020_07_11_070917_log_001.mf4"

    dbc_path = "C:\\Users\\Z0050908\\Downloads\\"
    signal = "C:\\Users\\Z0050908\\Desktop\\FR-IFC-Private CAN_Checklist.xlsx"

    print(pd.read_excel(signal))
    # dbc = load_dbc(dbc_path)
    # data = read_mf4(path, dbc)
    # print(data.shape)
    # print(extract_wanted_signal_data(data, signal))
