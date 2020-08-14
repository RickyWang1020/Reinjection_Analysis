import asammdf
from asammdf import MDF
import glob

path = "C:\\Users\\Z0050908\Documents\\Reinj_data\\Raw data\\GWM_TimeSycn_142_2020_07_11_070917_log_001.mf4"

dbc_path = "C:\\Users\\Z0050908\\Downloads\\"

mdf = MDF(path, "r")
dbc = glob.glob(dbc_path + "*.dbc")

information = mdf.extract_can_logging(dbc)
print(information.to_dataframe())

data = information.get_can_signal('EyeQ_Frame_ID_HIL')

print(data)
