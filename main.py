from data_operation import *
from plot import *
from ppt import *
from infra import read_config

conf = read_config("conf.yaml")

data_dir = conf["path"]["path_data_dir"]
dbc_dir = conf["path"]["path_dbc_dir"]
signal_excel = conf["path"]["path_signal_excel"]
folder_path = conf["path"]["path_to_create_folder"]

signal_enum, signal_val, cam_id_name = generate_wanted_signal(signal_excel)

folder_name = data_dir.strip("\\").split("\\")[-1] + "_HIL_Report"

ori, test_enum, test_val = generate_dataframe(data_dir, dbc_dir, signal_enum, signal_val, cam_id_name)





figure_path = create_folder(folder_path, folder_name)

for i in test_enum:
    print("Processing: " + i)
    test_df = merge_and_calculate(df_array, i)
    plot_tests(test_df, figure_path, i)
for j in test_val:
    print("Processing: " + j)
    square_bracket = j.find("[")
    if square_bracket != -1:
        file_name = j[:square_bracket]
    else:
        file_name = j
    test_df_s = merge_and_calculate(df_array, j)
    outlier_list, threshold = large_std_cam_id(test_df_s, 0.95)
    plot_tests_and_stats_with_outliers(test_df_s, figure_path, j, threshold)

    cam_id_interval = convert_to_interval(outlier_list)
    abnormals[file_name] = cam_id_interval

generate_ppt(figure_path, abnormal_lists)
