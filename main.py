from data_operation import *
from ppt import *
from infra import read_config

conf = read_config("conf.yaml")

data_dir = conf["path"]["path_data_dir"]
dbc_dir = conf["path"]["path_dbc_dir"]
signal_excel = conf["path"]["path_signal_excel"]
folder_path = conf["path"]["path_to_create_folder"]
ppt_path = conf["path"]["path_to_create_ppt"]

dbcs = conf["dbc_channels"]

signal_enum, signal_val, cam_id_name = generate_wanted_signal(signal_excel)
total_fpath, total_msg, total_signal = load_total_matrix(dbc_dir, dbcs)

folder_name = data_dir.strip("\\").split("\\")[-1] + "_HIL_Report"
ppt_name = folder_name

data_directory_dic = search_dir(data_dir)
data_dic = load_mf4_to_dic_for_all(data_directory_dic, total_fpath, signal_enum + signal_val)

figure_path = create_folder(folder_path, folder_name)

abnormals = {}

for i in signal_enum:
    if i != cam_id_name:
        print("Processing: " + i)
        test_df, _ = merge_one_type_data(data_dic, i, cam_id_name)
        plot_ori_and_test(test_df, figure_path, i, cam_id_name)
for j in signal_val:
    if j != cam_id_name:
        print("Processing: " + j)
        test_df, testcase_name_list = merge_one_type_data(data_dic, j, cam_id_name)
        test_df_s, changed = generate_stats(test_df, testcase_name_list)

        outlier_list, std_threshold = large_std_cam_id(test_df_s, cam_id_name, 0.95)
        cam_id_interval = convert_to_interval(outlier_list)
        abnormals[j] = cam_id_interval

        plot_data_and_stats_with_outliers(test_df_s, figure_path, changed, j, cam_id_name, std_threshold)

generate_ppt(figure_path, abnormals, ppt_path, ppt_name)
