from data_operation import *
from plot import *
from ppt import *
from infra import read_config

conf = read_config("conf.yaml")

directory = conf["path"]["path_data_excel"]
folder_path = conf["path"]["path_to_create_folder"]
folder_name = conf["path"]["folder_name"]
plot_data = conf["plot_data"]
plot_data_and_stats = conf["plot_data_and_stats"]

df_array = generate_dataframe(directory)
figure_path = create_folder(folder_path, folder_name)
abnormals = {}

for i in plot_data:
    print("Processing: " + i)
    test_df = merge_and_calculate(df_array, i)
    plot_tests(test_df, figure_path, i)
for j in plot_data_and_stats:
    print("Processing: " + j)
    square_bracket = j.find("[")
    if square_bracket != -1:
        file_name = j[:square_bracket]
    else:
        file_name = j
    test_df_s = merge_and_calculate(df_array, j)
    # plot_tests_and_stats(test_df_s, figure_path, j)

    outlier_list, threshold = large_std_cam_id(test_df_s, 0.95)
    plot_tests_and_stats_with_outliers(test_df_s, figure_path, j, threshold)

    cam_id_interval = convert_to_interval(outlier_list)
    abnormals[file_name] = cam_id_interval

generate_ppt(figure_path, abnormals, folder_path)
