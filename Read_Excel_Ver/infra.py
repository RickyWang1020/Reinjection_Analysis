import yaml
import os
import json

def read_config(file_path):
    with open(file_path, "r") as y_file:
        config_file = yaml.load(y_file, Loader=yaml.FullLoader)
        # try:
        #     config = config_file[read_config]
        # except:
        #     print("Error when handling", read_config)
        #     config = None
        return config_file


def read_json(file_path):
    with open(file_path, "r") as j_file:
        data = json.load(j_file)
    return data[0]
