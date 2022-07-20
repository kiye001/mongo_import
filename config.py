# coding: utf-8
"""
config.py
Config Class

kiye - 07/2022
"""
import os
import json

class config:
    """
    Class to get the content of json file
    """
    def __init__(self,dir_path,file_name):
        self.dir_path = dir_path
        self.file_name = file_name
        self.content = self._get_config()

    def _get_config(self):
        """
        Get content of json file
        return None or dict
        """
        local_path = os.path.join(self.dir_path, self.file_name)
        try:
            with open(local_path) as file_found:
                return json.load(file_found)
        except FileNotFoundError:
            print(f"{local_path} not available")
            return None
        except json.JSONDecodeError:
            print(f"JSON file {local_path} is faulty")
            return None
        except:
            print("Something went wrong")
            return None