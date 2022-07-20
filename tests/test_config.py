"""
test_config.py
Config Class test

kiye - 07/2022
"""
import config as C

# OK
dir_path = '.\\conf'
file_name = 'app_test.json'
res = {'app': {'app_name': 'mongo_import',
  'log_level': '10',
  'formatter': '%(asctime)s :: %(levelname)s :: %(message)s',
  'log_file_path': 'TO_DEFINE'},
 'mongo': {'url': 'TO_DEFINE', 'database': 'MY_DB', 'collection': 'MY_COLL'},
 'spark': {'app_name': 'Mongo Import'}}
my_conf = C.config(dir_path,file_name)

def test_conf_dir_path():
    assert my_conf.dir_path == dir_path

def test_conf_file_name():
    assert my_conf.file_name == file_name

def test_conf_content():
    assert my_conf.content == res

# KO -> bad file path
dir_path_err = '.\\conf1'
file_name_err = 'app_test.json'
res_err = None
my_conf_err = C.config(dir_path_err,file_name_err)

def test_conf_dir_path_KO():
    assert my_conf_err.dir_path == dir_path_err

def test_conf_file_name_KO():
    assert my_conf_err.file_name == file_name_err

def test_conf_content_KO():
    assert my_conf_err.content == res_err

# KO -> bad file structure
dir_path_bad = '.\\conf'
file_name_bad = 'app_bad.json'
res_bad = None
my_conf_bad = C.config(dir_path_bad,file_name_bad)

def test_conf_dir_path_bad():
    assert my_conf_bad.dir_path == dir_path_bad

def test_conf_file_name_bad():
    assert my_conf_bad.file_name == file_name_bad

def test_conf_content_bad():
    assert my_conf_bad.content == res_bad
