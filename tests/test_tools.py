"""
test_tools.py
tools module test

kiye - 07/2022
"""
import tools as T
import mylogger as LOG

# OK
dir_path = '.\\data'
file_name = 'test.xlsx'
log = LOG.MyLogger('test','10','%(asctime)s :: %(levelname)s :: %(message)s')
excel_file_path = f"{dir_path}/{file_name}"
res = T.convert_excel_to_dict(log.logger,excel_file_path)
expected = {'0': {'col1': 'hello', 'col2': 'world', 'col3': None},\
     '1': {'col1': 'How ', 'col2': 'are', 'col3': 'you'}}

def test_convert_excel_to_dict():
    assert res == expected

# OK empty excel file
dir_path = '.\\data'
file_name = 'test_empty.xlsx'
log = LOG.MyLogger('test','10','%(asctime)s :: %(levelname)s :: %(message)s')
excel_file_path = f"{dir_path}/{file_name}"
res = T.convert_excel_to_dict(log.logger,excel_file_path)
expected = {}

def test_convert_excel_to_dict_excel_empty():
    assert res == expected

# KO no logger defined
log = None
excel_file_path = None
res = T.convert_excel_to_dict(log,excel_file_path)
expected = None

def test_convert_excel_to_dict_log_ko():
    assert res == expected

# KO no excel file
dir_path = '.\\data'
file_name = 'null.xlsx'
log = LOG.MyLogger('test','10','%(asctime)s :: %(levelname)s :: %(message)s')
excel_file_path = f"{dir_path}/{file_name}"
res = T.convert_excel_to_dict(log,excel_file_path)
expected = None

def test_convert_excel_to_dict_excel_ko():
    assert res == expected