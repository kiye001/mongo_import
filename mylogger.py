# coding: utf-8
"""
mylogger.py
Class to handle log
kiye - 07/2022
"""
import logging as log

class MyLogger:
    """
    Class to handle log
    """
    def __init__(self,log_name,log_level,log_formatter):
        self.log_name = log_name
        self.log_level = log_level
        self.log_formatter = log_formatter
        self.logger = log.getLogger(self.log_name)
    
    def set_logs(self):
        """
        Init logger that will be used through this script
        """
        logger = self.logger
        logger.setLevel(int(self.log_level))        
        stream_handler = log.StreamHandler()
        stream_handler.setLevel(int(self.log_level))
        stream_handler.setFormatter(log.Formatter(self.log_formatter))
        logger.addHandler(stream_handler)
        self.logger = logger