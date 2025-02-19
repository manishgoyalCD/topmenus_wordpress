from datetime import date
import logging
import os

class ErrorLogger:
    def __init__(self, log_file_name="", log_to_terminal=False, log_to_file=False):
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        
        # Create formatter
        formatter = logging.Formatter("%(asctime)s %(levelname)s: %(message)s", datefmt="%F %A %T")

        # Optionally create and add file handler
        if log_to_file:
            # Check if logs directory exists or not
            if not os.path.exists('logs'):
                os.makedirs('logs')
            
            log_file = f'logs/{date.today()}{"-" + log_file_name if log_file_name else ""}.log'
            file_handler = logging.FileHandler(log_file, encoding='utf-8', mode='a+')
            file_handler.setLevel(logging.INFO)
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)
        
        # Optionally create and add console handler
        if log_to_terminal:
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.INFO)
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)

    def error(self, msg='ERROR'):
        self.logger.error(msg)
        
    def warning(self, msg='WARNING'):
        self.logger.warning(msg)
        
    def warn(self, msg='WARNING'):
        self.logger.warning(msg)

    def debug(self, msg='DEBUG'):
        self.logger.debug(msg)
    
    def exception(self, msg='ERROR'):
        self.logger.exception(msg)

    def info(self, msg='INFO'):
        self.logger.info(msg)

if __name__ == '__main__':
    # Usage
    logger = ErrorLogger('app', log_to_terminal=True, log_to_file=False)
    logger.info('This is an info message')
    logger.error('This is an error message')
    logger.warning('This is a warning message')
    logger.debug('This is a debug message')
    logger.exception('This is an exception message')









# from datetime import date
# import logging
# import os

# class ErrorLogger:
#     def __init__(self, log_file_name="", log_to_terminal=False):
#         # Check if logs directory exists or not
#         if not os.path.exists('logs'):
#             os.makedirs('logs')
        
#         log_file = f'logs/{date.today()}{"-" + log_file_name if log_file_name else ""}.log'
        
#         # Create a logger object
#         self.logger = logging.getLogger(__name__)
#         self.logger.setLevel(logging.INFO)
        
#         # Create file handler
#         file_handler = logging.FileHandler(log_file, encoding='utf-8', mode='a+')
#         file_handler.setLevel(logging.INFO)
        
#         # Create formatter and add it to the file handler
#         formatter = logging.Formatter("%(asctime)s %(levelname)s: %(message)s", datefmt="%F %A %T")
#         file_handler.setFormatter(formatter)
        
#         # Add the file handler to the logger
#         self.logger.addHandler(file_handler)
        
#         # Optionally create and add console handler
#         if log_to_terminal:
#             console_handler = logging.StreamHandler()
#             console_handler.setLevel(logging.INFO)
#             console_handler.setFormatter(formatter)
#             self.logger.addHandler(console_handler)

#     def error(self, msg='ERROR'):
#         self.logger.error(msg)
        
#     def warning(self, msg='WARNING'):
#         self.logger.warning(msg)
        
#     def warn(self, msg='WARNING'):
#         self.logger.warning(msg)

#     def debug(self, msg='DEBUG'):
#         self.logger.debug(msg)
    
#     def exception(self, msg='ERROR'):
#         self.logger.exception(msg)

#     def info(self, msg='INFO'):
#         self.logger.info(msg)

# if __name__ == '__main__':
#     # Usage
#     logger = ErrorLogger('app', log_to_terminal=True)
#     logger.info('This is an info message')
#     logger.error('This is an error message')
#     logger.warning('This is a warning message')
#     logger.debug('This is a debug message')
#     logger.exception('This is an exception message')