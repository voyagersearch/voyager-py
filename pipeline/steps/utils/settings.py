import os

base_path = os.path.dirname(__file__)
LOG_FILE_PATH = os.path.join(base_path, 'logs')
if not os.path.exists(LOG_FILE_PATH):
    os.makedirs(LOG_FILE_PATH)
