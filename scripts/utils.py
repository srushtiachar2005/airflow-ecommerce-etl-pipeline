import os
from datetime import datetime

def get_project_root():
    return os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

def get_today_folder():
    return datetime.now().strftime("%Y-%m-%d")

def build_path(*args):
    return os.path.join(get_project_root(), *args)

def ensure_dir(path):
    os.makedirs(os.path.dirname(path), exist_ok=True)