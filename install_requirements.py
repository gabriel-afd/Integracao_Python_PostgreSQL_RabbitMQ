import subprocess
import sys

required_packages = [
    'pika',
    'pandas',
    'psycopg2-binary'
]

for package in required_packages:
    try:
        __import__(package.split('-')[0])
    except ImportError:
        print(f"[INFO] Instalando: {package}")
        subprocess.check_call([sys.executable, "-m", "pip", "install", package])
