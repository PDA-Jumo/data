#! /usr/bin/env python
import subprocess

scripts = [
    'kosdaq_data.py',
    'kospi_data.py',
    'nasdaq_data.py',
    'sp500_data.py'
]

for script in scripts:
    # subprocess.run(['python', script])
    subprocess.run(['python3', script], cwd='/mnt/c/Users/프로디지털S006/Desktop/Jumo/data/')

    print("Success Run")