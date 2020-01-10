"""
Convert the downloaded L1 CARISMA data into L2 data

The l1_to_l2 function expects the folder structure outlined in the repository
README.

"""
import sys
import os
import glob
import numpy as np
import pandas as pd
from pathlib import Path
from calendar import monthrange


def l1_to_l2(station_name, year):
    # This will return '../fdl18-sw1' as the path
    root_folder = Path(os.getcwd()).parent.parent 

    # Setting up directory structure
    data_root = os.path.join(root_folder, 'data')
    l1_data = os.path.join(data_root, 'level1')
    station_folder = os.path.join(l1_data, station_name)
    this_year_folder = os.path.join(station_folder, str(year))
    
    if not os.path.exists(this_year_folder):
        print("Path " + this_year_folder + " does not exist!")
        print("Download data first using download_carisma_data.py")
        sys.exit(1) # Exit the program
        

    # Pre allocating arrays for efficiency
    ndays = 0 
    
    months = sorted(glob.glob(os.path.join(this_year_folder, '*')))
    for month in months:
        days = sorted(glob.glob(os.path.join(month, '*')))
        ndays + = len(days)
        
    
