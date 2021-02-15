import numpy as np
import scipy.spatial as spat
import scipy.optimize as opt
from scipy.interpolate import interp1d
from scipy.misc import derivative
import scipy.stats as st
import matplotlib
import matplotlib.pyplot as plt
import math
from functools import partial
from inspect import signature
from shutil import copyfile

import sqlite3
from os import listdir
from os.path import isfile, join

from astropy.time import Time
from datetime import datetime

from database_table import *

class database_file():

    def __init__(self, dir):
        # dir is full relative directory to file includoing file's name
        self.database_tables = []
        self.dir = dir
        self.file_cursor = sqlite3.connect(dir)

        table_names = []
        for i in self.file_cursor.execute("SELECT name FROM sqlite_master WHERE type='table';"):
            table_names.append(i[0])

        for name in table_names:
            self.database_tables.append(database_table(self.dir, name))

    def print_table_names(self):
        names = []
        for table in self.database_tables:
            names.append(table.name)
        print(names)

    def print_table_infos(self):
        print("\nFile dir: " + self.dir)
        for table in self.database_tables:
            print("\n" + table.name)
            table.print_table_info()
