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
import os
from os import listdir
from os.path import isfile, join

from astropy.time import Time
from datetime import datetime

class database_table():
    # tn is name of table within db file. dir is full path to db file including file name
    def __init__(self, dir, tn):
        self.dir = dir
        self.file_cursor = sqlite3.connect(dir)
        self.name = tn
        split_dir = dir.split('/')
        self.file_name = split_dir[len(split_dir) - 1]

    def gen_table_info(self):
        yield from self.file_cursor.execute("PRAGMA table_info({})".format(self.name))

    def gen_table(self):
        yield from self.file_cursor.execute("SELECT * FROM {}".format(self.name))

    def print_table_info(self):
        for i in self.gen_table_info():
            print(i)

    def print_file_info(self):
        for i in self.file_cursor.execute("SELECT name FROM sqlite_master WHERE type='table';"):
            print(i)

    def print_head(self, limit):
        for i in self.file_cursor.execute("SELECT * FROM {} LIMIT {};".format(self.name, limit)):
            print(i)

    def get_column_index(self, col_name):
        for i in self.gen_table_info():
            if i[1] == col_name:
                return i[0]
        return None

    def gen_column_names(self):
        for row in self.gen_table_info():
            yield row[1]

    def copy(self):
        # overides any file with directory/name new_dir
        new_dir = self.dir[:len(self.dir) - 3] + "_temp.db"
        try:
            os.remove(new_dir)
        except:
            pass
        copyfile(self.dir, new_dir)
        new_object = database_table(new_dir, self.name)
        return new_object

    def close(self):
        self.file_cursor.close()

    def size(self):
        count = 0
        for i in self.gen_table():
            count += 1
        return count

    def commit(self):
        self.file_cursor.commit()

    def print(self):
        print("database table info: | dir: " + self.dir + " | name: " + self.name + " | file_name: " + self.file_name)

    def print_runids(self):
        print(self.name + " runid info ---")
        runids = []
        for line in self.gen_table():
            runids.append(line[0])
        print(runids)
        print("max runid: " + str(max(runids)))
        print("number of runids: " + str(len(runids)))
