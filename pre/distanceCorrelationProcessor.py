import numpy as np
import scipy.spatial as spat
import scipy.optimize as opt
from scipy.interpolate import interp1d
import matplotlib
import matplotlib.pyplot as plt

import sqlite3
c = sqlite3.connect("pb2a_monitor.db-20200814")
#has one table: "pb2a_monitor"

try:
    c.execute("DROP TABLE correlations")
except:
    print("tried to delete tables which don't exist")

c.execute("CREATE TABLE correlations ( firstDataSet VARCHAR(255), secondDataSet VARCHAR(255), distanceCorrelation REAL )")
#note: Rows are type Tuples

cursor = c.execute("PRAGMA table_info(pb2a_monitor)")
for first_row in cursor:
    first_column_num = first_row[0]
    first_column_name = first_row[1]
    for second_row in cursor:
        second_column_num = second_row[0]
        if second_column_num > first_column_num:

            second_column_name = second_row[1]
            first_set, second_set = [], []
            for temp_row in c.execute("SELECT * FROM pb2a_monitor"):
                first_point = temp_row[first_column_num]
                second_point = temp_row[second_column_num]
                if first_point is not None and second_point is not None:
                    first_set.append(first_point)
                    second_set.append(second_point)
            #print(first_set)
            #print(second_set)

            #machine learning part
            distCorr = spat.distance.correlation(first_set, second_set)

            print(first_column_name, second_column_name, distCorr)
            try:
                c.execute("INSERT INTO correlations VALUES ({0}, {1}, {2})".format("\'" + first_column_name + "\'", "\'" + second_column_name + "\'", distCorr))
            except:
                print("error inserting into correlations the value")

c.execute("PRAGMA table_info(correlations)")
for this_row in c.execute("SELECT * from correlations"):
    print(this_row)
