import numpy as np
import scipy.spatial as spat
import scipy.optimize as opt
from scipy.interpolate import interp1d
import matplotlib
import matplotlib.pyplot as plt
import pandas as pd

import sqlite3
c = sqlite3.connect("pb2a_monitor.db-20200814")
#has one table: "pb2a_monitor"

cursor = c.execute("PRAGMA table_info(pb2a_monitor)")
columns = []
i = 0
for row in cursor:
    row_name = row[1]
    columns.append([this_column[0] for this_column in c.execute("SELECT {} FROM pb2a_monitor".format(row_name))])
    print(i)
    i += 1
print("Loading names is complete")

corr_matrix = []
for first_column in columns:
    corr_row = []
    for second_column in columns:
        data = {0: first_column, 1: second_column}
        temp_df = pd.DataFrame(data)
        # remove the rows of temp_df containing nulls
        temp_df = temp_df.dropna()
        corr = temp_df.corr().iat[0,1]
        corr_row.append(corr)
    corr_matrix.append(corr_row)

f = open("correlations.txt", "a")
f.write(str(corr_matrix))
f.close()

### openning the file





### below does not account for None data types

#c.execute("DROP TABLE temp")


#corr_dataframe = pd.DataFrame()
#
#cursor = c.execute("PRAGMA table_info(pb2a_monitor)")
#for row in cursor:
#    this_set = []
#    for point in c.execute("SELECT * FROM pb2a_monitor"):
#        point_value = point[row[0]]
#        this_set.append(point_value)
#
#    corr_dataframe[row[1]] = np.array(this_set)
#    print(corr_dataframe)
#
#print(corr_dataframe.corr())
