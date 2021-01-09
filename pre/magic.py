import numpy as np
import scipy.spatial as spat
import scipy.optimize as opt
from scipy.interpolate import interp1d
import matplotlib
import matplotlib.pyplot as plt

import sqlite3
from os import listdir
from os.path import isfile, join

files = [f for f in listdir(mypath) if isfile(join(mypath, f))]

cursors = []
for file in files:
    cursors.append(sqlite.connect(str(file)))


c = sqlite3.connect("pb2a_monitor.db-20200814")
#has one table: "pb2a_monitor"


for i in c.execute("PRAGMA table_info(pb2a_monitor)"):
    print(i)

c.execute("DROP TABLE temp")
c.execute("CREATE TABLE temp AS SELECT run_id, slowdaq_Outside_Temperature_mean FROM pb2a_monitor")

#note: Rows are type Tuples

#inputting data
run_id, slowdaq_Outside_Temperature_mean = [], []
for this_row in c.execute("SELECT * from temp"):
    print(this_row)
    run_id.append(this_row[0])
    slowdaq_Outside_Temperature_mean.append(this_row[1])



plt.xlim(20000034, 21200379)
plt.ylim(-10, 12)
plt.scatter(run_id, slowdaq_Outside_Temperature_mean, label = "Data")
#plt.plot(x, fit_y(x), label = "Fit")
plt.legend()
plt.title("test plot")
plt.xlabel("slowdaq_Outside_Temperature_mean")
plt.ylabel("run_id")
plt.show()
