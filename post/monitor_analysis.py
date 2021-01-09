import numpy as np
import scipy.spatial as spat
import scipy.optimize as opt
from scipy.interpolate import interp1d
import scipy.stats as stats
import matplotlib
import matplotlib.pyplot as plt
import math

import monitor_sql as msql

import sqlite3
from os import listdir
from os.path import isfile, join

from astropy.time import Time
from datetime import datetime

# this file only contains runner code

# Three epochs: 20000000, 20100000, 21200000

path = "../databases/pb2a-20200301"  # 301, 818
run_id = msql.database_table(path, "pb2a_runid.db", "pb2a_runid")
monitor = msql.database_table(path, "pb2a_monitor.db", "pb2a_monitor")
apex = msql.database_table(path, "pb2a_apex.db", "pb2a_APEX")
linear = lambda x, m, b: m * x + b
zero_to_one = Time(datetime(2019, 3, 8, 15, 25, 7, 706722)).mjd
one_to_twelve = Time(datetime(2020, 1, 30, 16, 21, 40, 910587)).mjd
partitions = [20000000, 20100000, 21200000, 30000000]

# rules as boolean lambda functions.
# take in j and k as tuples representing the data of one column of the database
# j[1] and k[1] are the names of the column as string
#outside stuff: ("UV" in j[1] or "Solar" in j[1] or "Wind" in j[1] or "Outside" in j[1])
rules = [
    lambda j, k: "mean" in j[1] and "mean" in k[1],
    lambda j, k: "MD_ARM" in j[1]
]
def gen_data_sets(db_1, db_2=None, rules=None):
    if rules is None:
        rules = []
    if db_2 is None:
        acc_db_2 = db_1
    for i in db_1.gen_table_info():
        for j in acc_db_2.gen_table_info():
            if i[0] < j[0]:
                rule_broken = False
                for rule in rules:
                    if not rule(i, j):
                        rule_broken = True
                        break
                if not rule_broken:
                    yield msql.data_set(db_1, i[1], j[1], y_db_table=db_2, xcol_err=i[1][:len(i[1])-4]+"std", ycol_err=i[1][:len(i[1])-4]+"std")

def plot_comber(db_1, db_2=None, rules=None, runid_partitions=partitions, discard_error=None):
    for set in gen_data_sets(db_1, db_2, rules):
        if discard_error is not None:
            set.discard_large_error_points(discard_error)
        set.make_plot(errorbars=True, runid_partitions=partitions)


x_col = "slowdaq_Iceboard0076_MB_PHY_Temp_mean"
y_col = "slowdaq_Iceboard0127_MB_PHY_Temp_mean"
x_col_err = "slowdaq_Iceboard0076_MB_PHY_Temp_std"
y_col_err = "slowdaq_Iceboard0127_MB_PHY_Temp_std"
centerLeft_APEXtemp = msql.data_set(monitor, x_col, y_col, xcol_err=x_col_err, ycol_err=y_col_err)
centerLeft_APEXtemp.make_plot(errorbars=True, runid_partitions=partitions, fit=linear)


exit()

cols = []
for i in monitor.gen_table_info():
    if "Mezz_2_Temp_mean" in i[1]:
        cols.append(i[1])
temps = []
for j in range(len(cols)):
    temps.append([])
for row in monitor.gen_table():
    addRow = True
    thisrow = []
    for i in range(len(cols)):
        element = row[monitor.get_column_index(cols[i])]
        if element is None:
            addRow = False
            break
        else:
            thisrow.append(element)
    if addRow:
        for j in range(len(temps)):
            temps[j].append(thisrow[j])
averages = []
for i in range(len(temps[0])):
    sum = 0
    points = 0
    for j in range(len(temps) - 1):
        sum += temps[j][i]
        points += 1
    averages.append(sum / points)
assert len(averages) == len(temps[0])
for i in range(len(temps)):
    plt.scatter(averages, temps[i], s=12)
x_space = np.linspace(min(averages), max(averages), 200)
plt.plot(x_space, x_space, label = "Linear fit: y = x", linewidth=1)
plt.xlabel("Average Mezz_2 temperature")
plt.ylabel("Mezz_2 temperature of specific iceboard")
plt.legend()
plt.show()

#for term in temps:
#    y = np.array(term) - np.array(averages)
#    plt.scatter(np.array(averages), y, s=12)
#plt.plot(np.linspace(min(averages), max(averages), 100), [0 for i in range(100)], linewidth=2, color='b')
#plt.legend()
#plt.title("Residual plot")
#plt.xlabel("Average Mezz_2 temperature")
#plt.ylabel("Residual in Mezz_2 temperature of specific iceboard")
#plt.show()




#plot_comber(monitor, rules=rules, discard_error=0.5)


exit()
# STOPS HERE
temp = msql.data_set(monitor, 'slowdaq_Backend_4K_Heat_Li_mean', 'slowdaq_Backend_4K_Head_mean' )
temp.make_plot()

# single day plot comber
day = [20100685, 20100685]
rules = [
    lambda j: "mean" in j[1],
    lambda j: "slowdaq_Iceboard0076_Mezz_2_Temp_mean" in j[1] or "Iceboard" not in j[1]
]
for i in monitor.gen_table_info():
    rule_broken = False
    for rule in rules:
        if not rule(i):
            rule_broken = True
            break
        if not rule_broken:
            temp = msql.data_set(run_id, "first_mjd", i[1], y_db_table=monitor, ycol_err=i[1][:len(i[1])-4]+"std")
            if len(temp.get_partition(day[0], day[1])[0]) > 5:
                print(temp.size())
                temp.make_plot(x_time_formated=True, runid_partitions=day)

# 14 159 39: DIFFERENCE IN DATA IS DIFFERENCE FROM THESE GROUPS
first = msql.data_set(monitor, 'slowdaq_Backend_4K_Head_mean', 'slowdaq_Backend_4K_Heat_Link_mean', xcol_err='slowdaq_Backend_4K_Head_std', ycol_err='slowdaq_Backend_4K_Heat_Link_std')
partitions = [20000000, 20100000, 21200000, 30000000]
first.make_plot(errorbars=True, runid_partitions=partitions)

# WACK
fiftybottom_fiftyhead = msql.data_set(monitor, 'slowdaq_50K_Bottom_mean', 'slowdaq_50K_Head_mean', xcol_err='slowdaq_50K_Bottom_std', ycol_err='slowdaq_50K_Head_std')
fiftybottom_fiftyhead.make_plot(errorbars=True, runid_partitions=partitions)

# uncertainty in temps increases with temp, same across runids
lowerleft_lowerright = msql.data_set(monitor, 'slowdaq_Primary_lower_left_mean', 'slowdaq_Primary_lower_right_mean', xcol_err='slowdaq_Primary_lower_left_std', ycol_err='slowdaq_Primary_lower_right_std')
lowerleft_lowerright.make_plot(fit=linear, errorbars=True)
lowerleft_lowerright.make_plot(errorbars=True, runid_partitions=partitions)
centerleft_upperright = msql.data_set(monitor, 'slowdaq_Secondary_center_left_mean', 'slowdaq_Secondary_upper_right_mean', xcol_err='slowdaq_Secondary_center_left_std', ycol_err='slowdaq_Secondary_upper_right_std')
centerleft_upperright.make_plot(fit=linear, errorbars=True)
centerleft_upperright.make_plot(errorbars=True, runid_partitions=partitions)

# monitor AND apex measurements are sparse related to time
time_temp = msql.data_set(run_id, 'first_mjd', 'slowdaq_Outside_Temperature_mean', y_db_table=monitor)

#no large uncertainties in the temp data
x_col = "APEX_temp_mean"
y_col = "slowdaq_Primary_center_left_mean"
x_col_err = "APEX_temp_std"
y_col_err = "slowdaq_Primary_center_left_std"
centerLeft_APEXtemp = msql.data_set(apex, x_col, y_col, y_db_table=monitor, xcol_err=x_col_err, ycol_err=y_col_err)
centerLeft_APEXtemp.make_plot(errorbars=True, runid_partitions=partitions)

#averages of the mirror temperatures
# i dont do errors since all the errors are pretty low
mirror_temps = []
for i in range(13):
    mirror_temps.append([])
cols = []
cols.append("slowdaq_Secondary_bottom_mean")
cols.append("slowdaq_Secondary_center_bottom_mean")
cols.append("slowdaq_Secondary_center_right_mean")
cols.append("slowdaq_Secondary_center_left_mean")
cols.append("slowdaq_Secondary_center_top_mean")
cols.append("slowdaq_Secondary_left_mean")
cols.append("slowdaq_Secondary_lower_left_mean")
cols.append("slowdaq_Secondary_lower_right_mean")
cols.append("slowdaq_Secondary_right_mean")
cols.append("slowdaq_Secondary_top_mean")
cols.append("slowdaq_Secondary_upper_left_mean")
cols.append("slowdaq_Secondary_upper_right_mean")
cols.append("slowdaq_Outside_Temperature_mean")
for row in monitor.gen_table():
    addRow = True
    thisrow = []
    for i in range(len(cols)):
        element = row[monitor.get_column_index(cols[i])]
        if element is None or element > 400 or element < -100:
            addRow = False
            break
        else:
            thisrow.append(element)
    if addRow:
        for j in range(len(mirror_temps)):
            mirror_temps[j].append(thisrow[j])
averages = []
for i in range(len(mirror_temps[0])):
    sum = 0
    points = 0
    for j in range(len(mirror_temps) - 1):
        sum += mirror_temps[j][i]
        points += 1
    averages.append(sum / points)
assert len(averages) == len(mirror_temps[0])
plt.scatter(mirror_temps[len(mirror_temps) - 1], averages, s=12)
plt.xlabel("slowdaq_Outside_Temperature_mean")
plt.ylabel("Average mirror temperature")
plt.show()
for i in range(len(mirror_temps)):
    plt.scatter(averages, mirror_temps[i], s=12, label=cols[i])
plt.xlabel("Average mirror temperature")
plt.ylabel("Mirror location temperature")
plt.legend()
plt.show()



times = []
for i in run_id.file_cursor.execute("SELECT * FROM pb2a_runid"):
    begin = Time(i[2], format='mjd').iso
    end = Time(i[3], format='mjd').iso
    times.append([str(begin), str(end)])




file = "pb2a_monitor.db"
table = "pb2a_monitor"

for i in gen_table_info(file, table):
    print(i)

def parse(d_type, comparator):
    most = iceboard_corr[0]
    for i in iceboard_corr:
        if comparator(most[d_type], i[d_type]) < 0:
            most = i
    return most

    # RUN THIS FILE AS IT IS FOR THE FINAL ANALYSIS
#plot_comber("pb2a_monitor.db", "pb2a_monitor", rules)

#make_plot(file, table, 'slowdaq_Iceboard0076_Mezz_2_Temp_mean', 'slowdaq_Iceboard0076_Mezz_2_Temp_mean')
corr = []
for i in iceboard_corr:
    corr.append(i[5])
corr = np.array(corr)
corr = corr[~np.isnan(corr)]
plt.hist(corr, density=True, rwidth=0.9, bins=40)
kde_space = np.linspace(-1, 1, 300)
kde = st.gaussian_kde(corr)
plt.plot(kde_space, kde.pdf(kde_space), label="PDF")
plt.legend(loc="upper left")
plt.title("Correlations of reduced data sets")
plt.xlabel("correlation")
plt.ylabel("number of data set pairs")
plt.show()

# to compare two data sets across files, must confirm for each point (x,y) that x and y have the same
# run_id AND run_subid
