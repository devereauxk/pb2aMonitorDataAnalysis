import numpy as np
import scipy.spatial as spat
import scipy.optimize as opt
from scipy.interpolate import interp1d
import scipy.stats as st
import matplotlib
import matplotlib.pyplot as plt
import math

import sqlite3
from os import listdir
from os.path import isfile, join

# runner code is at the very bottom

# mypath = "/data/pb2/ChileData/databases/pb2a-20200301"
mypath = "../databases/pb2a-20200301"

files = [f for f in listdir(mypath) if isfile(join(mypath, f))]
print(files)

cursors = []
for file in files:
    cursors.append(sqlite3.connect(mypath + "/" + str(file)))

# returns cursor of file given the file's name
def get_cursor(file_name):
    return sqlite3.connect(mypath + "/" + file_name)

def print_file_info(file_cursors):
    c = file_cursors
    #for each file
    for c in cursors:
        #this_file has the names of each table as a string
        this_file_tables = []
        # i is default unicode in a 1-long tuple
        for i in c.execute("SELECT name FROM sqlite_master WHERE type='table';"):
            this_file_tables.append(str(i[0]))
        print(this_file_tables)

#prints table given the cursor of the db file and table name
def gen_table_info(file, table):
    yield from get_cursor(file).execute("PRAGMA table_info({})".format(table))

iceboard_corr = []

# make a plot given the name of a database file and column of data
def make_plot(file, table, x_column, y_column):
    #import data...
    x = []
    y = []
    for i in get_cursor(file).execute("SELECT {}, {} FROM {}".format(x_column, y_column, table)):
        if (i[0] is not None and i[1] is not None and type(i[0]) == type(i[1])):
            x.append(i[0])
            y.append(i[1])
    #remove outliers?
    #plotting data...
    if len(x) > 10:
        try:
            # finding error in slope
            line_func = lambda x, m, b: m * x + b
            # opt.curve_fit(function, x vals, y vals)
            fit_vals, fit_err = opt.curve_fit(line_func, x, y)
            m = fit_vals[0]
            b = fit_vals[1]
            m_err = math.sqrt(fit_err[0][0])
            b_err = math.sqrt(fit_err[1][1])
            # 0.015
            corr = np.corrcoef(np.array(x), np.array(y))[0,1]
            iceboard_corr.append([x_column, y_column, m, b, m_err, corr])

            #x_space = np.linspace(1.3, max(x), num=100)
            #y_pred = line_func(x_space, m, b)
            #plot_residual(x, y, lambda x: line_func(x, m, b), x_column, y_column)


            #plt.xlim(min(x), max(x))
            #plt.ylim(min(y), max(y))
            #plt.scatter(x, y, s=12)
            #plt.plot(x_space, y_pred, label = "Linear fit: {}x + {}".format("%.2f" % m, "%.2f" % b), linewidth=1)
            #plt.legend()
            #plt.title(y_column + " vs " + x_column)
            #plt.xlabel(x_column)
            #plt.ylabel(y_column)
            #plt.show()
            #plt.savefig(x_column + y_column)
            #plt.close()
        except ValueError:
            print("no overlapping data for", x_column, "and", y_column)
    else:
        print("not enought data points for", x_column, "and", y_column)

def plot_comber(file, table, rules):
    #file = "pb2a_apex.db"
    #table = "pb2a_APEX"
    print_file_info(cursors)
    for i in gen_table_info(file, table):
        print(i)
        i = 0
    for j in gen_table_info(file, table):
        for k in gen_table_info(file, table):
            # add other arguements here to vet what plots you see
            # ex: and "mean" in j[0] and "mean" in k[0]
            # the k[0] > j[0] is required to avoid redundancy
            if k[0] > j[0]:
                rule_broken = False
                for rule in rules:
                    if not rule(j, k):
                        rule_broken = True
                        break
                if not rule_broken:
                    if i % 100 == 0:
                        print(i)
                    make_plot(file, table, j[1], k[1])
                    i += 1

# for tables made with sets from different files,
def two_file_make_plot(file, table, x_column, y_column):
    #import data...
    filex = "pb2a_apex.db"
    tablex = "pb2a_APEX"
    x = []
    y = []
    for i in get_cursor(filex).execute("SELECT run_id, run_subid, {} FROM {}".format(x_column, tablex)):
        for j in get_cursor(file).execute("SELECT run_id, run_subid, {} FROM {}".format(y_column, table)):
            if i[0] == j[0] and i[1] == j[1] and i[2] is not None and j[2] is not None and type(i[2]) == type(j[2]):
                x.append(i[2])
                y.append(j[2])
    #remove outliers?
    #plotting data...
    try:
        # finding error in slope
        line_func = lambda x, m, b: m * x + b
        quad_func = lambda x, a, b, c: a*x**2 + b*x + c
        # opt.curve_fit(function, x vals, y vals)
        fit_vals, fit_err = opt.curve_fit(line_func, x, y)
        m = fit_vals[0]
        b = fit_vals[1]
        m_err = math.sqrt(fit_err[0][0])
        b_err = math.sqrt(fit_err[1][1])
        x_space = np.linspace(min(x), max(x), num=100)
        y_pred = line_func(x_space, m, b)

        fit_vals, fit_err = opt.curve_fit(quad_func, x, y)
        a = fit_vals[0]
        b = fit_vals[1]
        c = fit_vals[2]
        y_acc = quad_func(x_space, a, b, c)


        plt.xlim(min(x), max(x))
        plt.ylim(min(y), max(y))
        plt.scatter(x, y, s=12)
        plt.plot(x_space, y_pred, label = "Linear fit: {}x + {}".format("%.2f" % m, "%.2f" % b), linewidth=1)
        #plt.plot(x_space, y_acc, label = "Quadratic fit", linewidth=2, color='r')
        plt.legend()
        plt.title(y_column + " vs " + x_column)
        plt.xlabel(x_column)
        plt.ylabel(y_column)
        plt.show()
        #plt.savefig(x_column + y_column)
        #plt.close()
    except ValueError:
        print("no overlapping data for", x_column, "and", y_column)

def two_file_plot_comber(file1, table1, file2, table2, rules):
    #file = "pb2a_apex.db"
    #table = "pb2a_APEX"
    print_file_info(cursors)
    for i in gen_table_info(file1, table1):
        print(i)
    for i in gen_table_info(file2, table2):
        print(i)
    for j in gen_table_info(file1, table1):
        for k in gen_table_info(file2, table2):
            # add other arguements here to vet what plots you see
            # ex: and "mean" in j[0] and "mean" in k[0]
            # the k[0] > j[0] is required to avoid redundancy
            if k[0] > j[0]:
                rule_broken = False
                for rule in rules:
                    if not rule(j, k):
                        rule_broken = True
                        break
                if not rule_broken:
                    two_file_make_plot(file2, table2, j[1], k[1])

def plot_residual(x, y, fit, x_name, y_name):
    fit_points = fit(np.array(x))
    y = np.array(y) - fit_points

    plt.xlim(min(x), max(x))
    plt.ylim(min(y), max(y))
    plt.scatter(x, y, s=12, color='b')
    plt.plot(np.linspace(min(x), max(x), 100), [0 for i in range(100)], linewidth=2, color='b')
    plt.legend()
    plt.title("Residual plot")
    plt.xlabel(x_name)
    plt.ylabel("Residual in " + y_name)
    plt.show()
    #plt.savefig(x_name + y_name + "res")
    #plt.close()

# rules as boolean lambda functions.
# take in j and k as tuples representing the data of one column of the database
# j[1] and k[1] are the names of the column as string
rules = [
    lambda j, k: "mean" in j[1] and "mean" in k[1],
    #lambda j, k: "Iceboard" in j[1] and "Iceboard" in k[1]
    lambda j, k: "slowdaq_Iceboard0076_MB_FPGA_Temp_mean" in j[1] or "slowdaq_Iceboard0076_MB_FPGA_Temp_mean" in k[1] or
        ("Iceboard" not in j[1] and "Iceboard" not in k[1]),
    lambda j, k: "slowdaq_Primary_center_left_mean" in j[1] or "slowdaq_Primary_center_left_mean" in k[1] or
        ("Primary" not in j[1] and "Primary" not in k[1]),
    lambda j, k: "slowdaq_Secondary_center_left_mean" in j[1] or "slowdaq_Secondary_center_left_mean" in k[1] or
        ("Secondary" not in j[1] and "Secondary" not in k[1])
]
file = "pb2a_monitor.db"
table = "pb2a_monitor"

def parse(d_type, comparator):
    most = iceboard_corr[0]
    for i in iceboard_corr:
        if comparator(most[d_type], i[d_type]) < 0:
            most = i
    return most

    # RUN THIS FILE AS IT IS FOR THE FINAL ANALYSIS
plot_comber("pb2a_monitor.db", "pb2a_monitor", rules)

#make_plot(file, table, 'slowdaq_Iceboard0076_MB_FPGA_Temp_mean', 'slowdaq_Iceboard0076_MB_Power_Temp_mean')
#make_plot(file, table, 'slowdaq_Outside_Pressure_mean', 'slowdaq_Wind_Direction_mean')
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
