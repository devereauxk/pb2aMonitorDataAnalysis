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

# convinient way to store two sets of data (an x and an y set) joined on the same run_id and runsubid, \
# with the option of storing their error sets too
class data_set():
    # arguements: database_table objects, collumn names, column names of error data sets
    # errors, are by convention, one standard deviation
    # this files and the databases can be in different folders
    # assumptions: joins on runid and runsubid, assumes ending of each db filename is ".db", errors of some data are in the same table
    def __init__(self, x_db_table, xcol, ycol, y_db_table=None, xcol_err=None, ycol_err=None):
        self.x_table = x_db_table
        self.y_table = y_db_table
        self.x_col = xcol
        self.y_col = ycol
        self.x_data = []
        self.y_data = []
        self.runid = []
        self.x_data_err = None
        self.y_data_err = None
        if xcol_err:
            self.x_data_err = []
        if ycol_err:
            self.y_data_err = []

        if self.y_table == None:
            if xcol_err and ycol_err:
                for i in self.x_table.file_cursor.execute("SELECT {0}, {1}, {2}, {3}, run_id FROM {4} WHERE {0} IS NOT NULL AND {1} IS NOT NULL;".format(self.x_col, self.y_col, xcol_err, ycol_err, self.x_table.name)):
                    self.x_data.append(i[0])
                    self.y_data.append(i[1])
                    self.x_data_err.append(i[2])
                    self.y_data_err.append(i[3])
                    self.runid.append(i[4])
            elif xcol_err:
                for i in self.x_table.file_cursor.execute("SELECT {0}, {1}, {2}, run_id FROM {3} WHERE {0} IS NOT NULL AND {1} IS NOT NULL;".format(self.x_col, self.y_col, xcol_err, self.x_table.name)):
                    self.x_data.append(i[0])
                    self.y_data.append(i[1])
                    self.x_data_err.append(i[2])
                    self.runid.append(i[3])
            elif ycol_err:
                for i in self.x_table.file_cursor.execute("SELECT {0}, {1}, {2}, run_id FROM {3} WHERE {0} IS NOT NULL AND {1} IS NOT NULL;".format(self.x_col, self.y_col, ycol_err, self.x_table.name)):
                    self.x_data.append(i[0])
                    self.y_data.append(i[1])
                    self.y_data_err.append(i[2])
                    self.runid.append(i[3])
            else:
                for i in self.x_table.file_cursor.execute("SELECT {0}, {1}, run_id FROM {2} WHERE {0} IS NOT NULL AND {1} IS NOT NULL;".format(self.x_col, self.y_col, self.x_table.name)):
                    self.x_data.append(i[0])
                    self.y_data.append(i[1])
                    self.runid.append(i[2])
        else:
            second_db_name = self.y_table.file_name[:len(self.y_table.file_name)-3]
            self.x_table.file_cursor.execute("ATTACH {} AS {};".format("'"+ self.y_table.dir + "'", second_db_name))
            if xcol_err and ycol_err:
                for i in self.x_table.file_cursor.execute("SELECT {1}.{3}, {0}.{2}.{4}, {1}.{5}, {0}.{2}.{6}, {1}.run_id FROM {1} INNER JOIN {0}.{2} ON {1}.run_id = {0}.{2}.run_id AND {1}.run_subid = {0}.{2}.run_subid WHERE {1}.{3} IS NOT NULL AND {0}.{2}.{4} IS NOT NULL;".format(second_db_name, self.x_table.name, self.y_table.name, self.x_col, self.y_col, xcol_err, ycol_err)):
                    self.x_data.append(i[0])
                    self.y_data.append(i[1])
                    self.x_data_err.append(i[2])
                    self.y_data_err.append(i[3])
                    self.runid.append(i[4])
            elif xcol_err:
                for i in self.x_table.file_cursor.execute("SELECT {1}.{3}, {0}.{2}.{4}, {1}.{5}, {1}.run_id FROM {1} INNER JOIN {0}.{2} ON {1}.run_id = {0}.{2}.run_id AND {1}.run_subid = {0}.{2}.run_subid WHERE {1}.{3} IS NOT NULL AND {0}.{2}.{4} IS NOT NULL;".format(second_db_name, self.x_table.name, self.y_table.name, self.x_col, self.y_col, xcol_err)):
                    self.x_data.append(i[0])
                    self.y_data.append(i[1])
                    self.x_data_err.append(i[2])
                    self.runid.append(i[3])
            elif ycol_err:
                for i in self.x_table.file_cursor.execute("SELECT {1}.{3}, {0}.{2}.{4}, {0}.{2}.{5}, {1}.run_id FROM {1} INNER JOIN {0}.{2} ON {1}.run_id = {0}.{2}.run_id AND {1}.run_subid = {0}.{2}.run_subid WHERE {1}.{3} IS NOT NULL AND {0}.{2}.{4} IS NOT NULL;".format(second_db_name, self.x_table.name, self.y_table.name, self.x_col, self.y_col, ycol_err)):
                    self.x_data.append(i[0])
                    self.y_data.append(i[1])
                    self.y_data_err.append(i[2])
                    self.runid.append(i[3])
            else:
                for i in self.x_table.file_cursor.execute("SELECT {1}.{3}, {0}.{2}.{4}, {1}.run_id FROM {1} INNER JOIN {0}.{2} ON {1}.run_id = {0}.{2}.run_id AND {1}.run_subid = {0}.{2}.run_subid WHERE {1}.{3} IS NOT NULL AND {0}.{2}.{4} IS NOT NULL;".format(second_db_name, self.x_table.name, self.y_table.name, self.x_col, self.y_col)):
                    self.x_data.append(i[0])
                    self.y_data.append(i[1])
                    self.runid.append(i[2])
            self.x_table.file_cursor.execute("DETACH DATABASE {};".format(second_db_name))

    def get_correlation(self):
        return np.corrcoef(np.array(self.x_data), np.array(self.y_data))[0,1]

    # fit is un-fitted lambda function with idependent variable as the first arguement
    # runid_partitions is an array of at least length two which specify the limits of each runid partition as a runid
    # broken_time_axis is array of runids for which to break the x axs at. only if x axis is time formatted (i.e. x_time_formated=True)
    # save_dir saves plot to dir (without filename), doesn't show plot
    def make_plot(self, fit=None, errorbars=False, x_time_formated=False, runid_partitions=None, broken_time_axis=False, legend=True, save_dir=None):
        x = self.x_data
        y = self.y_data
        x_err = self.x_data_err
        y_err = self.y_data_err
        if runid_partitions:
            partitions = []
            for i in range(len(runid_partitions) - 1):
                temp = self.get_partition(runid_partitions[i], runid_partitions[i+1])
                if temp is not None:
                    temp.append([runid_partitions[i], runid_partitions[i+1]])
                    partitions.append(temp)
        elif runid_broken_axis:
            # TODO
            pass

        try:
            if x_time_formated:
                temp_x_data = x
            if fit:
                f, f_param, f_err = self.fit(fit)
                print("Fitted values parameters are:")
                for i in range(len(f_param)):
                    print("\t"+"%.2f" % f_param[i]+" +/- "+"%.2f" %f_err[i][i])
                print("with reduced chi-squared of:\n\t", self.get_reduced_chi_sq(f))
            if runid_partitions:
                for part in partitions:
                    if part[4][0] == part[4][1] - 1:
                        label = "Run_id "+str(part[4][0])
                    else:
                        label = "Run_id "+str(part[4][0])+" to "+str(part[4][1])
                    if x_time_formated:
                        part[0] = Time(part[0], format='mjd').datetime
                    plt.scatter(part[0], part[1], s=12, label=label)
            else:
                if x_time_formated:
                    for i in range(self.size()):
                        x[i] = Time(x[i], format='mjd').datetime
                plt.scatter(x, y, s=12)
            if errorbars:
                if runid_partitions:
                    for part in partitions:
                        plt.errorbar(part[0], part[1], yerr=part[3], xerr=part[2], ls='none', capsize=2, elinewidth=1, markeredgewidth=1)
                else:
                    plt.errorbar(x, y, yerr=y_err, xerr=x_err, ls='none', capsize=2, elinewidth=1, markeredgewidth=1)
            if fit:
                x_space = np.linspace(min(x), max(x), num=100)
                y_space = f(x_space)
                plt.plot(x_space, y_space, label = "Fit", linewidth=1)
            if x_time_formated:
                plt.gcf().autofmt_xdate()
                x = temp_x_data
            if legend:
                #plt.legend(bbox_to_anchor=(1.04,1))
                #plt.subplots_adjust(right=0.7)
                plt.legend(loc='best', fontsize=8)
            plt.title(self.y_col + " vs " + self.x_col)
            plt.xlabel(self.x_col)
            plt.ylabel(self.y_col)
            if save_dir:
                plt.savefig(save_dir + self.x_col + "-" + self.y_col)
                plt.close()
            else:
                plt.show()

        except Exception as e:
            print("charting " + self.x_col + " and " + self.y_col + " threw error: " + str(e))

    def make_residual(self):
        x_space, y_space, m, b = self.linear_fit()
        y = np.array(self.y_data) - m * np.array(self.x_data) - b
        plt.xlim(min(self.x_data), max(self.y_data))
        plt.ylim(min(y), max(y))
        plt.scatter(np.array(self.x_data), y, s=12, color='b')
        plt.plot(np.linspace(min(self.x_data), max(self.x_data), 100), [0 for i in range(100)], linewidth=2, color='b')
        plt.legend()
        plt.title("Residual plot")
        plt.xlabel(self.x_col)
        plt.ylabel("Residual in " + self.y_col)
        plt.show()
        #plt.savefig(x_name + y_name + "res")
        #plt.close()

    # f given as some lambda function with no fitting
    # for example, linear would be
    # f = lambda x, m, b: m * x + b
    # first argument of f must be the independent variable
    # returns function with one arguemnt being indendent variable,
    # fitted parameters as array
    # error in fitted parameters as array
    def fit(self, f):
        fit_vals, fit_err = opt.curve_fit(f, self.x_data, self.y_data)
        def fitted_f(x):
            temp_f = partial(f, x)
            for i in range(len(fit_vals)-1):
                temp_f = partial(temp_f, fit_vals[i])
            return temp_f(fit_vals[len(fit_vals)-1])
        return fitted_f, fit_vals, fit_err

    # arguemnt is a fitted function f
    # converts the y and x err into only y error
    # doesn't destory y_col_err, only returns set with errors
    def get_compounded_y_err(self, f):
        only_y_err = []
        for i in range(self.size()):
            slope = derivative(f, self.x_data[i])
            only_y_err.append(np.sqrt(self.y_data_err[i] ** 2 + (slope ** 2) * (self.x_data_err[i] ** 2)))
        return only_y_err

    def remove_x_err(self, f):
        self.x_data_err = [0 for i in range(self.size())]
        self.y_data_err = get_compounded_y_err(f)

    # arguemnt is a fitted function f
    def get_reduced_chi_sq(self, f):
        nu = self.size() - len(signature(f).parameters) + 1
        # y-err, by convention, is the std
        y_err = self.get_compounded_y_err(f)
        chi_sq = 0
        for i in range(self.size()):
            chi_sq += ((self.y_data[i] - f(self.x_data[i])) / y_err[i]) ** 2
        return chi_sq / nu

    def size(self):
        return len(self.x_data)

    # contrains data between two runids
    def constrain_data(self, id1, id2):
        new_x = []
        new_y = []
        new_t = []
        new_x_err = []
        new_y_err = []
        for i in range(self.size()):
            t = self.runid[i]
            if t >= id1 and t <= id2:
                new_x.append(self.x_data[i])
                new_y.append(self.y_data[i])
                new_t.append(t)
                new_x_err.append(self.x_data_err[i])
                new_y_err.append(self.y_data_err[i])
        self.x_data = new_x
        self.y_data = new_y
        self.runid = t
        self.x_data_err = new_x_err
        self.y_data_err = new_y_err

    # returns partitioned data as in array [x, y, x_err, y_err]
    # retirns inclusive data for both id1 and exclusive for id2. id1 < id2
    # returns none if no data in partition
    def get_partition(self, id1, id2):
        new_x = []
        new_y = []
        new_x_err = []
        new_y_err = []
        for i in range(self.size()):
            t = self.runid[i]
            if t >= id1 and t < id2:
                new_x.append(self.x_data[i])
                new_y.append(self.y_data[i])
                if self.x_data_err:
                    new_x_err.append(self.x_data_err[i])
                else:
                    new_x_err = None
                if self.y_data_err:
                    new_y_err.append(self.y_data_err[i])
                else:
                    new_y_err = None
        if len(new_x) == 0:
            return None
        return [new_x, new_y, new_x_err, new_y_err]

    def discard_large_error_points(self, error):
        n_x_d = []
        n_y_d = []
        n_x_d_e = []
        n_y_d_e = []
        n_r = []
        for i in range(len(self.x_data)):
            if (self.x_data_err is not None and self.x_data_err[i] <= error) or (self.y_data_err is not None and self.y_data_err[i] <= error):
                n_x_d.append(self.x_data[i])
                n_y_d.append(self.y_data[i])
                if self.x_data_err is not None:
                    n_x_d_e.append(self.x_data_err[i])
                if self.y_data_err is not None:
                    n_y_d_e.append(self.y_data_err[i])
                n_r.append(self.runid[i])
        self.x_data = n_x_d
        self.y_data = n_y_d
        self.x_data_err = n_x_d_e
        self.y_data_err = n_y_d_e
        self.runid = n_r

    # discards all points with values over y
    def discard_large_values(self, y):
        if y is None:
            return
        n_x_d = []
        n_y_d = []
        n_x_d_e = []
        n_y_d_e = []
        n_r = []
        for i in range(len(self.x_data)):
            if self.y_data[i] <= y:
                n_x_d.append(self.x_data[i])
                n_y_d.append(self.y_data[i])
                if self.x_data_err is not None:
                    n_x_d_e.append(self.x_data_err[i])
                if self.y_data_err is not None:
                    n_y_d_e.append(self.y_data_err[i])
                n_r.append(self.runid[i])
        self.x_data = n_x_d
        self.y_data = n_y_d
        self.x_data_err = n_x_d_e
        self.y_data_err = n_y_d_e
        self.runid = n_r

    def print_y_stats(self, partitions=False):
        """
            computes and prints basic stats for the y data for each runid partition
            can be easily modified for 2d stats
        """
        def f_format(f):
            " input: float of int"
            return format(f, '.3f')

        if not partitions:
            partitions = [self.runids[0], self.runids[len(self.rundis)-1]]
        print("\n"+self.y_col)
        for i in range(len(partitions)-1):
            part_start = partitions[i]
            part_end = partitions[i+1]
            parts = self.get_partition(part_start, part_end)
            if parts is None:
                continue
            [_, part_y_data, _, _] = parts
            print("\t"+str(part_start)+" - "+str(part_end))
            print("\t\tmean\tmedian\tstd\tmin\tmax\t")
            print("\t\t" +  f_format(np.mean(part_y_data)) + "\t" + f_format(np.median(part_y_data))
                        + "\t" + f_format(np.std(part_y_data)) + "\t" + f_format(np.min(part_y_data))
                        + "\t" + f_format(np.max(part_y_data)))
