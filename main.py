import datetime as dt
import sqlite3
from astropy.time import Time

from monitor_sql import *
from run import *
from time_frame import *

""" __main__ method is at very bottom """

def single_run():
    file = open("runid_time_table.txt", "r")
    path = "../databases/pb2a-20200818"  # 301, 818
    monitor = database_table(path, "pb2a_monitor.db", "pb2a_monitor")

    copy = monitor.copy()
    temp_run = run(20100685, file)  # specify runid
    temp_run.mod_database(copy)
    temp_ds = data_set(copy, "time", "slowdaq_focal_plane_3_mean", ycol_err="slowdaq_focal_plane_3_std")   # specify data point
    temp_ds.make_plot(errorbars=True, x_time_formated=True)
    copy.close()


def long_runs():
    file = open("runid_time_table.txt", "r")
    path = "../databases/pb2a-20200818"  # 301, 818
    monitor = database_table(path, "pb2a_monitor.db", "pb2a_monitor")

    temp = data_set(monitor, "run_id", "run_subid")
    long_run_ids = []
    for i in range(len(temp.x_data)):
        counter = 1
        this = temp.x_data[i]
        for j in range(len(temp.x_data)):
            if temp.x_data[j] == this:
                counter += 1
            if counter >= 7:
                long_run_ids.append(this)
                break
    temp = []
    for i in long_run_ids:
        if i not in temp:
            temp.append(i)
    long_run_ids = temp
    print(long_run_ids)

    runs = []
    for id in long_run_ids:
        file = open("runid_time_table.txt", "r")
        runs.append(run(id, file))

    monitor_copy = monitor.copy()
    for run in runs:
        run.mod_database(monitor_copy) # changes monitor copy
        print(run.runid)
        temp_ds = data_set(monitor_copy, "time", "slowdaq_focal_plane_3_mean", ycol_err="slowdaq_focal_plane_3_std")   # specify data point
        temp_ds.make_plot(errorbars=True, x_time_formated=True)
        monitor_copy.close()
        monitor_copy = monitor.copy()


def long_time_frames():
    file = open("runid_time_table.txt", "r")
    path = "../databases/pb2a-20200818"  # 301, 818
    monitor = database_table(path, "pb2a_monitor.db", "pb2a_monitor")


    run_ids = []
    lines = file.read().splitlines()
    for i in range(len(lines)):
        entry = lines[i].split(",")
        if int(entry[0]) not in run_ids:
            run_ids.append(int(entry[0]))

    time_frames = []
    interval = 10
    for i in range(len(run_ids) - interval):
        start = int(run_ids[i])
        end = int(run_ids[i+interval])
        file = open("runid_time_table.txt", "r")
        temp_frame = time_frame(start, end, file)
        time_frames.append(temp_frame)
    print(len(time_frames))

    points = 7
    copy = monitor.copy()
    for frame in time_frames:
        frame.mod_database(copy)
        if copy.size() >= points:
            temp_ds = data_set(copy, "time", "slowdaq_focal_plane_3_mean", ycol_err="slowdaq_focal_plane_3_std")
            temp_ds.make_plot(errorbars=True, x_time_formated=True)
        copy.close()
        copy = monitor.copy()

def __main__():
    long_time_frames()

__main__()
