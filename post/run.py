import sqlite3
import astropy.time as Time
import datetime as dt

from monitor_sql import *
from time_frame import *

class run:
    """
        destructively modifies a database_table to include only entries with a specific runid. this is done by calling the mod_database method
    """

    def __init__(self, runid, file):
        """
            arguments:
                runid: the runid of this run as an integer
                file: a file in the format of runid_time_table with info pertaining to the runid
            assignemnts:
                runid: the runid
                subid_times: a (N,2,) array where N is the number of subids, the first element for each element is the subid number
                    and the second element is the start datetime of that exposure
        """
        self.runid = runid
        self.subid_times = []

        lines = file.read().splitlines()
        for i in range(len(lines)):
            entry = lines[i].split(",")
            if int(entry[0]) == runid:
                self.subid_times = [[entry[1], self.get_entry_dt(entry)]]

                if i != len(lines) - 1:
                    next_entry = lines[i+1].split(",")
                    while int(next_entry[0]) == runid and i < len(lines) - 2:
                        i += 1
                        entry = lines[i].split(",")
                        next_entry = lines[i+1].split(",")

                        self.subid_times.append([entry[1], self.get_entry_dt(entry)])
                break

    def get_entry_dt(self, entry):
        """
            takes an array of a line of a runid_time_table.txt-like file and returns the start time of that entry as a dt object
        """
        date = entry[2].split("-")
        date = [int(i) for i in date]
        time = entry[3].split(":")
        return dt.datetime(date[0], date[1], date[2], int(time[0]), int(time[1]), round(float(time[2]) // 1), round(1000000 * (float(time[2]) % 1)))

    def mod_database(self, db):
        """
            THIS METHOD IS DESTRUCTIVE, COPY DB BEFORE EXECUTING
            input: db: the db object with runids in the first column
            output: replaces the db with a new one which has only entries with this runid and adds the subid start times as a new column
        """
        db.file_cursor.execute("CREATE TABLE temp (run_id INT, run_subid INT, time REAL);")
        for entry in self.subid_times:
            db.file_cursor.execute("INSERT INTO temp (run_id, run_subid, time) VALUES ({0}, {1}, {2})".format(self.runid, entry[0], Time(entry[1]).mjd))

        new_name = "run_" + str(self.runid)
        db.file_cursor.execute("CREATE TABLE {0} AS SELECT * FROM {1} INNER JOIN temp ON temp.run_id = {1}.run_id AND temp.run_subid = {1}.run_subid".format(new_name, db.name))

        db.file_cursor.execute("DROP TABLE temp")
        db.file_cursor.execute("DROP TABLE {0}".format(db.name))
        db.name = new_name



""" TESTS """
"""
file = open("runid_time_table.txt", "r")
temp_run = run(21200344, file)
copy = monitor.copy()
temp_run.mod_database(copy)
copy.print_head(1)

path = "../databases/pb2a-20200818"  # 301, 818
monitor = database_table(path, "pb2a_monitor.db", "pb2a_monitor")
runids = []
for row in monitor.gen_table():
    runids.append(row[0])


good_runs = []
for runid in runids:
    file = open("runid_time_table.txt", "r")
    temp_run = run(runid, file)
    if len(temp_run.subid_times) >= 10:
        good_runs.append(temp_run)
"""
