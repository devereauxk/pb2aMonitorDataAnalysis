import sqlite3
import astropy.time as Time
import datetime as dt

from monitor_sql import *
from run import *

class time_frame:
    """
        destructively modifies a database_table to include only entries with a runid within a specific interval. this is done by calling the mod_database method
    """

    def __init__(self, start, end, file):
        """
            start and end runids
            file: same file type as fed into run in run.py
            db: COPY of the database_table object to modify to include only the entries with runids between start and end
        """
        self.runs = []
        ids = []
        self.start = start
        self.end = end

        lines = file.read().splitlines()
        for i in range(len(lines)):
            entry = lines[i].split(",")
            if int(entry[0]) >= start and int(entry[0]) <= end and int(entry[0]) not in ids:
                file.seek(0, 0)
                self.runs.append(run(int(entry[0]), file))
                ids.append(int(entry[0]))

    def mod_database(self, db):
        """
            THIS METHOD IS DESTRUCTIVE, COPY DB BEFORE EXECUTING
            input: db: the db object with runids in the first column
            output: replaces the db with a new one which has only entries with runids between start and end
                and adds the subid start times as a new column
        """

        db.file_cursor.execute("CREATE TABLE temp (run_id INT, run_subid INT, time REAL);")
        for run in self.runs:
            for entry in run.subid_times:
                db.file_cursor.execute("INSERT INTO temp (run_id, run_subid, time) VALUES ({0}, {1}, {2})".format(run.runid, entry[0], Time(entry[1]).mjd))

        new_name = "run_ids_" + str(self.start) + "_to_" + str(self.end)
        db.file_cursor.execute("CREATE TABLE {0} AS SELECT * FROM {1} INNER JOIN temp ON temp.run_id = {1}.run_id AND temp.run_subid = {1}.run_subid".format(new_name, db.name))

        db.file_cursor.execute("DROP TABLE temp")
        db.file_cursor.execute("DROP TABLE {0}".format(db.name))
        db.name = new_name

""" MAIN : this is for graphing data for multiple runs """

""" TESTS

copy = monitor.copy()
frame = time_frame(20100497, 20100497, file)
frame.mod_database(copy)
print(copy.size())
temp_ds = data_set(copy, "time", "slowdaq_focal_plane_3_mean", ycol_err="slowdaq_focal_plane_3_std")
temp_ds.make_plot(errorbars=True, x_time_formated=True)
copy.close()

"""


""" by time
start = dt.datetime(2020, 2, 1, 0, 0, 0, 0)
window = [start, start + dt.timedelta(hours=10)]

while window[1] < dt.datetime(2021, 1, 1):
    time_d = dt.timedelta(minutes=10)
    window[0] = window[0] + time_d
    window[1] = window[1] + time_d
"""
