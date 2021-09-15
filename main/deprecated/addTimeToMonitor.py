import sqlite3
import datetime as dt
from monitor_sql import *

path = "../databases/pb2a-20200818"  # 301, 818
monitor = database_table(path, "pb2a_monitor.db", "pb2a_monitor")

copy = monitor.copy()
monitor.close()

copy.file_cursor.execute("CREATE TABLE temp (run_id INT, run_subid INT, time REAL);")

def get_entry_dt(entry):
    """
        takes an array of a line of a runid_time_table.txt-like file and returns the start time of that entry as a dt object
    """
    date = entry[2].split("-")
    date = [int(i) for i in date]
    time = entry[3].split(":")
    return dt.datetime(date[0], date[1], date[2], int(time[0]), int(time[1]), round(float(time[2]) // 1), round(1000000 * (float(time[2]) % 1)))

file = open("runid_time_table.txt", "r")
lines = file.read().splitlines()
for i in range(len(lines)):
    entry = lines[i].split(",")
    time = get_entry_dt(entry)
    copy.file_cursor.execute("INSERT INTO temp (run_id, run_subid, time) VALUES ({0}, {1}, {2})".format(entry[0], entry[1], Time(time).mjd))

new_name = "monitor_with_time"
copy.file_cursor.execute("CREATE TABLE {0} AS SELECT * FROM {1} INNER JOIN temp ON temp.run_id = {1}.run_id AND temp.run_subid = {1}.run_subid".format(new_name, copy.name))

copy.file_cursor.execute("DROP TABLE temp")
copy.file_cursor.execute("DROP TABLE {0}".format(copy.name))
copy.name = new_name
