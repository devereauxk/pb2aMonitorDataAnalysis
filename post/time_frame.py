from monitor_sql import *

file = open("runid_time_table.txt", "r")

def get_long_runs(file, limit):
    """
        input:
            file: the file (in the format of the runid_time_table.txt file) of run_id.db info
            limit: the number of subids a runid must have to be considered
        output: the array of runid numbers of runs which have at least 10 associated subids
    """
    file_lines = file.read().splitlines()
    good_runs = []
    for i in range(len(file_lines) - 1):
        entries = file_lines[i].split(",")
        last_subid = entries[1]
        next_entry = file_lines[i+1].split(",")
        next_subid = next_entry[1]
        subid_count = 1
        while next_subid == last_subid + 1:
                i += 1
                subid_count += 1
                last_subid = next_subid
                next_subid = file_lines[i+1].split(",")
                if subid_count >= limit:
                    good_runs += entries[0]
                    break
    return good_runs
