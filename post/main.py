from monitor_sql import *
from database_file import *
from database_table import *


def print_all_tables_in_folder(path):
    from os import walk
    _, _, filenames = next(walk(path))

    for name in filenames:
        temp = database_file(path+name)
        temp.print_table_infos()

def make_new_db_file_set_runs(dt, start, end):
    """
        dt: a database_table object, nondestructive on dt
        start, end: runids (int)
        makes a new db file containing the database_table object and trims the table to include
            only rows for the specified runids (runids between start and end). when end=start, output contains only
            the start runid
        names this file the same name as the file containing database_table with a '_temp.db' at the end
        titles the table the same
        returns the newly created and modified database_table object
    """
    db = dt.copy()

    # the following is destructive on 'db'
    db.file_cursor.execute("DELETE FROM {0} WHERE {0}.runid != {1}".format(dt.name, runid))
    db.name = new_name
    return db

def add_time_to_db_table(dt):
    #TODO
    return 0



def __main__():

    path = "../databases/pb2a-20210128/"

    print_all_tables_in_folder(path)

    monitor = database_file(path+"pb2a_monitor.db")
    runids = []
    for line in monitor.database_tables[0].gen_table():
        runids.append(line[0])
    print("max runid: " + str(max(runids)))
    print("runids: " + str(len(runids)))




__main__()
