from os import walk
import datetime as dt
import sqlite3
from astropy.time import Time

from data_set import *
from database_file import *
from database_table import *

def print_all_tables_in_folder(path):

    _, _, filenames = next(walk(path))

    for name in filenames:
        temp = database_file(path+name)
        temp.print_table_infos()

def make_new_db_file_set_runids(db_in, runids):
    """
        dt: a database_table object, nondestructive on dt
        runids: array of runids (int). For a single runid do [runid]
        makes a new db file containing the database_table object and trims the table to include
            only rows for the specified runids (runids between start and end). when end=start, output contains only
            the start runid
        names this file the same name as the file containing database_table with a '_temp.db' at the end
        titles the table the same
        returns the newly created and modified database_table object
    """
    db = db_in.copy()

    # the following is destructive on 'db'

    db.file_cursor.execute("CREATE TABLE temp (run_id INT);")
    for runid in runids:
        db.file_cursor.execute("INSERT INTO temp (run_id) VALUES ({0})".format(runid))

    db.file_cursor.execute("CREATE TABLE temp2 AS SELECT * FROM {0} INNER JOIN temp ON temp.run_id = {0}.run_id".format(db.name))
    db.file_cursor.execute("DROP TABLE {0}".format(db.name))
    db.file_cursor.execute("ALTER TABLE temp2 RENAME TO {0}".format(db.name))
    db.file_cursor.execute("DROP TABLE temp")
    db.commit()

    return db

""" IF YOU WISH TO PLOT SOMETHING WRT, JUST USE X_DATA_TABLE = PB2A_RUNID.DB """


def __main__():

    path = "../databases/"
    run_id = database_table(path+"pb2a_runid.db", "pb2a_runid")
    monitor = database_table(path+"pb2a_monitor.db", "pb2a_monitor")
    stat = database_table(path+"pb2a-20210205/pb2a_data_quality.db", "pb2a_scan_stat")
    linear = lambda x, m, b: m * x + b
    zero_to_one = Time(datetime(2019, 3, 8, 15, 25, 7, 706722)).mjd
    one_to_twelve = Time(datetime(2020, 1, 30, 16, 21, 40, 910587)).mjd
    partitions = [20000000, 20100000, 21200000, 22300000, 30000000]



    """ gen plots for postretrofit including preretrofit data (in POSTRETROCOMPARE folder)
    columns = [ 'slowdaq_250mK_far_mean', 'slowdaq_250mK_bottom_left_mean',

                'slowdaq_350mK_stripline_heatsink_mean', 'slowdaq_350mK_ring_mean',

                'slowdaq_1K_stripline_heatsink_mean',

                'slowdaq_2K_ring_mean',

                'slowdaq_4K_blackbody_mean', 'slowdaq_Backend_4K_Head_mean', 'slowdaq_Backend_4K_Heat_Link_mean', 'slowdaq_4K_ring_mean'

                'slowdaq_50K_Bottom_mean', 'slowdaq_50K_Head_mean',

                'slowdaq_SC_Fridge_Mainplate_mean', 'slowdaq_SC_Fridge_Ultrahead_mean',  'slowdaq_SQUID_card_mean', 'slowdaq_bottom_wafer_lc_board_mean']

    for col in columns:
        temp = data_set(stat, "scan_speed", col,
                y_db_table=monitor,
                ycol_err= col[:len(col)-4] + "std")
        err_std = np.std(temp.y_data_err)                   # some bars really big, clutters plot, this removes those bars
        print(err_std)
        temp.discard_large_error_points(1*err_std)
        temp.make_plot(errorbars=True, runid_partitions=partitions,
                legend=True)
    """


    """ temp """
    columns = [ 'slowdaq_250mK_far_mean', 'slowdaq_250mK_bottom_left_mean',

                'slowdaq_350mK_stripline_heatsink_mean', 'slowdaq_350mK_ring_mean',

                'slowdaq_1K_stripline_heatsink_mean',

                'slowdaq_2K_ring_mean',

                'slowdaq_4K_blackbody_mean', 'slowdaq_Backend_4K_Head_mean', 'slowdaq_Backend_4K_Heat_Link_mean', 'slowdaq_4K_ring_mean',

                'slowdaq_50K_Bottom_mean', 'slowdaq_50K_Head_mean',

                'slowdaq_SC_Fridge_Mainplate_mean', 'slowdaq_SC_Fridge_Ultrahead_mean',  'slowdaq_SQUID_card_mean', 'slowdaq_bottom_wafer_lc_board_mean']

    for col in columns:
        temp = data_set(stat, "scan_speed", col,
                y_db_table=monitor,
                ycol_err= col[:len(col)-4] + "std")
        err_std = np.std(temp.y_data_err)                   # some bars really big, clutters plot, this removes those bars
        temp.discard_large_error_points(1*err_std)
        temp.print_y_stats(partitions=partitions)


    """ gen plots for postretrofit (in POSTRETROFIT folder)
    parts = [i for i in range(22300403, 22300416)]
    monitor_after_retrofit = make_new_db_file_set_runids(monitor, parts)

    columns = ['slowdaq_bottom_wafer_lc_board_mean', 'slowdaq_250mK_bottom_left_mean',
                'slowdaq_1K_stripline_heatsink_mean', 'slowdaq_4K_ring_mean', 'slowdaq_350mK_ring_mean',
                'slowdaq_2K_ring_mean', 'slowdaq_350mK_stripline_heatsink_mean', 'slowdaq_focal_plane_3_mean',

                'slowdaq_250mK_far_mean', 'slowdaq_4K_blackbody_mean', 'slowdaq_50K_Bottom_mean', 'slowdaq_50K_Head_mean',

                'slowdaq_SC_Fridge_Mainplate_mean', 'slowdaq_SC_Fridge_Ultrahead_mean', 'slowdaq_Backend_4K_Head_mean', 'slowdaq_Backend_4K_Heat_Link_mean']

    for col in columns:
        temp = data_set(stat, "start_mjd", col,
                y_db_table=monitor_after_retrofit,
                ycol_err= col[:len(col)-4] + "std")
        temp.make_plot(errorbars=True, runid_partitions=parts,
                legend=True)
    """


    """ single plot lookup
    azs_focal_mean = data_set(stat, "scan_speed",
                'slowdaq_250mK_bottom_left_mean',
                y_db_table=monitor_after_retrofit,
                ycol_err='slowdaq_250mK_bottom_left_std')

    azs_focal_mean.make_plot(errorbars=True, runid_partitions=parts,
                legend=False)
    """

    """ test if monitor contains data
    fp = monitor_after_retrofit.get_column_index('slowdaq_lyot_stop_blackbody_mean')
    for row in monitor_after_retrofit.gen_table():
        print(row[fp])
    """


    """ inspect files
    path = "../databases/"

    print_all_tables_in_folder(path)

    monitor = database_file(path+"pb2a_monitor.db")
    runids = []
    for line in monitor.database_tables[0].gen_table():
        runids.append(line[0])
    print("max runid: " + str(max(runids)))
    print("runids: " + str(len(runids)))
    """


__main__()
