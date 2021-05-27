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

""" IF YOU WISH TO PLOT SOMETHING WRT TIME, JUST USE X_DATA_TABLE = PB2A_RUNID.DB """


def main():

    path = "../databases/current/"
    run_id = database_table(path+"pb2a_runid.db", "pb2a_runid")
    monitor = database_table(path+"pb2a_monitor.db", "pb2a_monitor")
    stat = database_table(path+"pb2a_data_quality.db", "pb2a_scan_stat")
    dq_file = database_file(path+"pb2a_data_quality.db")
    timestream = database_table(path+"pb2a_data_quality.db", "pb2a_timestream")

    end_dt = dt.datetime(2021, 5, 2)
    end_mjd = Time(end_dt).mjd
    start_mjd = Time(end_dt - dt.timedelta(days=30)).mjd

    # disclusive on start inclusing on end

    runids = []
    runid_col = run_id.get_column_index("run_id")
    mjd_col = run_id.get_column_index("first_mjd")
    for row in run_id.gen_table():
        if row[mjd_col] > start_mjd and row[mjd_col] <= end_mjd and row[runid_col] not in runids:
            runids.append(row[runid_col])

    trimmed_monitor = make_new_db_file_set_runids(monitor, runids)


    """ checking sparsity of monitor data
    data_ids = []
    for row in monitor.gen_table():
        if row[0] > runids[0] and row[0] < runids[len(runids) - 1]:
            data_ids.append(row[0])

    # print(data_ids)
    """

    time_trend_cols = [

    # sub-K
    ['slowdaq_250mK_far_mean', 0.8], ['slowdaq_250mK_bottom_left_mean', 0.8],

    ['slowdaq_350mK_stripline_heatsink_mean', 1], ['slowdaq_350mK_ring_mean', 1],

    'slowdaq_bottom_wafer_lc_board_mean',

    'slowdaq_focal_plane_3_mean',

    # Backend 4K
    ['slowdaq_1K_stripline_heatsink_mean', 2.5],

    ['slowdaq_2K_ring_mean', 2.5],

    'slowdaq_4K_blackbody_mean', 'slowdaq_4K_ring_mean',

    'slowdaq_Backend_4K_Head_mean', 'slowdaq_Backend_4K_Heat_Link_mean',

    # PTC (compressor)
    'slowdaq_PTC_BE_helium_temp_mean', 'slowdaq_PTC_BE_oil_temp_mean', 'slowdaq_PTC_BE_pressure_high_mean',
    'slowdaq_PTC_BE_pressure_low_mean', 'slowdaq_PTC_BE_water_in_temp_mean', 'slowdaq_PTC_BE_water_out_temp_mean',

    'slowdaq_PTC_OT_helium_temp_mean', 'slowdaq_PTC_OT_oil_temp_mean', 'slowdaq_PTC_OT_pressure_high_mean',
    'slowdaq_PTC_OT_pressure_low_mean', 'slowdaq_PTC_OT_water_in_temp_mean', 'slowdaq_PTC_OT_water_out_temp_mean',

    # OTA 4K
    'slowdaq_OT_4K_Head_mean', 'slowdaq_OT_4K_Heat_Link_mean',

    # Fridge
    'slowdaq_SC_Fridge_Mainplate_mean', 'slowdaq_SC_Fridge_Ultrahead_mean', ['slowdaq_SC_Fridge_Interhead_mean', 2],

    # 50K
    'slowdaq_50K_Bottom_mean', 'slowdaq_50K_Head_mean',

    # ambient weather
    'slowdaq_Outside_Temperature_mean', 'slowdaq_Outside_Pressure_mean', 'slowdaq_Outside_Humidity_mean', 'slowdaq_Inside_Humidity_mean'

    ]

    plot_dir = './figures/main/'

    for col_pair in time_trend_cols:
        if isinstance(col_pair, list):
            col = col_pair[0]
            y_lim = col_pair[1]
        else:
            col = col_pair
            y_lim = None
        print(col)
        temp = data_set(run_id, 'first_mjd', col, y_db_table=trimmed_monitor, ycol_err=col[:len(col)-4] + "std")
        temp.discard_large_values(y_lim)
        temp.make_plot(errorbars=True, runid_partitions=runids, legend=False, x_time_formated=True, save_dir=plot_dir)



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
    fp = monitor.get_column_index('slowdaq_lyot_stop_blackbody_mean')
    for row in monitor.gen_table():
        print(row[fp])
    """


    """ inspect files """
    #monitor.print_table_info()

    #print("data_quality")
    #dq_file.print_table_names()
    #dq_file.print_table_infos()



if __name__ == "__main__":
    main()
