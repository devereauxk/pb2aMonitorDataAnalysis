from monitor_sql import *
from database_file import *
from database_table import *

path_current = "../databases/current"

runid = database_file(path_current + "pb2a_runid.db")
runid.print_table_infos()

runid_g3 = database_file(path_current + "pb2a_runid_g3.db")
runid_g3.print_table_infos()

slowdaq = database_file(path_current + "pb2a_slowdaq.db")
slowdaq.print_table_infos()
