from monitor_sql import *
from database_file import *
from database_table import *

path = "../databases/current/"

from os import walk
_, _, filenames = next(walk(path))

for name in filenames:
    temp = database_file(path+name)
    temp.print_table_infos()

monitor = database_file(path+"pb2a_runid.db")
runids = []
for line in monitor.database_tables[0].gen_table():
    runids.append(line[0])
print("max runid: " + str(max(runids)))
