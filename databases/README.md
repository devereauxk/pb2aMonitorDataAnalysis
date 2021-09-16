# POLARBEAR-2A MONITOR DATA ANALYSIS

Kyle Devereaux 2021

Plot upload site: [PB2aMonitorAnalysis bolowiki](https://bolowiki.berkeley.edu/bin/view/Main/PB2aMonitorAnlysis)

Presentations on analysis done with these scripts:

- [initial monitor analysis](https://docs.google.com/presentation/d/1ugGBA4jjo91tdPJiQCl8lcbv7_iC8NMmgRg74NTbaPg/edit?usp=sharing)

- [follow-up monitor analysis](https://docs.google.com/presentation/d/1HsNVAku05eWuxp1pSIDlYrGU5EsoTyBtdEVg8sL-erI/edit?usp=sharing)

- [post-retrofit monitor analysis](https://docs.google.com/presentation/d/1mIaqy1sGAE4TfUy66gD8XjxPQgVM_lmjFqaHCm6gBxo/edit?usp=sharing)


## Background

An analysis of the peripheral temperature sensors of the POLARBEAR-2A (pb-2a) experiment. This included analysis of the timetrend of sensor temperature averages, variances, and other statistical measures, and correlations between component temperatures.

This analysis is significant for the following reasons.

* Timetrend: this analysis gives a method for routinely generating seasonal timetrend plots for any of the sensors within the pb2a experiment. This is especially relevant for the monitoring the behaviors of the sub-K and 4K layers of the cryogenic fridge, the PTC pressures, and ambient weather.

* Retrofit characterization: pb2a underwent a major retrofit over several months in 2020. Using the monitor data from before and after the retrofit, we are able to characterize how effective some changes were. For example the focal plane temperature stability and SC fridge temperature levels were monitored since significant changes were made to those components to facilitate greater heat control.

* Correlation detection: estimating the magnitude of temperature correlations between components within the same shell and across shells of the fridge. Also finding unexpected correlations between components is important for preventing unintended routes of heat transfer.

* Anomaly detection: using both the timetrend and correlations for component temperatures, this analysis also gives an easy way to spot anomalous temperature readings and possible malfunctions of the fridge system.



## Features

This analysis package can produce plots for all time, weather, scan parameter, and temperature sensor data available on the Tostada server. Timetrend plots can be made for single set of data over any time interval or any range of run_id. 2D scatterplots can be generated for any two sets of data with intersecting run_ids for any range of run_id. For both of these options, binning data points by run_ids is supported on the same plot. Error bars, regression, correlation, and statistical analysis can be done on any plot (and any set of data) with results either displayed graphically or printed to the command line. Furthermore, all data analysis done can easily be done routinely for any number and any type of plots.


## Organization and file information

### Main scripts

The python data analysis scripts are in the `main` directory. Of these scripts, main.py is the only file which is intended to be compiled - data_set.py, database_table.py, etc. only contain classes and useful helper methods. Also in the `main` directory is the `main/figures` subdirectory which contains, unorganized, all plots generated.

#### `main.py`
File, which when compiled, generates the desired plots as specified within the `main.main()` method. This is the file where all the data analysis is centralized. See "Instructions" for more information.

#### `data_set.py`
Contains the definition of the data_set class. This class stores up to two sets of data and two sets of errors. Any column that appears in any of the databases described below can be added into a data set. As a result any two sets of data which exist - time, weather, sensor temperature, or scan parameter - can be compared and analyzed with this class.

The class is constructed by specifying the name of a column of one database and an other column of another database, the database_table objects for these two databases, and if the errors for these two sets of data should also be stored. These databases are read using SQLite3, and their data is stored as instance variable arrays in the class. The data_set class contains several methods which allow us to perform useful analysis on these two sets of data. The most useful method is the make_plot method which plots one set of data wrt the other set (with many options). There are also methods which fits arbitrary functions to the data, prints useful statistics, computes the correlation of the data sets, discard outliers, makes a residual plot for linear fit, and more.

#### `database_table.py`
Stores the SQLite3 pointer to a specified SQL database within a .db file. Constructed by giving the name/directory of the .db file and the name of the database desired. Contains several basic SQLite3 operations useful for reading the data from the table, managing the .db file, and printing information about the database table.

#### `database_file.py`
Stores the SQLite3 pointer to a specified SQL .db file. Contains methods useful for printing the contents of the .db file.

#### `monitor_analysis.py`
A deprecated version of main.py kept for backwards compatibility. Contains several analysis methods, and originally used for debugging and hard-coding plot generation without full utilization of the data_set methods.

### Databases

Databases are stored in the `databases` directory. The databases used for current analysis are stored in the `databases/current` directory. Deprecated databases (not containing current data) are stored in the `databases/pb2a-yyyymmdd` directories where yyyy/mm/dd corresponds to the date the databases in that directory were compiled. These databases contain all the data used in our analysis and were obtained from the Tostada server (tostada1.berkeley.physics.edu).

#### `pb2a_monitor.db`
All temperature sensor data is recorded in the pb2a_montior.db files. This data is indexed by (run_id, run_subid) and includes for each sensor the mean, median, min/max, rms, std, and variance of the temperature for that (run_id, run_subid).

#### `pb2a_data_quality.db`
Scan parameters including the azimuth, azimuth acceleration, elevation, scan speed, scan acceleration, scan frequency and locations of the sun and moon.

#### `pb2a_apex.db`
Ambient weather conditions for the site. This includes outside temperature, pressure, humidity and wind information.

#### `pb2a_runid.db`
Maps a given (run_id, run_subid) to the corresponding modified Julian time and date (mjd) for the beginning and end of each scan.

### Other files

There are also three slides decks given in this directory in both .pdf and .pptx format. These slide decks summarize and constitute all the analysis completed.


## Instructions
As is, running `main.py` will produce several seasonal timetrend plots for sub-K, 4K, PTC, ambient weather, and SC data over the time intervals 'week', 'month', and 'since retrofit'. Previous generations of these plots can be found at [PB2aMonitorAnalysis bolowiki](https://bolowiki.berkeley.edu/bin/view/Main/PB2aMonitorAnlysis).

To produce other plots, the `main()` method should be modified. The following is a rough procedure on how a plot can be produced by a script written in `main()`.
1. Instantiate `database_table` objects for each database with data you desire to analyze - this involves specifying the file location, name, and table name for each table.
2. If you want to plot only over a range of dates, use the `get_runids_between_dates` helper method to get an array of the corresponding run_ids.
3. Create a modified database (which includes only this range of dates) by calling the `make_new_db_file_set_runids` method.
4. Instantiate a `data_set` object using the modified database(s), specifying which two sets of data from the database(s) you would like to constitute the data set, and if error data should be included.
5. Perform modifications on the `data_set` object. This may include discarding outlier points based on their value or error.
6. Call any desired data analysis methods of the `data_set` object. This may include performing a regression or calculating statistics on the data.
7. Generate the plot for the `data_set` by calling the `data_set.make_plot()` function. You may specify the directory to save the plot to, if error bars should be included, if the plot is a timetrend, and what binnings the run_ids should take on - if any.
