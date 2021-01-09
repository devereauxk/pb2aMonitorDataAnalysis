import matplotlib
import matplotlib.pyplot as plt
import sqlite3

f = open("correlations.txt", "r")
corr_matrix = f.read()
corr_matrix = corr_matrix[1:len(corr_matrix)-1]
corr_matrix = corr_matrix.split()
for i in range(len(corr_matrix)):
    corr_matrix[i] = corr_matrix[i].replace("[", "")
    corr_matrix[i] = corr_matrix[i].replace("]", "")
    corr_matrix[i] = corr_matrix[i].replace(",", "")
    if corr_matrix[i] != "nan":
        corr_matrix[i] = float(corr_matrix[i])

b = []
i = 0
while i < len(corr_matrix):
    temp = []
    for j in range(1895):
        temp.append(corr_matrix[i+j])
    b.append(temp)
    i += 1895
corr_matrix = b

print("File loaded")
print(len(corr_matrix))

high_corr = []
for i in range(len(corr_matrix)):
    for j in range(len(corr_matrix)):
        if corr_matrix[i][j] != "nan" and abs(corr_matrix[i][j]) > 0.9 and abs(corr_matrix[i][j]) < 1.0:
            high_corr.append([i, j])

print(high_corr)
print(len(high_corr))


def show_graph(x, y):
    c = sqlite3.connect("pb2a_monitor.db-20200814")
    #has one table: "pb2a_monitor"

    cursor = c.execute("PRAGMA table_info(pb2a_monitor)")
    x_data = []
    y_data = []
    for row in cursor:
        if row[0] == x:
            x_data = [this_data[0] for this_data in c.execute("SELECT {} from pb2a_monitor".format(row[1]))]
            x_name = row[1]
        if row[0] == y:
            y_data = [this_data[0] for this_data in c.execute("SELECT {} from pb2a_monitor".format(row[1]))]
            y_name = row[1]

    new_x = []
    new_y = []
    for i in range(len(x_data)):
        if x_data[i] is not None and y_data[i] is not None:
            new_x.append(x_data[i])
            new_y.append(y_data[i])

    plt.xlim(min(new_x), max(new_x))
    plt.ylim(min(new_y), max(new_y))
    plt.scatter(new_x, new_y, label = "Data")
    #plt.plot(x, fit_y(x), label = "Fit")
    plt.legend()
    plt.xlabel(x_name)
    plt.ylabel(y_name)
    plt.show()

show_graph(1885, 30)
