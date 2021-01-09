import tensorflow as tf
import numpy as np
import matplotlib.pyplot as plt
import math

# Create noisy data
t_data = np.linspace(0, 2*np.pi, num=1000)
x_data = np.cos(t_data) + 0.1*np.random.normal(size=1000)
y_data = np.sin(t_data) + 0.1*np.random.normal(size=1000)
comp_data = []
for i in range(len(t_data)):
    comp_data.append([x_data[i], y_data[i]])
comp_data = np.array(comp_data)

print(comp_data.shape)
print(comp_data)

#normalization

#what about regularization, changing the learning rate, batch size, etc.
learning_rate = 0.01
epochs = 200
batch_size = 100
label_name = None
classification_threshold = None

model = tf.keras.Sequential()
model.add(tf.keras.layers.Flatten(input_shape=(1,)))
model.add(tf.keras.layers.Dense(units = 128, activation = 'relu'))
model.add(tf.keras.layers.Dense(units = 128, activation = 'relu'))
model.add(tf.keras.layers.Dense(units = 2, activation = 'linear'))

#model.compile(loss='mse', optimizer="adam")
model.compile(optimizer=tf.keras.optimizers.RMSprop(lr=learning_rate),
                loss="mean_squared_error",
                metrics=[tf.keras.metrics.RootMeanSquaredError()])

model.summary()

model.fit(t_data, comp_data, epochs=epochs, batch_size=batch_size, shuffle=True, verbose=1)

#loss graph
def plot_the_loss_curve(epochs, rmse):
  """Plot the loss curve, which shows loss vs. epoch."""

  plt.figure()
  plt.xlabel("Epoch")
  plt.ylabel("Root Mean Squared Error")

  plt.plot(epochs, rmse, label="Loss")
  plt.legend()
  plt.ylim([rmse.min()*0.97, rmse.max()])
  plt.show()

# Compute the output
r_predicted = model.predict(t_data)
print(r_predicted.shape)
x_predicted = []
y_predicted = []
for i in range(len(r_predicted)):
    x_predicted.append(r_predicted[i][0])
    y_predicted.append(r_predicted[i][1])
x_predicted = np.array(x_predicted)
y_predicted = np.array(y_predicted)

#is there a way to smoothen it without machine learning???

# Display the result
plt.xlim(-3, 3)
plt.ylim(-3, 3)
plt.scatter(x_data, y_data, 10)
plt.plot(x_predicted, y_predicted, 'r', linewidth=4)
plt.grid()
plt.show()



def create_model(my_learning_rate, feature_layer, my_metrics):
  """Create and compile a simple classification model."""
  # Most simple tf.keras models are sequential.
  model = tf.keras.models.Sequential()

  # Add the feature layer (the list of features and how they are represented)
  # to the model.
  model.add(feature_layer)

  # Funnel the regression value through a sigmoid function.
  model.add(tf.keras.layers.Dense(units=1, input_shape=(1,),
                                  activation=tf.sigmoid),)

  # Call the compile method to construct the layers into a model that
  # TensorFlow can execute.  Notice that we're using a different loss
  # function for classification than for regression.
  model.compile(optimizer=tf.keras.optimizers.RMSprop(lr=my_learning_rate),
                loss=tf.keras.losses.BinaryCrossentropy(),
                metrics=my_metrics)

  return model


def train_model(model, dataset, epochs, label_name,
                batch_size=None, shuffle=True):
  """Feed a dataset into the model in order to train it."""

  # The x parameter of tf.keras.Model.fit can be a list of arrays, where
  # each array contains the data for one feature.  Here, we're passing
  # every column in the dataset. Note that the feature_layer will filter
  # away most of those columns, leaving only the desired columns and their
  # representations as features.
  features = {name:np.array(value) for name, value in dataset.items()}
  label = np.array(features.pop(label_name))
  history = model.fit(x=features, y=label, batch_size=batch_size,
                      epochs=epochs, shuffle=shuffle)

  # The list of epochs is stored separately from the rest of history.
  epochs = history.epoch

  # Isolate the classification metric for each epoch.
  hist = pd.DataFrame(history.history)

  return epochs, hist
