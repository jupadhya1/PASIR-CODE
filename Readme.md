# PASIR Classr 

Lightweight web-based JSON-RPC API for machine-learning-based text classification and model management, written in Python. This API is part of the IBM PASIR ecosystem and used for incident ticket classification.

## Installation

1. Checkout from Git: 
  * `git clone https://gitlab.zurich.ibm.com/dla/pasir-classr.git`
2. Run installer:
  * `cd pasir-classr && chmod +x install.sh`
  * `sudo ./install.sh` (This will take 5-15 minutes)
3. Configure the server:
  * `cd /opt/pasir-classr/config`
  * ```cp defaults.cfg `hostname -f`.cfg```
  * ```nano `hostname -f`.cfg```
4. Start the server:
  * `/etc/init.d/pasir-classr start`
5. Check if API resources are on-line:
  * `http://<hostname>:8080` &ndash; Welcome page
  * `http://<hostname>:8080/api/browse` &ndash; API docs
  * `http://<hostname>:8080/classify` &ndash; User test interface
5. Check the logs:
  * `tail -f -n 25 <LOG_PATH>/classr.log` &ndash; See config for `LOG_PATH` 


## Features

* Classification of incident tickets into pre-defined categories based on the unstructured description and resolution text.
* Management and automatable training of classifier models on variable sets of training data.
* Exchange and synchronization of classifier objects remotely over HTTP.
* Configurable automatic clean-up of old data objects to prevent disk fill-up
* API-key authentication.
* Multi-server configuration.

## Algorithms

Current implementation uses the following machine learning algorithms:

* __Gradient Boosting Machine__ (GBM) from the `xgboost` library
* __Paragraph Vectors__ &ndash; implementation in C included

## Extension

The code enables simple integration of additional text classifier modules with a Python interface. To refactor an existing classifier to fit this API, just keep the following design principles in mind:

1. Create a fully stateless interface. It should support two operations and receive all necesasry configuration as call parameters: 
  * `train()` to create a classifier object on a specific set of training data,
  * `classify()` to use such an object for predictions on some input data.
2. Input and output are CSV files. Paths and input/output column names arrive as run-time parameters.
3. The algorithm is assigned a working directory every time it's invoked where it's free to create any files (including the output itself).
4. If the algorithm needs to read files other than the CSV input, (e.g. an object dump), think about it as a Resource dependency. Your algorithm can specify such dependencies and will receive the respective paths at invocation time. Resources can be added to the API at installation or runtime.
5. The algorithm is provided with a logger object at invocation. Use that instead of `print`s.
6. A progress update callback is also provided, which allowsAPI users to track the progress of the execution, which is convenient if you run for long.
6. If something isn't right with an operation, simply raise an `Exception`.


## Dependencies

Dependencies are installed automatically by the installer script:

* `Flask`
* `Flask-JSONRPC`
* `Flask-HTTPAuth`
* `xgboost`
* `pandas`
* `SciPy`
* `sklearn`
* `requests`
* `JayDeBeApi`, `JPype1` and Java 7 (e.g. IBM Java or OpenJDK) for DB2 connectivity


