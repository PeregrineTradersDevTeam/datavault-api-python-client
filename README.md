# DataVault API Client Library for Python

[TOC]

## Overview

The DataVault API Client Library for Python is designed to offer a straightforward access to the Intercontinental Exchange (ICE) DataVault data platform. The library offers a way to programmatically interact with the DataVault API to discover and download Replay, PlusTick and Watchlist files and their accompanying reference data. 

The functions in the DataVault API Client Library for Python can be used to create scripts to automate the downloading of the above mentioned files. At the same time, the library offers a convenient command line application that can be use to initiate downloads in an effortless manner.

The library is designed for Python client-application developers that want to interact with the DataVault API from within the Python ecosystem. However, the command line application allows to democratise the access to the API and allow non-technical roles to interact with the API, discover and download their chosen files.

## Features

- Discovers all the files available for download, given a user-defined DataVault API endpoint.
- Supports both synchronous and concurrent download.
- If the concurrent download modality is chosen, splits files larger than an internally calculated multi-part threshold into multiple partitions that are downloaded concurrently and then assembled at the end of the process in a single file.
- Implements a data integrity testing process to guarantee that the downloaded data respects the expected file-specific characteristics as provided by the DataVault API.
- The data is downloaded respecting the directory structure originally found on the DataVault API. This means that the data is saved in a directory tree that branches according to the YEAR/MONTH/DAY/SOURCE_ID/FILE_TYPE structure.
- Users can define the partition size that is used to define the multi-part threshold in concurrent downloads.
- Users can define the number of workers to be used in concurrent downloads and hence to affect the scalability of the download process.
- Supports the specification of the Onyx credentials used to access the DataVault API in dedicated environment variables.

## Setup Instructions

### Requirements/Pre-requisites

#### Software Requirements

This software requires to have Python 3.6 or above installed.

#### Access Requirements

In order to operate the program you need to have an account with the Intercontinental Exchange (ICE) that gives you access to the ICE DataVault platform.

### Installation

#### Cloning the Repository

To use the DataVault API Client Library for Python, first clone the repository on your device using the command below:

```shell
git clone <insert-here-the-link-to-git-repo> datavault-api-python-client
cd datavault-api-python-client
```

This creates the directory *datavault-api-python-client* and clones the content of the repository.

#### Installation Within a Conda Environment

We always recommend to create and use a dedicated environment when installing and running the application.

If you decide to run the DataVault API Client Library for Python within a Conda virtual environment, make sure to:

- Install either [Anaconda](https://www.anaconda.com/products/individual) or [Miniconda](https://docs.conda.io/en/latest/miniconda.html) (recommended), if not already installed.

- Update Conda (`conda update conda`).

- `cd` to the DataVault API Client Library source directory (the directory where the repository was cloned).

Once all the prerequisites are in place, you run the following sequence of commands:

```shell
# Create and activate the production environment
conda env create -f prod_environment.yml
conda activate datavault-client-prod-env

# Install watchlist-api-python-client
python -m pip install .
```

This will create the new environment and install the application in your virtual environment, without affecting your other existing environments, nor any existing Python installation.

At this point you should be able to import `datavault_api_client` from your locally built version:

```shell
$ python  # start an interpreter
>>> import datavault_api_client
>>> print(datavault_api_client.__version__)
1.0.0
```

To return to your root environment:

```
conda deactivate
```

For full details on Conda commands, see the Conda docs [here](https://conda.io/projects/conda/en/latest/).

#### Installing Within a Virtualenv

If you are not using Conda to manage your virtual environments, follow these instructions. You will need to have at least Python 3.6.1 installed on your system.

##### Unix/Mac OS with virtualenv

```shell
# Create a virtual environment
# Use an ENV_DIR of your choice. We will use ~/virtualenvs/dac-prod
# Any parent directories should already exist
python3 -m venv ~/virtualenvs/dac-prod

# Activate the virtualenv
. ~/virtualenvs/dac-prod/bin/activate

# Install the dependencies
python -m pip install -r requirements-prod.txt

# Install watchlist-config-generator
python -m pip install .
```

##### Unix/Mac OS with pyenv + pyenv-virtualenv

Consult the docs for setting up pyenv [here](https://github.com/pyenv/pyenv).

```shell
# Create a virtual environment
# Use an ENV_DIR of your choice. We will use ~/Users/<yourname>/.pyenv/versions/dac-prod
# To specify a pyenv-virtualenv environment we use: pyenv virtualenv <version> <env-name>
pyenv virtualenv 3.8.6 dac-prod

# Activate the virtualenv
pyenv activate dac-prod

# Install the dependencies using the requirements-prod.txt file in the cloned repo
python -m pip install -r requirements-prod.txt

# Install watchlist-config-generator
python -m pip install .
```

##### Windows

Below is brief overview on how to set-up a virtual environment with PowerShell under Windows. For further details, please refer to the [official virtualenv user guide](https://virtualenv.pypa.io/en/latest/).

```shell
# Create a virtual environment
# Use an ENV_DIR of your choice. Use %USERPROFILE% fro cmd.exe
python -m venv $env:USERPROFILE\virtualenvs\dac-prod

# Activate the virtualenv. User activate.bat for cmd.exe
~\virtualenvs\pandas-dev\Scripts\Activate.ps1

# Install the package dependencies
python -m pip install -r requirements-prod.txt

# Install watchlist-config-generator
python -m pip install .
```

## Usage

After installing the DataVault API Client Library for Python, you can decide whether to use the functions in the library to write custom Python scripts to automate the download process, or use the provided command line application to interact with the DataVault API.

In the following section, we will illustrate the usage of the command line application.

### The `datavault` Command

When installing the DataVault API Client Library for Python, a `setuptools` script generates executable wrappers that make possible to directly call the `watchlist` command from your terminal or command prompt. If working on Unix, the `watchilst` command can be called without the need of activating the virtual environment in which the package was originally installed; in Windows, on the other hand, the first step to use the `watchlist` command is to activate the virtual environment in which the library was originally installed. 

The `datavault` help prompt can be invoked by running:

```shell
datavault --help
```

this will return:

```
Usage: datavault [OPTIONS] COMMAND [ARGS]...

  datavault is a command line interface to interact with the DataVault API.

Options:
  --help  Show this message and exit.

Commands:
  get  Discovers and downloads files from the DataVault API server.
```

As shown by the help prompt, at the moment of writing this instructions, the `datavault` command has a `get` sub-command that is used to discover and download files from the DataVault API server.

### Using the `get` Command

The `get` command is invoked by running:

```
datavault get DATAVAULT_ENDPOINT ROOT_DIRECTORY [OPTIONS] 
```

where:

- `DATAVAULT_ENDPOINT` is a URL to a DataVault API endpoint.
- `ROOT_DIRECTORY` is the full path to the root directory where we want to save the downloaded data.

The `get` command supports the following options:

- `-u` or `--username` to specify the Onyx username used to access the DataVault API.
- `-p` or `--password` to specify the Onyx password used to access the DataVault API.
- `--concurrent` to specify that the application should proceed with a concurrent download. This is the default download type of the application.
- `--synchronous` to specify that the application should proceed with a synchronous download. If this flag is selected, the program will download one file at a time. 
- `-s` or `--source` to specify a specific market source ID. If the `--source` option is used, only the files corresponding to the selected source will be downloaded. 
- `--partition-size` to specify the partition size in MiB that should be used to split the larger files in multiple partitions before a concurrent download. The `--partition-size` option influence the definition of the multi-part threshold that is used to determine the cut-off size for files to be downloaded as a whole or to be split in partitions and be downloaded in a fragmented way. If no partition size is specified, the program will use the default size of 5 MiB. This option is used only in case of concurrent downloads.
- `--num-workers` to specify the number of workers to be used by the concurrent download executor. If                                  omitted, the executor will set the number of workers automatically to the minimum between 32 and the number of CPUs in the system being used plus 4. In this way, at least 5 workers are preserved for I/O bound tasks, and no more than 32 CPU cores are used for CPU bound tasks, thus avoiding using very large resources implicitly on many-core machines. This option is used only in case of concurrent downloads.
- `--max-download-attempts` to specify the maximum number of download attempts that should be allowed in case any specific file download fails. 

For example, running:

```shell
datavault get https://api.icedatavault.icedataservices.com/v2/list/2020/12/22 C:/mkt_data
```

will result in the DataVault API Client Library to query all the directories underneath the `https://api.icedatavault.icedataservices.com/v2/list/2020/12/23` DataVault API endpoint and then to download the discovered files in the `C:\mkt_data` directory, respecting the directory tree structure of the DataVault API. Notice that in this case we did not explicitly specified the username and the password using the `--username` and `--password` options; therefore, in this case the application will first check if among the system or environment's environment variables the `ICE_API_USERNAME` and `ICE_API_PASSWORD` environment variables exist, and, if that is the case, will use the username and password encoded in those variables to access the API throughout the crawling and the download phases. 

Let's now assume that you have downloaded the files using the previous command but you noticed that the files belonging to source 207 are missing from the download. You reach out to the ICE Feed Support and they notice that indeed there was an issue with the data upload and they proceed with re-uploading the source 207 data to your profile. Once the data is uploaded, you want to download only the source 207 data, which can be easily done in the command line application by using the `--source` option to specify the source ID as follows:

```shell
datavault get https://api.icedatavault.icedataservices.com/v2/list/2020/12/22 C:/mkt_data -s 207
```

As a result of running this command, the program will look only for source 207 files and, if any is found, it will proceed with downloading only those files. 

Let's now consider another scenario. Your are in a situation where you have a limited bandwidth allocated to your machine but you still want to download some files from the DataVault API to further process them. Instead of using the default concurrent download process, you decide to download the files synchronously (which is the recommended download type in cases where limited or low bandwidth is available). The command to download the data will then look like this:

```
datavault get https://api.icedatavault.icedataservices.com/v2/list/2020/12/22 C:/mkt_data -u Username -p Password --synchronous
```

Note that in this example we used the `--synchronous` flag to specify that we want to download the data synchronously. At the same time, not how, instead of using the environment variables to specify the credentials, in this example we have used the `-u` and `-p` options to explicitly specify the API credentials.

Finally, for our last example, we consider a scenario where we want to download files concurrently using 48 workers (remember that by default the download executor will use a number of workers that is defined as the minimum between 32 and the number of cores in your machine + 4, which means that, for all those machines with more than 32 CPU cores, by default only 32 workers will be used in the concurrent download) and we also want to change the size of the partitions to 4.2 MiB. In this case the command run will look like the following:

```
datavault get https://api.icedatavault.icedataservices.com/v2/list/2020/12/22 C:/mkt_data -u Username -p Password --partition-size 4.2 --num-workers 48
```

here we can see that we used the `--partition-size` option to modify the partition size from the default 5 MiB to 4.2 MiB and, at the same time, we have used the `--num-workers` flag to specify that we want to use 48 workers instead of the default number. 

### Using Environment Variables to Configure Access Credentials 

In alternative to passing every time that a command is run, the credentials to access the DataVault API through the `--username` and `--password` options, the CLI of the DataVault API Client Library allows for credentials to be stored as environment variables.  

The DataVault API Client library supports the following environment variables:

- `ICE_API_USERNAME`: specifies the username used to access the DataVault API.
- `ICE_API_PASSWORD`: specifies the password used to access the DataVault API.

#### Setting Access Credentials as Environment Variables in Windows

If the Windows command prompt is used, the environment variables are set as follows:

```shell
C:\> setx ICE_API_USERNAME <your-username>
C:\> setx ICE_API_PASSWORD <your-password>
```

Using `setx` to set the environment variables, changes the value used in both the current command prompt session and all command prompt sessions that you create <u>after</u> running the command. It does not affect other command shells that are already running at the time you run the command. Alternatively, if you want to affect only the current command prompt session, you can use the `set` command instead of `setx`.

Alternatively, you can set the environment variables through PowerShell as:

```powershell
PS C:\> $Env:ICE_API_USERNAME="<your-username>"
PS C:\> $Env:ICE_API_PASSWORD="<your-password>"
```

This will save the value only for the duration of the current session. To set the variables such that they are accessible to all the future PowerShell sessions, you can add them to your PowerShell profile as explained in the [PowerShell documentation](https://docs.microsoft.com/en-gb/powershell/module/microsoft.powershell.core/about/about_environment_variables?view=powershell-7.1).

Finally, to make the environment variable setting persistent across all Command Prompt and PowerShell sessions, you can store them by using the System application in the Control Panel.

#### Setting Access Credentials as Environment Variables in Linux or macOS

To set the environment variables in Linus or macOS, you can run:

```shell
$ export ICE_API_USERNAME=<your-username>
$ export ICE_API_PASSWORD=<your-password>
```

By setting the environment variables in this way, the two environment variables will be persisted until the end of the shell session, or until the variables are set to a different value.

To persist the environment variables across future sessions, simply set them in the shell's start-up script.

## TODO

### Next Steps

- Test on Linux
- Include pip requirements generated with `pip-tools`
- Include logging

## License

Copyright Peregrine Traders B.V.

Distributed under the terms of the MIT license, DataVault API Python Client  is free and open source software.

## Credits

Python Watchlist Config Generator is developed and maintained by [Jacopo Abbate](mailto:jacopo.abbate@peregrinetraders.com "jacopo.abbate@peregrinetraders.com").