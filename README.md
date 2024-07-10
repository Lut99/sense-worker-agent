# sense-worker-agent
A test worker agent that will be capable of simulating traffic for experiments with the SENSE orchestrator.

This project is written in [Python](https://python.org) and is based on the [SENSE Northbound API pip-package](https://pypi.org/project/sense-o-api/).


## Installation
To prepare your environment, ensure you have a working Python version first. We recommend installing 3.9.18, as `sense-o-api` is guaranteed to work with that version. If you have [pyenv](https://github.com/pyenv/pyenv), then the project's [`.pyenv-version`](./.python-version) should automatically cause pyenv to use the correct version.

Then we recommend setting up a virtual environment. Run the following to create the virtual environment:
```sh
python3 -m venv ./venv
```
and then the following command to activate it:
```sh
. venv/bin/activate
```
Note that the second command has be run for every new terminal instance.

Then, you can install the project's dependencies. Simply run:
```sh
pip3 install -r requirements.txt
```
to install the requirements.


## Usage

