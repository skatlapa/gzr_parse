# What's this?! 
Script to bulk download dashboards via `gzr cat` and arrange the downloaded content into a specific folder structure: Department_Name/(dashboards,looks)/content.json 

The downloaded content is to be used for importing content onto a target looker instance via ldeploy (looker_deployer) tool.

Requires: https://github.com/looker-open-source/gzr and https://github.com/looker-open-source/looker_deployer and python3 (3.7 ideally.. just coz)

# Usage

## Install requirements
pip3 install -r requirements.txt

## Set-up virtual environment to install all the packages
virtualenv .venv <br />
source .venv/bin/activate

## Run the script
python3 gzr_automation.py -l /path/to/my_looker.ini -f /path/to/dashboard_info.csv -i <IP || hostname>
