import pandas as pd
import subprocess
from pathlib import Path
import configparser
import os
import time
import shutil
import json
import click
import sys

class GZR_PARSE(object):


    def __init__(self, looker_ini, source_csv, gzr_instance, start_path):
        self.source_csv = source_csv
        self.gzr_instance = gzr_instance
        self.start_path = start_path


    def validate_jsons(self):
        root_dir = self.start_path
        for subdir, dirs, files in os.walk(root_dir):
            for file in files:
                split = file.split('.')
                if split[-1] == 'json':
                    my_json_path = os.path.join(subdir, file)
                    with open(my_json_path, 'r') as myjson:
                        data = myjson.read()
                        parsed_json = json.loads(data)
                        if parsed_json['deleted_at'] is not None:
                            trash_path = Path(f"Bad_Jsons_{self.gzr_instance}")
                            trash_path.mkdir(exist_ok=True)
                            shutil.move(my_json_path, trash_path)
                            print(f"file {my_json_path} has been moved to {trash_path} as it's trashed")


    def failed_file_list(self, file_name):
        with open('dodgy_entries.txt', 'a+') as w_fd:
            w_fd.write(f"{file_name}\n")


    def create_folders(self,
            dataframe,
            agg_columns: list,
            sub_folders: list,
            root_path: str):

        p = Path(root_path)

        series = dataframe[agg_columns].agg('-'.join, 1)

        for person in series:
            trg_path = p.joinpath(person)
            if not trg_path.is_dir():
                trg_path.mkdir(parents=True)

            for path in sub_folders:
                if not trg_path.joinpath(path).is_dir():
                    trg_path.joinpath(path).mkdir()
        return series


    def run_gzr_command(self,
            ctype: str,
            cid: int,
            save_location: str,
            client_id: str,
            client_secret: str):

        save_location = save_location.replace(' ', '\\ ').replace('&', '\\&').replace("'", "\\'").replace('(', '\\(').replace('[', '\\[').strip()
        cid = int(cid)  # To avoid float values
        if ctype == 'Dashboard':
            content_type = 'dashboard'
        elif ctype == 'Look':
            content_type = 'look'

        gzr_command = f"gzr {content_type} cat {cid} --host {self.gzr_instance} --client-id {client_id} --client-secret {client_secret} --no-ssl --dir {self.start_path}/{save_location}/{content_type}/"
        #print(gzr_command)

        try:
            subprocess.run(gzr_command, timeout=10, shell=True, check=True, capture_output=True)
        except subprocess.TimeoutExpired as e:
            self.failed_file_list(gzr_command)
            print(f"Timeout Errored:{gzr_command}: {e}")
        except Exception as e:
            self.failed_file_list(gzr_command)
            print(f"Errored:{gzr_command}: {e}")


    def iteration(self, df, client_id, client_secret):
        for index, row in df.iterrows():
            if row['Type'] == 'looks':
                ctype = 'Look'
                ctitle = row['Title']
                save_location = row['Department']
                cid = int(row['ID'])
                self.run_gzr_command(
                    ctype=ctype,
                    cid=cid,
                    save_location=save_location,
                    client_id=client_id,
                    client_secret=client_secret)
                print(
                    f'downloading {ctitle} to {self.start_path}/{save_location}/look')
            elif row['Type'] == 'dashboards':
                ctype = 'Dashboard'
                ctitle = row['Title']
                save_location = row['Department']
                cid = row['ID']
                self.run_gzr_command(
                    ctype=ctype,
                    cid=cid,
                    save_location=save_location,
                    client_id=client_id,
                    client_secret=client_secret)
                print(
                    f'downloading {ctitle} to {self.start_path}/{save_location}/dashboard')


@click.command()
@click.option('-l', '--looker_ini', help='The looker.ini file with the credentials needed to establish connections')
@click.option('-f', '--source_csv', help='CSV with source dashboards information')
@click.option('-i', '--gzr_instance', help='IP || Hostname. Target GZR instance to retrieve dashboard jsons')
@click.option('-p', '--start_path', default='Dept_Folders', help='The folder to house the downloaded dashboards.')
def main(looker_ini, source_csv, gzr_instance, start_path):

    gzr_parse = GZR_PARSE(looker_ini, source_csv, gzr_instance, start_path)

    gzr_parse.source_csv = source_csv
    gzr_parse.gzr_instance = gzr_instance
    gzr_parse.start_path = start_path

    config = configparser.ConfigParser()
    config.read(looker_ini)
    client_id_var = config.get('main', 'client_id')
    client_secret_var = config.get('main', 'client_secret')
    host_var = config.get('main', 'base_url')
    port_var = host_var.split(':')[-1]
    df = pd.read_csv(source_csv)

    gzr_parse.create_folders(df, ['Department'], ['look', 'dashboard'], 'Dept_Folders')
    gzr_parse.iteration(df, client_id_var, client_secret_var)
    gzr_parse.validate_jsons()

if __name__ == '__main__':
    sys.exit(main())
