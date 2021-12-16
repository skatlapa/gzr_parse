import sys
import subprocess
import configparser
import os
import time
import shutil
import json

# non-stdlib
import click
import pandas as pd

from pathlib import Path

class GZR_PARSE(object):


    def __init__(self, looker_ini, source_csv, gzr_instance, start_path):
        self.source_csv = source_csv
        self.gzr_instance = gzr_instance
        self.start_path = start_path
        self.content_names = set()


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
                        trash_path = Path(f"Bad_Jsons_{self.gzr_instance}")
                        try:
                            if parsed_json['deleted_at'] is not None:
                                trash_path.mkdir(exist_ok=True)
                                shutil.move(my_json_path, trash_path)
                                print(f"file {my_json_path} has been moved to {trash_path} as it's trashed")
                        except KeyError as ke:
                            print(f"Key Error: File {my_json_path} cannot be moved to {trash_path} due to {ke}")
                        except Exception as e:
                            print(f"General Error: File {my_json_path} cannot be moved to {trash_path} due to {e}")


    def cleanse_jsons(self):
        root_dir = self.start_path
        for subdir, dirs, files in os.walk(root_dir):
            for file in files:
                split = file.split('.')
                if split[-1] == 'json':
                    my_json_path = os.path.join(subdir, file)
                    with open(my_json_path, 'r+') as myjson_fd:
                        parsed_json = json.load(myjson_fd)
                        try:
                            if parsed_json["title"]:
                                if parsed_json["title"] not in self.content_names:
                                    self.content_names.add(parsed_json["title"])
                                else:
                                    #suffix = parsed_json["space"]["name"].replace("\\ ", "_")  #TODO: Check if spaces need to be stripped
                                    suffix = parsed_json["space"]["name"]
                                    orig_title = parsed_json["title"]
                                    parsed_json["title"] = f"{orig_title}_{suffix}" # To preserve spaces with python F-strings
                                    myjson_fd.seek(0)
                                    json.dump(parsed_json, myjson_fd)
                                    myjson_fd.truncate()
                                    self.content_names.add(parsed_json["title"])    # To check
                        except KeyError as ke:
                            print(f"Key Error: File {my_json_path} cannot be parsed due to {ke}")
                        except Exception as e:
                            print(f"General Error: File {my_json_path} cannot be parsed due to {e}")


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
            subprocess.run(gzr_command, timeout=7, shell=True, check=True, capture_output=True)
        except subprocess.TimeoutExpired as e:
            self.failed_file_list(gzr_command)
            print(f"Timeouted Error:{gzr_command}: {e}")
        except Exception as e:
            self.failed_file_list(gzr_command)
            print(f"Errored:{gzr_command}: {e}")


    def iteration(self, df, client_id, client_secret):
        for index, row in df.iterrows():
            ctitle = row['Title']
            save_location = row['Department']
            if pd.isnull(df['ID'][index]):
                continue
            cid = int(row['ID'])
            if row['Type'] == 'looks' or row['Type'] == 'Look' or row['Type'] == 'look':
                ctype = 'Look'
            elif row['Type'] == 'dashboards' or row['Type'] == 'Dashboard' or row['Type'] == 'dashboard':
                ctype = 'Dashboard'
            self.run_gzr_command(ctype, cid, save_location, client_id, client_secret)
            print(f'downloading {ctitle} to {self.start_path}/{save_location}/dashboard')


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
    #host_var = config.get('main', 'base_url')
    #port_var = host_var.split(':')[-1]
    df = pd.read_csv(source_csv)

    gzr_parse.create_folders(df, ['Department'], ['look', 'dashboard'], 'Dept_Folders')
    gzr_parse.iteration(df, client_id_var, client_secret_var)
    gzr_parse.validate_jsons()
    gzr_parse.cleanse_jsons()
    #print(gzr_parse.content_names) #TODO: Remove to debug

if __name__ == '__main__':
    sys.exit(main())
