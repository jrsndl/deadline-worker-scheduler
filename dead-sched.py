from anyascii import anyascii
import argparse
import csv
import datetime
import json
import os
import platform

import re
import subprocess
import sys
from logging import getLogger, StreamHandler

import pprint

def make_logging(lvl):
    lvls = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
    lvl = lvl.upper()
    if lvl not in lvls:
        lvl = "WARNING"

    logger = getLogger(__name__)
    handler = StreamHandler()
    handler.setLevel(lvl)
    logger.setLevel(lvl)
    logger.addHandler(handler)

    return logger

def get_deadline_executable():
    pth = None
    if os.environ.get("DEADLINE_PATH", None):
        pth = os.environ["DEADLINE_PATH"] + os.sep + "deadlinecommand"
        if platform.system() == "Windows":
            pth += ".exe"
    return pth.replace("\\", "/") if pth else None

def external_execute(args):
    kwargs = {
        "stdout": subprocess.PIPE,
        "stderr": subprocess.PIPE,
        "text": True  # Automatically decodes to string
    }
    if platform.system().lower() == "windows":
        kwargs["creationflags"] = (
                subprocess.CREATE_NEW_PROCESS_GROUP
                | getattr(subprocess, "DETACHED_PROCESS", 0)
                | getattr(subprocess, "CREATE_NO_WINDOW", 0)
        )
    popen = subprocess.Popen(args, **kwargs)
    popen_stdout, popen_stderr = popen.communicate()
    if popen_stdout:
        #print(popen_stdout)
        pass
    if popen_stderr:
        #print(f"Error: {popen_stderr}")
        pass

    # Check the return code
    if popen.returncode != 0:
        #print(f"Command failed with return code {popen.returncode}")
        pass

    return popen_stdout, popen_stderr, popen.returncode

class WorkerSchedule:
    def __init__(self, args, logger):
        """
        Some arguments to be passed in args:
        {'check': True,
         'comments_only': False,
         'dry': False,
         'use_comments': False,
         'workstations_render': False}
        """
        self.args = args

        # used to speed up the debug
        # can read cached json file instead of deadline read
        self.skip_reading_from_Deadline = False

        self.logger = logger
        self.deadline_path = None
        self.current_folder = self.get_current_folder()

        self.csv_root = None
        self.path_ignore_people = None
        self.path_ignore_workers = None
        self.worker_info_folder = None

        # read team data
        self.get_setup()
        self.current_team_file = None
        self.get_current_team_file()
        self.team_data = self.get_current_team_data()

        self.checks_ok = self.init_checks()
        if not self.checks_ok:
            self.logger.error("-=Failed=-")
            return

        # read machines and users to be ignored (will not render)
        self.ignore_people = []
        self.get_ignored_names()
        self.ignore_machines = []
        self.get_ignored_machines()

        # read workers info from Deadline
        self.workers = []
        self.workers_info = {}
        self.get_deadline_info()
        if not self.workers or not self.workers_info:
            self.logger.error("Reading worker info from Deadline failed.")
            self.checks_ok = False

        # workers_parsed to contain necessary info from parsed workers_info
        self.users_to_workers, self.workers_parsed = self.parse_description_from_info()
        # this sets if user is active or not by matching team name to worker parsed artist name
        # also sets team_user_found for workers with matching user (team member)
        self.assign_team_member_to_worker_by_name()

        # decide if worker should render 24/7, overnight, or not render at all
        # write the result to worker comment in workers_parsed
        self.assign_comment_to_workers()

        # write the worker comment back to deadline
        self.comment_to_deadline()

        # deadline enable or disable workers by the comment, also store in workers_parsed
        self.slave_enabled_by_comment()

        # read back enabled / disabled from Deadline, compare to expected and report
        self.check_if_set()

        # write workers_parsed to json file with today date
        self.workers_parsed_to_json()

        my_json = self.worker_info_folder + os.sep + datetime.datetime.now().strftime("%y%m%d") + ".json"
        self.worker_info_to_json(my_json)

    def str_to_bool(self, s):
        if s == 'True':
            return True
        elif s == 'False':
            return False
        else:
            raise ValueError

    def _read_deadline_info(self):

        _workers = []
        _workers_info = {}

        _workers = self.get_workers()
        if not _workers:
            self.logger.error("Reading worker names from Deadline failed.")
            return _workers, _workers_info
        self.logger.info(f"Reading {len(_workers)} worker's info from Deadline. This can take some time...")
        for worker in _workers:
            #print(".", end="")
            _workers_info[worker] = self._get_worker_info(worker)

        return _workers, _workers_info

    def get_deadline_info(self):

        my_json = self.worker_info_folder + os.sep + datetime.datetime.now().strftime("%y%m%d") + ".json"
        if self.skip_reading_from_Deadline:
            # try to read from cached json first
            try:
                with open(my_json, "r") as json_file:
                    self.workers_info = json.load(json_file)
                    self.workers = list(self.workers_info.keys())
            except Exception as e:
                self.logger.info(f"Failed to read worker info from json file: {my_json}. {e}")

        if self.workers_info == {}:
            self.workers, self.workers_info = self._read_deadline_info()
            if not self.workers:
                self.logger.error("Reading worker names from Deadline failed.")
                return
            if self.workers_info == {}:
                self.logger.error("Reading workers info from Deadline failed.")
                return
            if self.skip_reading_from_Deadline:
                self.worker_info_to_json(my_json)
            self.logger.info(f"Info about {len(self.workers)} was read from Deadline.")


    def worker_info_to_json(self, my_json):
        try:
            with open(my_json, "w") as json_file:
                json.dump(self.workers_info, json_file, indent=4)
        except Exception:
            self.logger.error(f"Failed to write worker info to json file: {my_json}")

    def workers_parsed_to_json(self):
        my_json = self.worker_info_folder + os.sep + datetime.datetime.now().strftime("%y%m%d") + "_parsed.json"
        try:
            with open(my_json, "w") as json_file:
                json.dump(self.workers_parsed, json_file, indent=4)
        except Exception:
            self.logger.error(f"Failed to write worker info to json file: {my_json}")


    def get_ignored_names(self):

        try:
            with open(self.path_ignore_people, "r", encoding="utf-8") as f:
                lines = f.readlines()
                for line in lines:
                    if line != "":
                        line = line.replace(".", " ").strip()
                        self.ignore_people.append(line)
        except FileNotFoundError:
            self.logger.warning(f"Warning: File '{self.path_ignore_people}' not found.")
        except UnicodeDecodeError:
            self.logger.warning(f"Warning: Could not decode file '{self.path_ignore_people}'")
        except Exception as e:
            self.logger.warning(f"Warning: Unexpected error while reading '{self.path_ignore_people}'")


    def get_ignored_machines(self):

        try:
            with open(self.path_ignore_workers, "r", encoding="utf-8") as f:
                lines = f.readlines()
                for line in lines:
                    if line != "":
                        s = line.strip().split(" ")
                        self.ignore_machines.append(s[0])
        except FileNotFoundError:
            self.logger.warning(f"Warning: File '{self.path_ignore_workers}' not found.")
        except UnicodeDecodeError:
            self.logger.warning(f"Warning: Could not decode file '{self.path_ignore_workers}'")
        except Exception as e:
            self.logger.warning(f"Warning: Unexpected error while reading '{self.path_ignore_workers}'")

    def init_checks(self):
        checks_ok = True
        self.deadline_path = get_deadline_executable()
        if not self.deadline_path:
            self.logger.error("Deadline executable not found. Please set DEADLINE_PATH environment variable.")
            checks_ok = False

        if not self.csv_root or not os.path.exists(self.csv_root):
            self.logger.error(f"Path to CSV root not found. Please set path_team in setup.json to valid path.")
            checks_ok = False
        if not self.path_ignore_people or not os.path.exists(self.path_ignore_people):
            self.logger.warning(f"Path to ignore_people.txt not found. Please set path_ignore_people in setup.json to valid path.")
            checks_ok = True
        if not self.path_ignore_workers or not os.path.exists(self.path_ignore_workers):
            self.logger.warning(f"Path to ignore_workers.txt not found. Please set path_ignore_workers in setup.json to valid path.")
            checks_ok = True

        if not self.current_team_file or not os.path.exists(os.path.join(self.csv_root, self.current_team_file)):
            self.logger.error(f"Current team file YYMMDD.csv not found at folder {self.csv_root}")
            checks_ok = False
        if not self.team_data or self.team_data == {}:
            self.logger.error(f"Problem with Current team file  {self.current_team_file}")
            checks_ok = False

        if not self.worker_info_folder:
            self.logger.error(f"{self.worker_info_folder} not defined.")
        if not os.path.exists(self.worker_info_folder):
            try:
                os.makedirs(self.worker_info_folder)
            except OSError as e:
                self.logger.error(f"Failed to create folder {self.worker_info_folder}: {e}")

        return checks_ok

    def get_current_folder(self):
        # Get the current file location, even if the script is frozen (e.g., built with pyinstaller)
        if getattr(sys, 'frozen', False):
            # If the application is run as a frozen executable
            current_file_location = sys.executable
        else:
            # If the application is run in a standard Python environment
            current_file_location = os.path.abspath(__file__)
        current_file_location = os.path.dirname(current_file_location)
        self.logger.debug(f"Current file location: {current_file_location}")
        return current_file_location

    def get_setup(self):

        def to_absolute_path(relative):
            if relative.startswith("."):
                relative = self.current_folder + relative[1:]
            return relative

        # set sane defaults
        self.csv_root = self.current_folder
        self.path_ignore_people = self.current_folder + os.sep + "ignore_people.txt"
        self.path_ignore_workers = self.current_folder + os.sep + "ignore_machines.txt"
        self.worker_info_folder = self.current_folder + os.sep + "worker_info"

        # now try to read it from setup.json
        try:
            with open(self.current_folder + os.sep + "setup.json", "r") as json_file:
                setup = json.load(json_file)
                self.csv_root = setup.get("path_team", self.csv_root)
                self.csv_root = to_absolute_path(self.csv_root).replace("\\", "/")
                self.path_ignore_people = setup.get("path_ignore_people", self.path_ignore_people)
                self.path_ignore_people = to_absolute_path(self.path_ignore_people)
                self.path_ignore_workers = setup.get("path_ignore_workers", self.path_ignore_workers)
                self.path_ignore_workers = to_absolute_path(self.path_ignore_workers)
                self.worker_info_folder = setup.get("worker_info_folder", self.worker_info_folder)
                self.worker_info_folder = to_absolute_path(self.worker_info_folder).replace("\\", "/")
        except:
            pass

        if self.csv_root.endswith("/"):
            self.csv_root = self.csv_root[:-1]
        if self.worker_info_folder.endswith("/"):
            self.worker_info_folder = self.worker_info_folder[:-1]

        self.logger.debug(f"path_team: {self.csv_root}")
        self.logger.debug(f"path_ignore_people: {self.path_ignore_people}")
        self.logger.debug(f"path_ignore_workers: {self.path_ignore_workers}")
        self.logger.debug(f"worker_info_folder: {self.worker_info_folder}")

    def get_current_team_file(self):
        # Define the current date
        today = datetime.datetime.today().date()

        # Prepare regex pattern to match filenames like 'data YYMMDD.csv'
        filename_pattern = r"^(\d{6})\.csv$"

        # Store matched files and parsed dates
        files_with_dates = []

        # Walk through the directory for matching files
        for filename in os.listdir(self.csv_root):
            match = re.match(filename_pattern, filename)
            if match:
                date_str = match.group(1)  # Extract YYMMDD
                try:
                    # Convert to date object
                    file_date = datetime.datetime.strptime(date_str, "%y%m%d").date()
                    files_with_dates.append((filename, file_date))
                except ValueError:
                    continue

        # Find the file closest to today's date
        files_with_dates.sort(key=lambda x: abs(x[1] - today))

        # Return the closest file and its date
        self.current_team_file = files_with_dates[0][0] if files_with_dates else None

    def get_current_team_data(self):
        """
        Parses a CSV file containing team data to map team member names to their
        active/inactive status.

        The function reads a CSV file in the specified root directory to extract
        the team's current status. If the file does not exist or is invalid, the
        function returns an empty dictionary. For each row in the CSV file, the
        name is extracted from the first column, and the status is compared
        against a predefined list of inactive statuses ("", "paused", "off").

        :return: A dictionary that maps team member names (lowercased) to their
            active status. The active status is ``True`` if the member is active
            and ``False`` otherwise. If no valid file is provided, or rows do not
            meet the expected format, an empty dictionary is returned.
        :rtype: dict
        """
        inactive = ["", "paused", "off"]
        if not self.current_team_file:
            return {}
        csv_path = os.path.join(self.csv_root, self.current_team_file)
        if not os.path.exists(csv_path):
            self.logger.error(f"File {csv_path} not found.")
            return {}
        team = {}
        try:
            with open(csv_path, "r", encoding="utf-8") as f:
                reader = csv.reader(f)
                for row in reader:
                    if len(row) < 2:
                        continue
                    try:
                        name = str(anyascii(row[0]).lower())
                        status = str(row[1]).lower()
                        if status in inactive:
                            team[name] = False
                        else:
                            team[name] = True
                    except:
                        continue
        except UnicodeDecodeError:
            self.logger.error(f"File {csv_path} could not be read with UTF-8 encoding.")
        return team

    def get_workers(self):
        """
        Fetches the list of worker names by leveraging a command-line tool and executes
        it to retrieve the data. Outputs are processed based on the success or failure
        status of the execution. The method dynamically interfaces with an external
        program to obtain the required list of workers.

        :raises RuntimeError: If the execution of the external tool fails.
        :return: A list containing the names of workers, if the execution of the tool
            is successful. Otherwise, returns an empty list.
        :rtype: list of str
        """
        _out, _err, return_code = external_execute([self.deadline_path, "-GetSlaveNames"])
        if return_code == 0:
            return _out.splitlines()
        else:
            return []

    def _get_worker_info(self, worker_name):
        """
        Fetches detailed information about a specific worker from the Deadline system
        by executing a command through an external process. The information gathered
        from the output of the command is parsed and organized into a dictionary. If
        the command fails, an empty dictionary is returned.

        :param worker_name: The name of the worker for which detailed information
            is to be fetched.
        :type worker_name: str

        :return: A dictionary containing details about the specified worker. Each
            key represents a property name of the worker, and its value is either
            the corresponding property value or None if the property is not
            provided.
        :rtype: dict
        """
        _out, _err, return_code = external_execute([self.deadline_path, "-GetSlave", worker_name])
        worker_info = {}
        if return_code == 0:
            for line in _out.splitlines():
                _s = line.split("=")
                if len(_s) == 2:
                    worker_info[_s[0].strip()] = "=".join(_s[1:])
                elif len(_s) == 1:
                    worker_info[_s[0].strip()] = None
        return worker_info

    def parse_description_from_info(self):
        """
        Parses the description from the workers' info and extracts specific attributes
        such as worker type, GPU information, user information, occupation, and worker
        state. The parsed data is categorized into users based on their artistic
        attributes and workers with their respective details.

        workers_info:
            A dictionary containing information about workers. Each key represents a
            worker's identifier, while the value is a dictionary detailing worker-specific
            attributes such as `Description` and `SlaveState`.

        :return:
            A tuple containing two dictionaries:
            - `users`: A dictionary where the keys are the names of artistic users, and
              the values are lists of worker identifiers associated with these users.
            - `workers`: A dictionary where the keys are worker identifiers, and the
              values are dictionaries containing parsed details for each worker.
        """
        users = {}
        workers = {}
        for worker, info in self.workers_info.items():
            desc = info['Description']

            # get worker type
            worker_type = "?"
            if desc.lower().startswith("w"):
                worker_type = "W"
            elif desc.lower().startswith("r"):
                worker_type = "R"
            elif desc.lower().startswith("v"):
                worker_type = "VM"

            sdesc = desc.split(" ")

            # get gpu
            gpu = ""
            if len(sdesc) >= 2:
                g = sdesc[1].strip().lower()
                if g.startswith("gpu:"):
                    gpu = g.strip("gpu:")

            # get artist name or other name
            usr = ""
            is_artist = False
            if len(sdesc) >= 3:
                usr = sdesc[2].strip().lower()
                if '.' in usr:
                    usr = usr.replace(".", " ")
                    is_artist = True
                    if usr not in users:
                        users[usr] = [worker]
                    else:
                        users[usr].append(worker)
            occ = ""
            if len(sdesc) >= 4:
                occ = sdesc[3].strip().lower()

            new_info = {'type': worker_type,
                        'gpu': gpu,
                        'is_artist': is_artist,
                        'usr': usr,
                        'occupation': occ,
                        'comment': info['Comment'],
                        'state': info['SlaveState'],
                        'user_active': False,
                        'team_user_found': False,
                        'read_enabled': self.str_to_bool(info['SlaveEnabled']),
                        'slave_to_be_enabled': None,
                        'check_enabled': None
                        }
            workers[worker] = new_info
        return users, workers

    def assign_team_member_to_worker_by_name(self):
        team_members = list(self.team_data.keys())
        team_members_not_assigned = list(self.team_data.keys())
        worker_user_not_in_team = []
        matched_team_members = 0
        for worker, info in self.workers_parsed.items():
            if not info['is_artist']:
                continue
            if info['usr'] in team_members:
                info['team_user_found'] = True
                try:
                    team_members_not_assigned.remove(info['usr'])
                except ValueError:
                    pass
                info['user_active'] = self.team_data[info['usr']]
                matched_team_members += 1
            else:
                worker_user_not_in_team.append(info['usr'])

        self.logger.info(f"{matched_team_members} team members matched to workers.")
        if len(team_members_not_assigned) > 0:
            self.logger.info(f"{len(team_members_not_assigned)} team members not matched to workers:")
            self.logger.info(pprint.pformat(team_members_not_assigned))
        if len(worker_user_not_in_team) > 0:
            self.logger.debug(f"{len(worker_user_not_in_team)} worker users not matched to team:")
            self.logger.debug(pprint.pformat(team_members_not_assigned))


    def assign_comment_to_workers(self):
        """
        Comments explained:
        R - (Render node) it is a render node, use it all the time render on 27/7
        IM - ignore it (do not use for render), it is in ignore machines list
        IU - ignore it (do not use for render), it is in ignore user list
        F - (Free) it is a workstation not used by any artist, render on 24/7
        W - (artist Working) it is a workstation used by artist - only use it for night renders
        P - (Paused) it is a workstation used by artists that is not active today, render on 24/7

        Note that only lowercase sof first letter is used for enabling / disabling the slave
        """
        comments = {
            'r': 'Render Node',
            'im': 'Ignore - Machine',
            'f': 'Free Workstation',
            'iu': 'Ignore - User',
            'fnt': 'Free Workstation - User not found in team',
            'w': 'Workstation in use in working hours',
            'p': 'Paused - User not active',
        }

        if not self.args['use_comments']:
            self.logger.info("Making Comments by team attendance")
            for worker, info in self.workers_parsed.items():

                if worker in self.ignore_machines:
                    info['comment'] = comments['im']
                    continue
                if info['type'] == 'R':
                    info['comment'] = comments['r']
                    continue

                if info['type'] == 'W':
                    if not info['is_artist']:
                        # artist name was not parsed
                        info['comment'] = comments['f']
                    elif info['usr'] in self.ignore_people:
                        # parsed artist name deems this machine to not be used for renders
                        info['comment'] = comments['iu']
                    elif not info['team_user_found']:
                        # parsed artist name was not found in team names
                        # consider it free
                        info['comment'] = comments['fnt']
                    elif info['user_active']:
                        # artist uses this workstation today
                        info['comment'] = comments['w']
                        continue
                    elif not info['user_active']:
                        # parsed artist name in team csv states this machine can be used today
                        info['comment'] = comments['p']

    def comment_to_deadline(self):
        if not self.args['use_comments']:
            if self.args['dry']:
                self.logger.info("Dry run, not setting comment to deadline")
                return
            else:
                self.logger.info("Setting Comments to Deadline")
                for worker, info in self.workers_parsed.items():
                    if info['comment'] != '':
                        cmd = [self.deadline_path, "-SetSlaveSetting", worker, 'SlaveComment', info['comment']]
                        _out, _err, return_code = external_execute(cmd)
                        if return_code != 0:
                            self.logger.error(f"Setting Comment to slave {worker} failed with return code {return_code}")
        else:
            self.logger.debug("Skipping comment to deadline, use_comments argument is set to True")

    def check_if_set(self):
        if self.args['check']:
            matching_workers = []
            not_matching_workers = []
            for worker, info in self.workers_parsed.items():
                if info['slave_to_be_enabled'] is not None:
                    fresh_info = self._get_worker_info(worker)
                    info['check_enabled'] = self.str_to_bool(fresh_info['SlaveEnabled'])
                    if info['slave_to_be_enabled'] == info['check_enabled']:
                        matching_workers.append(worker)
                    else:
                        not_matching_workers.append(worker)

            self.logger.info(f"Check found {len(matching_workers)} workers set correctly and {len(not_matching_workers)} workers set wrongly.")


    def slave_enabled_by_comment(self):

        enabled_firsts = ['f', 'p', 'r']
        disabled_firsts = ['i']
        if self.args['workstations_render']:
            self.logger.info("\n-=Enabling Workstations Render=-\n")
            enabled_firsts.append('w')
        else:
            self.logger.info("\n-=Disabling Workstations Render=-\n")
            disabled_firsts.append('w')

        # make sure workers are launched first
        # separate loop to give deadline some time
        if not self.args['comments_only'] and not self.args['dry']:
            for worker, info in self.workers_parsed.items():
                if info['comment'] != '':
                    first = info['comment'][0].lower()
                    if first in enabled_firsts:
                        self.logger.debug(f"Launching Slave {worker}")
                        cmd = [self.deadline_path, "-RemoteControl", worker, 'LaunchSlave']
                        _out, _err, return_code = external_execute(cmd)
                        if return_code != 0:
                            self.logger.error(f"Launching Slave {worker} failed with return code {return_code}")

        # now enable the slaves
        for worker, info in self.workers_parsed.items():
            if info['comment'] != '':
                first = info['comment'][0].lower()
                if first in enabled_firsts:
                    slave_enabled = 'True'
                else:
                    slave_enabled = 'False'
                self.workers_parsed[worker]['slave_to_be_enabled'] = self.str_to_bool(slave_enabled)
                if not self.args['comments_only'] and not self.args['dry']:
                    cmd = [self.deadline_path, "-SetSlaveSetting", worker, 'SlaveEnabled', slave_enabled]
                    _out, _err, return_code = external_execute(cmd)
                    if return_code != 0:
                        self.logger.error(f"Setting slave {worker} enabled to {slave_enabled} failed with return code {return_code}")

def get_args():
    parser = argparse.ArgumentParser(description="Uses DeadlineCommand to control if slaves are enabled or not.\nReads team attendance csv and exceptions (machines and users to be skipped) sets the Deadline comments accordingly, and enables or disables Deadline workers by the comments")
    parser.add_argument(
        '--comments_only',
        action='store_true',
        help="Sets comments but doesn't enable/disable workers.",
        required=False
    )
    parser.set_defaults(comments_only=False)

    parser.add_argument(
        '--use_comments',
        action='store_true',
        help="Enable/disable workers by existing comments read from the Deadline.",
        required=False
    )
    parser.set_defaults(use_comments=False)

    parser.add_argument(
        '--workstations_render',
        action='store_true',
        help="Enable worker on workstations. For off-hours.",
        required=False
    )
    parser.set_defaults(workstations_render=False)

    parser.add_argument(
        '--dry',
        action = 'store_true',
        help="Display results but do not execute them.",
        required=False
    )
    parser.set_defaults(dry=False)

    parser.add_argument(
        '--check',
        action='store_true',
        help="After enabling / disabling workers, read back workers status and report success/failure.",
        required=False
    )
    parser.set_defaults(check=True)

    parser.add_argument(
        '-log',
        '--log_level',
        default='DEBUG',
        help='Set the logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)'
    )

    return parser.parse_args()

if __name__ == "__main__":

    args = vars(get_args())
    log = make_logging(args['log_level'])
    ws = WorkerSchedule(args, log)



