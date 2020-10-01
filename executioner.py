from typing import List, Any
from typing import NamedTuple
import uuid

try:
    from loguru import logger
except(ModuleNotFoundError):
    import logging
import yaml
import pexpect
import csv
import re
# import sys
# import time
import sqlhelpers

class HostInfo4SSH(NamedTuple):
    hostname: str
    port: int


# noinspection PyUnreachableCode
class Executioner:
    inventory = ""
    hosts: List[Any] = []
    d_allitems = {}
    # current temp folders  in executioner tmp dir
    d_run_tmp_folders = {}
    CMDLINE = r"\[PEXPECT\]\$"
    SQLLINE = r"PEXPECT_SQL"
    OLDSQL = "SQL>"

    cmdSH = "sh"
    cmdORA = "ora"
    # CMDLINE = ['# ', '>>> ', '> ', '\$ ']
    initialized = False
    TERMINAL_TYPE = 'vt100'
    ROOT_TMP_DIR = '~/.executioner'
    # default value
    __leave_artifacts = False
    regexp_pre = "##\$"
    regexp_post = "\$##"
    regexp_pre_str = regexp_pre.replace("\\", "")
    regexp_post_str = regexp_post.replace("\\" ,"")
    ################################################################################################
    def __generate_file_name(self, dirname=None):
        # generate file name for directory and resutls
        if dirname:
            return (f"{dirname}/{str(uuid.uuid4())}")
        else:
            return str(uuid.uuid4())

    ################################################################################################
    def __check_folder(self, hostname, folder, createFolder=False):
        # check if we have executioned host directory here
        path, folder_name = folder.rsplit("/", 1)
        logger.debug(f"Checking folder {folder} on {hostname}, if exists ")
        logger.debug(f"{path}, {folder_name}")
        self.d_allitems[hostname].sendline(f"cd {path}")
        self.d_allitems[hostname].expect(self.CMDLINE)
        self.d_allitems[hostname].sendline(f"ls -la| grep {folder_name}")
        self.d_allitems[hostname].expect(self.CMDLINE)
        res = self.d_allitems[hostname].before
        info = res.decode('utf-8').split("\r\n")
        del [info[0]]
        del [info[-1]]
        if not len(info):
            if createFolder:
                logger.debug(f"No directory {folder_name} at {path}exists on {hostname}, need to create one.")
                self.d_allitems[hostname].sendline(f"mkdir {folder_name}")
                self.d_allitems[hostname].expect(self.CMDLINE)
        else:
            logger.debug("Folder exists")

    ################################################################################################
    def __delete_folder(self, hostname):
        # delete folder afterwards unless we set flag to leave artifacts in place
        if not self.__leave_artifacts:
            tmpFolderName = f"{self.d_run_tmp_folders[hostname]}"
            logger.debug(f"Deleting folder {tmpFolderName} on {hostname}.")
            self.d_allitems[hostname].sendline(f"rm -rf {tmpFolderName}")
            self.d_allitems[hostname].expect(self.CMDLINE)
            res = self.d_allitems[hostname].before
            logger.debug(res)

    ################################################################################################
    def just_run(self, ccon, cmd: str):
        self.history.append(cmd)
        _local_debug = False
        # logger.debug("Executing [{}] - [{}]".format(cmd, len(cmd)))
        ccon.sendline(cmd)
        ccon.expect([self.CMDLINE, self.SQLLINE, self.OLDSQL])
        logger.debug(f"Executed [ {cmd[0:32]}... ]")

        res = ccon.before.decode('utf-8').split("\r\n")
        if _local_debug:
            print("--->>> Local debug <<<---")
            print(f"------- > {cmd}")
            print(f"------- > \n\n")
            print(len(res))
            print(res)
            print("--->>>   <<<---")
        if len(res) > 2:
            del res[0]
            # TODO - check - why I was deleting 2 top lines
            # del res[0]
        return res

    ################################################################################################
    def runSSH(self, host, command):
        return None

    ################################################################################################
    # str = "ps -ef | grep ##$PREV_1$## | grep ##$PREV_2$##"
    #
    # pattern =
    #
    # m = re.findall(pattern, str)
    #
    # print(m)

    def run(self, command: list) -> dict:
        parameters_regexp = f"{self.regexp_pre}(.*?){self.regexp_post}"
        parameter_pattern = fr'{parameters_regexp}'
        results = {}

        if command is not None and len(self.hosts) > 0:
            logger.debug(f"Current Hosts: f{self.hosts}")
            for idx_command, current_command in enumerate(command):
                for server in self.hosts:
                    # check if current command should have values from previous command
                    parameters_needed = re.findall(parameter_pattern, current_command)

                    if parameters_needed:
                        logger.debug("We need parameter to be replaced.")
                        logger.debug(f"PRE-command: {current_command}")
                        for parameter_position, item in enumerate(parameters_needed):
                            if server.hostname not in results:
                                raise Exception(
                                    "EXECUTIONER EXCEPTION: Sorry, results from previous comman is not available YET. "
                                    "Please fix your code.")
                            # let's replace regexp in command with values from previous run
                            previous_results = results[server.hostname][idx_command-1][1]
                            item_tpl = item.split("_")
                            # logger.debug(f"PRE-command: {current_command}")
                            # logger.debug(f"Current item: {item}")
                            #logger.debug(item_tpl)
                            #logger.debug(f"{self.regexp_pre}{item}{self.regexp_post}")
                            current_command = current_command.replace(f"{self.regexp_pre_str}{item}{self.regexp_post_str}", previous_results[int(item_tpl[1])])
                        logger.debug(f"POST-command: {current_command}")


                    ccon = self.d_allitems[server.hostname]
                    # res = ccon.before
                    logger.debug(f"[{server.hostname}] Executing [{current_command}, command #{idx_command}]")
                    ccon.sendline(current_command)
                    ccon.expect(self.CMDLINE)
                    ccon.sendline(current_command)
                    ccon.expect(self.CMDLINE)

                    res = ccon.before.decode('ascii').split("\r\n")
                    del res[0]
                    del res[-1]
                    cmd_id = f"server.hostname:cmd"
                    if server.hostname not in results:
                        results[server.hostname] = []
                    results[server.hostname].append([current_command,res])

        return results

    ################################################################################################
    def getRunningDBS(self):
        # ps -ef | grep pmon | grep -v grep  | grep -v grid | awk -F_ '{print $3}'
        return None

    ################################################################################################

    def setOracleEnv(self, hostname , instance) -> None:
        logger.debug("Setting oracle environment")
        ccon = self.d_allitems[hostname]
        ccon.sendline(". oraenv")
        ccon.expect("ORACLE_SID")
        ccon.sendline(instance)
        ccon.expect(self.CMDLINE)

    ################################################################################################
    def __generate_full_sql_for_results(self, sql: str, result_file_name: str) -> list:
        rsql = []
        for item in sqlhelpers.prepare_sql_for_results:
            rsql.append(item)
        rsql.append(f"spool {result_file_name}")
        rsql.append(sqlhelpers.sqls['sql_results_body'].format(sql))
        rsql.append(f"spool off")
        return rsql

    ################################################################################################
    def __generate_tmp_sql_file(self, hostname: str, rsql: list, tmp_file: str) -> None:
        try:
            logger.debug(f"Generating tmp SQL file: {tmp_file} on {hostname}")
            conn = self.d_allitems[hostname]
            cmd = f"echo -- Generated with executioner script >>{tmp_file}"
            self.just_run(conn, cmd)
            for item in rsql:
                cmd = f"echo \"{item}\" >>{tmp_file}"
                self.just_run(conn, cmd)
            logger.debug(f"{tmp_file} has been generated")
        except Exception as e:
            logger.error(e)

    ################################################################################################
    def runSQLwithResults(self, hostname: str, instance: str, sql: str) -> list:
        result_set = []

        logger.info(self.d_allitems)
        ccon = self.d_allitems[hostname]
        # let's generate temp file for execution first
        result_file_name = f"{self.__generate_file_name(self.d_run_tmp_folders[hostname])}.log"
        tmp_sql_file = f"{self.__generate_file_name(self.d_run_tmp_folders[hostname])}.sql"

        logger.debug(f"Result will be generated in {result_file_name} file")
        running_sql = self.__generate_full_sql_for_results(sql, result_file_name)
        # generate file for exec
        self.__generate_tmp_sql_file(hostname, running_sql, tmp_sql_file)

        self.just_run(ccon, f"cd ~")
        self.just_run(ccon, f"export ORACLE_SID={instance}")
        self.setOracleEnv(hostname, instance)
        self.just_run(ccon, "sqlplus / as sysdba")

        self.just_run(ccon, f"spool {result_file_name}")
        # res = self.just_run(ccon, "set sqlprompt {}".format(self.SQLLINE))
        self.just_run(ccon, f"@{tmp_sql_file}")
        self.just_run(ccon, "spool off")
        ######
        # exit from SQL Plus
        self.just_run(ccon, "exit")
        res = self.just_run(ccon, f"cat {result_file_name}")
        logger.debug(res)
        # now let's parse result like 'csv' file, 1st line is headers
        csv_reader = csv.reader(res, dialect='excel', delimiter=',', quotechar='"')
        result_set = [row for row in csv_reader if row]
        return result_set

    ################################################################################################
    def runSQLOnAllhostDBS(self, hostname, sql):
        pass

    ################################################################################################
    def close(self):
        for hostname in self.hosts:
            logger.info(self.d_allitems)
            logger.debug(f"Closing connection to {hostname}")
            self.__delete_folder(hostname[0])
            self.d_allitems[hostname[0]].sendline("exit")

        logger.info("History of commands:")
        # for item in self.history:
        # logger.debug(f"{len(item)}: {item}")

    ################################################################################################
    def readinventory(self, config_file):
        logger.debug("Reading inventory file")
        try:
            with open(config_file) as f:
                data = yaml.load(f, Loader=yaml.FullLoader)
            self.inventory = data
            logger.info(self.inventory)
        except yaml.YAMLError as exc:
            logger.error(exc)

    ################################################################################################
    def __init__(self, config_file, hostname, group, **kwargs):
        if 'leave_artifacts' in kwargs:
            self.__leave_artifacts = kwargs['leave_artifacts']
            logger.debug("Setting value for leave_artifacts")
        self.history = []
        # 2DO: add check if we can't create new session....
        self.readinventory(config_file)
        logger.debug(self.inventory)
        # add host if we passed via host
        if hostname is not None:
            logger.info(f"Adding host {hostname} via parameter")
            self.hosts.append(hostname)
        port = None
        # add items by group (group passed)
        if group is not None:
            lst = self.inventory.get(group)
            if lst is not None:
                for item in self.inventory[group]:
                    add_info = self.inventory[group]
                    if isinstance(add_info[item], dict):
                        port = add_info[item].get("port")
                    if not port:
                        port = "22"
                    logger.debug(f"Adding host {item} with port {port} via group")
                    host_tpl = HostInfo4SSH(item, int(port))
                    self.hosts.append(host_tpl)

        if len(self.d_allitems) == 0:
            # we didn't create ssh connection yet
            logger.debug("We didn't create sessions yet, let's do this")

            for chost_info in self.hosts:
                # we will need to pass TERM = dumb b/c of we don't want colors and etc
                logger.debug(f"Connecting to [{chost_info.hostname}:{chost_info.port}].")

                self.d_allitems[chost_info.hostname] = pexpect.spawn(
                    f"ssh -l oracle -p {chost_info.port} {chost_info.hostname}", timeout=10, maxread=10000000000,
                    searchwindowsize=20000000)
                self.d_allitems[chost_info.hostname].setwinsize(64000, 64000)
                logger.debug(f"Setting prompt on {chost_info}")
                self.d_allitems[chost_info.hostname].sendline("PS1=" + self.CMDLINE)
                self.d_allitems[chost_info.hostname].expect(self.CMDLINE)

                res = self.d_allitems[chost_info.hostname].before
                logger.info(res)
                # find home dir for .executioner folder
                self.d_allitems[chost_info.hostname].sendline("echo $HOME")
                self.d_allitems[chost_info.hostname].expect(self.CMDLINE)
                res = self.d_allitems[chost_info.hostname].before
                info = res.decode('utf-8').split("\r\n")
                self.ROOT_TMP_DIR = f"{info[1]}/.executioner"
                logger.debug(f"Root folder for Executioner tmp file is: {self.ROOT_TMP_DIR}")
                self.__check_folder(chost_info.hostname, self.ROOT_TMP_DIR, True)
                new_tmp_folder = self.__generate_file_name()
                # set full_path
                tmp_folder_fp = f"{self.ROOT_TMP_DIR}/{new_tmp_folder}"
                self.__check_folder(chost_info.hostname, f"{tmp_folder_fp}", True)
                self.d_run_tmp_folders[chost_info.hostname] = tmp_folder_fp
        logger.debug(f"Initialized {len(self.d_allitems)}")
    ################################################################################################
