import pexpect
import sys
import time
CMDLINE = r"\[PEXPECT\]\$"
SQLLINE = "SQL>"
child = pexpect.spawn("ssh -l oracle {}".format("localhost"), timeout=10, maxread=10000000000, searchwindowsize=20000)
child.sendline("PS1=" + CMDLINE)
child.expect(CMDLINE)
child.sendline("date")
child.expect(CMDLINE)
child.sendline("export ORACLE_SID=XE")
child.expect(CMDLINE)
child.sendline("sqlplus / as sysdba")
child.expect([CMDLINE,SQLLINE])
child.sendline("set heading off")
child.expect([CMDLINE,SQLLINE])
child.sendline("set echo off")
child.expect([CMDLINE,SQLLINE])
child.sendline("select file_name  from dba_data_files;")
child.expect([CMDLINE,SQLLINE])

res = child.before.decode('utf-8').split('\r\n')
del res[0]
del res[0]

print(res)