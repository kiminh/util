#coding:utf-8
import sys
from paramiko import SSHClient
from scp import SCPClient

REMOTE_ADDRESS = '192.168.211.1'
REMOTE_PATH = "/ssd/model_push/models/"
FILE_LIST = ['../model/lr_model_online.dat',
             '../../shitu_generate2/script/features.list']


def trigger_remote_push():
    ssh = SSHClient()
    ssh.load_system_host_keys()
    ssh.connect(REMOTE_ADDRESS)
    with SCPClient(ssh.get_transport()) as scp:
        scp.put(FILE_LIST, REMOTE_PATH)

    command = 'sh /ssd/model_push/scripts/meepo_push.sh'
    stdin, stdout, stderr = ssh.exec_command(command)

    for line in stdout.read().splitlines():
        print 'stdout', line
    for line in stderr.read().splitlines():
        print 'stderr', line
    ssh.close()


if __name__ == '__main__':
    trigger_remote_push()
