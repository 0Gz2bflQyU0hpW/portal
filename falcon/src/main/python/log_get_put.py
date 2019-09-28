# encoding=utf-8
import commands
import time

DATA_SET_PATH = '/user/hdfs/rawlog/app_weibomobile03x4ts1kl_appleiap/'
TARGET_PATH = '/data0/client-appleiap-log/'
TARGET_MACHINE = '10.73.15.174::vip/iap_log/'

def getfile():
    source_path = DATA_SET_PATH + time.strftime("%Y_%m_%d/%H", time.localtime(time.time() - 3600))
    command = "hadoop dfs -get %s %s" % (source_path, TARGET_PATH)
    print_exe_commands(command)

def mergeflie(temp_file):
    filename = temp_file + "app_weibomobile03x4ts1kl_appleiap-yf235028.scribe.dip.sina.com.cn_%s" % (time.strftime("%Y%m%d%H", time.localtime(time.time() - 3600)))
    command = "cat %s > %s" % (temp_file + "app_weibomobile03x4ts1kl*", filename)
    print_exe_commands(command)
    return filename

def putfile(temp_file):
    target_path = TARGET_MACHINE + time.strftime("%Y%m%d%H", time.localtime(time.time() - 3600)) + "/"
    command = "rsync -avz %s root@%s" % (temp_file, target_path)
    print_exe_commands(command)

def delefile(dele_file):
    command = "rm -rf %s" % (dele_file)
    print_exe_commands(command)

def print_exe_commands(command):
    print time.strftime("%Y%m%d%H%M", time.localtime(time.time()))+" "+command
    out = commands.getoutput(command)
    print out

if __name__ == "__main__":
    temp_file = TARGET_PATH + time.strftime("%H", time.localtime(time.time() - 3600)) + "/"
    getfile()
    putfile(mergeflie(temp_file))
    delefile(temp_file)
