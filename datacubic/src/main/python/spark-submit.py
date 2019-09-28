import sys
import os
import logging
import MySQLdb
import ConfigParser
import subprocess

logging.basicConfig(level=logging.INFO)

__SPARK_HOME__ = '${SPARK_HOME}'

__SPARK_SUBMIT_INI__ = '/spark-submit.ini'


def get_spark_app(name):
    cf = ConfigParser.ConfigParser()

    cf.read(__SPARK_SUBMIT_INI__)

    host = cf.get('mysql', 'host')
    port = cf.getint('mysql', 'port')
    user = cf.get('mysql', 'user')
    passwd = cf.get('mysql', 'passwd')
    db = cf.get('mysql', 'db')
    timeout = cf.getint('mysql', 'timeout')

    conn = None
    cursor = None

    app = None

    try:
        conn = MySQLdb.connect(host=host, port=port, user=user,
                               passwd=passwd, db=db, connect_timeout=timeout)

        cursor = conn.cursor()

        cursor.execute("select * from spark_app where name = '%s'" % name)

        app = cursor.fetchone()
    finally:
        if cursor:
            try:
                cursor.close()
            except expression as identifier:
                pass

        if conn:
            try:
                conn.close()
            except expression as identifier:
                pass

    return app


def get_spark_submit_command(app):
    name = app[1]
    logging.info('name: %s' % name)

    master = app[2]
    logging.info('master: %s' % master)

    deploy_mode = app[3]
    logging.info('deploy_mode: %s' % deploy_mode)

    clazz = app[4]
    logging.info('class: %s' % clazz)

    jars = app[5]
    logging.info('jars: %s' % jars)

    files = app[6]
    logging.info('files: %s' % files)

    confs = app[7].split(';')
    logging.info('confs: %s' % confs)

    properties_file = app[8]
    logging.info('properties-file: %s' % properties_file)

    queue = app[9]
    logging.info('queue: %s' % queue)

    driver_cores = app[10]
    logging.info('driver-cores: %d', driver_cores)

    driver_memory = app[11]
    logging.info('driver-memory: %d' % driver_memory)

    driver_java_options = app[12]
    logging.info('driver-java-options: %s' % driver_java_options)

    num_executors = app[13]
    logging.info('num-executors: %d' % num_executors)

    executor_cores = app[14]
    logging.info('executor-cores: %d' % executor_cores)

    executor_memory = app[15]
    logging.info('executor-memory: %d' % executor_memory)

    application_jar = app[16]
    logging.info('application-jar: %s' % application_jar)

    application_arguments = app[17].split(' ')
    logging.info('application-arguments: %s' % application_arguments)

    command = 'sudo -u hdfs %s/bin/spark-submit ' % __SPARK_HOME__
    command += '--name %s ' % name
    command += '--master %s ' % master
    command += '--deploy-mode %s ' % deploy_mode

    if clazz:
        command += '--class %s ' % clazz

    if jars:
        command += '--jars %s ' % jars

    if files:
        command += '--files %s ' % files

    if confs:
        for conf in confs:
            command += '--conf %s ' % conf

    if properties_file:
        command += '--properties-file %s ' % properties_file

    command += '--queue %s ' % queue

    command += '--driver-cores %d ' % driver_cores
    command += '--driver-memory %dM ' % driver_memory
    if driver_java_options:
        command += '--driver-java-options %s ' % driver_java_options

    command += '--num-executors %d ' % num_executors
    command += '--executor-cores %d ' % executor_cores
    command += '--executor-memory %dM ' % executor_memory

    command += '%s ' % application_jar

    if application_arguments:
        command += '%s ' % ' '.join(application_arguments)

    return command.strip()


def execute_command(command):
    process = subprocess.Popen(
        command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

    while True:
        line = process.stdout.readline()

        print line.strip()

        if line == '' and process.poll() != None:
            break

    sys.exit(process.poll())

if __name__ == '__main__':
    if len(sys.argv) < 2:
        logging.warn('you must specify app name')

        sys.exit(-1)

    name = sys.argv[1]

    if not os.path.exists(__SPARK_SUBMIT_INI__):
        logging.warn(__SPARK_SUBMIT_INI__ + ' not exist')

        sys.exit(-1)

    app = get_spark_app(name)
    if not app:
        logging.warn('app %s not exist' % name)

        sys.exit(-2)

    command = get_spark_submit_command(app)
    logging.info('command: %s' % command)

    execute_command(command)
