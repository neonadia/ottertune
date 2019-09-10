#
# OtterTune - fabfile.py
#
# Copyright (c) 2017-18, Carnegie Mellon University Database Group
#

import re
import sys
import json
import logging
import time
import os.path
import re
import glob
import statistics                 
from multiprocessing import Process
from fabric.api import (env, local, task, lcd)
from fabric.state import output as fabric_output
from collections import OrderedDict
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec
from xml.etree import ElementTree as et

LOG = logging.getLogger()
LOG.setLevel(logging.INFO)
Formatter = logging.Formatter("%(asctime)s [%(levelname)s]  %(message)s")  # pylint: disable=invalid-name

# print the log
ConsoleHandler = logging.StreamHandler(sys.stdout)  # pylint: disable=invalid-name
ConsoleHandler.setFormatter(Formatter)
LOG.addHandler(ConsoleHandler)

# Fabric environment settings
env.hosts = ['localhost']
fabric_output.update({
    'running': True,
    'stdout': True,
})

# intervals of restoring the databse
RELOAD_INTERVAL = 1
# maximum disk usage
MAX_DISK_USAGE = 90

# Webdriver option for HBASE configuration
options = webdriver.FirefoxOptions()
options.add_argument('--headless')
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')
options.add_argument('window-size=1920x1480')
wait_time = 120                                                                                                                                             

# EsgynDB nodes IP
nodes = ['10.1.10.20','10.1.10.21','10.1.10.22','10.1.10.23','10.1.10.24','10.1.10.25']



with open('driver_config.json', 'r') as f:
    CONF = json.load(f)


@task
def check_disk_usage():
    partition = CONF['database_disk']
    disk_use = 0
    cmd = "df -h {}".format(partition)
    out = local(cmd, capture=True).splitlines()[1]
    m = re.search('\d+(?=%)', out)  # pylint: disable=anomalous-backslash-in-string
    if m:
        disk_use = int(m.group(0))
    LOG.info("Current Disk Usage: %s%s", disk_use, '%')
    return disk_use


@task
def check_memory_usage():
    cmd = 'free -m -h'
    local(cmd)

@task
def restart_database():
    if CONF['database_type'] == 'postgres':
        cmd = 'sudo service postgresql restart'
    elif CONF['database_type'] == 'ESGYNDB':
        # restart HBASE
        LOG.info('Rebooting HBase, it could take a few mins')
        driver = webdriver.Firefox(options=options)
        driver.implicitly_wait(wait_time)
        #login to saving page
        driver.get("http://localhost:7180/cmf/home")
        time.sleep(1)
        elem1 = driver.find_element_by_name("j_username")
        elem1.send_keys('admin')
        time.sleep(1)
        elem2 = driver.find_element_by_name("j_password")
        elem2.send_keys('admin')
        driver.find_element_by_xpath('//*[@name="submit"]').click()
        time.sleep(2)
        driver.find_element_by_xpath('//*[@id="servicesTable1"]/tbody/tr[2]/td[4]/div/a').click()
        time.sleep(2)
        driver.find_element_by_xpath('//*[@id="servicesTable1"]/tbody/tr[2]/td[4]/div/ul/li[4]/a').click()
        time.sleep(2)
        driver.find_element_by_xpath('//*[@id="modalDialog"]/div/div/div[3]/button[2]').click()
        time.sleep(2)
        locator = (By.XPATH, '//*[@id="commandDialog"]/div/div/div[2]/div/dl/dd[1]/span')
        WebDriverWait(driver, 999999).until(ec.text_to_be_present_in_element(locator, "Finished"))
        driver.quit()
        # restart ESGYNDB
        LOG.info('Shutdown ESGYNDB')
        cmd = "sudo runuser -l trafodion -c 'sqstop abrupt'"   
        local(cmd)
        time.sleep(20)
        LOG.info('Bootup ESGYNDB')
        cmd = "sudo runuser -l trafodion -c 'sqstart'"
        local(cmd)
    else:
        raise Exception("Database Type {} Not Implemented !".format(CONF['database_type']))
    print("Start waiting : %s" % time.ctime())
    time.sleep(5)
    print("End waiting : %s" % time.ctime()) #wait until the esgyndb fully restart 


@task
def drop_database():
    if CONF['database_type'] == 'postgres':
        cmd = "PGPASSWORD={} dropdb -e --if-exists {} -U {}".\
              format(CONF['password'], CONF['database_name'], CONF['username'])
    elif CONF['database_type'] == 'ESGYNDB':
        with open('drop.sql', 'w') as dropq:
            dropq.write('DROP SCHEMA '+CONF['database_name']+' cascade;')            
        cmd = "sudo runuser -l trafodion -c 'sqlci -i /home/trafinstall/ottertune/client/driver/drop.sql'"   
    else:
        raise Exception("Database Type {} Not Implemented !".format(CONF['database_type']))
    local(cmd)


@task
def create_database():
    if CONF['database_type'] == 'postgres':
        cmd = "PGPASSWORD={} createdb -e {} -U {}".\
              format(CONF['password'], CONF['database_name'], CONF['username'])
    elif CONF['database_type'] == 'ESGYNDB':
        with open('createdb.sql', 'w') as createq:
            createq.write('CREATE SCHEMA '+CONF['database_name']+';') 
        cmd = "sudo runuser -l trafodion -c 'sqlci -i /home/trafinstall/ottertune/client/driver/createdb.sql'"
    else:
        raise Exception("Database Type {} Not Implemented !".format(CONF['database_type']))
    local(cmd)


@task
def change_conf():
    next_conf = '/home/trafinstall/ottertune/client/driver/next_config'
    if CONF['database_type'] == 'postgres':
        cmd = 'sudo python3 PostgresConf.py {} {}'.format(next_conf, CONF['database_conf'])
    elif CONF['database_type'] == 'ESGYNDB':
######################### ESGYN Config ########################
        LOG.info('Change ESGYN Config')
        LOG.info('Show ms.env settings before changes')
        cmd = 'sudo runuser -l trafodion -c "grep '+ "'SQ_SB_KEEPALIVE\|SQ_SB_KEEPIDLE'"+ ' {}"'.format(CONF['database_conf'])
        local(cmd)
        cmd = 'sudo python3 /home/trafinstall/ottertune/client/driver/ESGYNConf.py {} {}'.format(next_conf,CONF['database_conf'])                                       
        local(cmd)                 
        cmd = 'sudo runuser -l trafodion -c "pdcp -w {},{},{},{},{} ~/esgynDB_server-2.7.0/tmp/ms.env ~/esgynDB_server-2.7.0/tmp/ms.env"'.format(nodes[1],nodes[2],nodes[3],nodes[4],nodes[5])
        local(cmd)
        LOG.info('Show ms.env settings after changes')
        cmd = 'sudo runuser -l trafodion -c "grep '+ "'SQ_SB_KEEPALIVE\|SQ_SB_KEEPIDLE'"+ ' {}"'.format(CONF['database_conf'])
        local(cmd)                                                                             
######################### Linux Config ########################
        LOG.info('Change Linux Config')
        with open('next_config', "r") as f:
            conf = json.load(f,
                             encoding="UTF-8",
                             object_pairs_hook=OrderedDict)
        conf = conf['recommendation']
        conf_list = list(conf.items())        
        linux_conf_catalog = ['vm_swappiness']
        linux_list = []
        for i in range(len(conf_list)):
            if conf_list[i][0] in linux_conf_catalog:
                linux_list.append(conf_list[i])
        if len(linux_list) != 0: #linux setting does not use current config file, edit it by using cmd.
            for knobs in linux_list:
                if knobs[0] == linux_conf_catalog[0]:
                    for ip in nodes:
                         cmd = "ssh trafinstall@{} 'sudo sysctl vm.swappiness={}'".format(ip,knobs[1])
                         local(cmd)
######################### CQD Config ########################## 
        LOG.info('Change CQD Config')
        cqd_conf_catalog = ['HASH_JOINS','MERGE_JOINS','DEFAULT_DEGREE_OF_PARALLELISM','MDAM_SCAN_METHOD','SEMIJOIN_TO_INNERJOIN_TRANSFORMATION','TRAF_ALLOW_ESP_COLOCATION','ATTEMPT_ESP_PARALLELISM','PARALLEL_NUM_ESPS','OPTIMIZATION_LEVEL']
        cqd_list = []
        for i in range(len(conf_list)):
            if conf_list[i][0] in cqd_conf_catalog:
                cqd_list.append(conf_list[i])
        with open('change_cqd.sql',"w") as cqd:
            for knobs in cqd_list:
                if knobs[0] == cqd_conf_catalog[0]:
                    cqd.write('DELETE FROM "_MD_".defaults' + " WHERE ATTRIBUTE = 'HASH_JOINS';\n")                                                                               
                    if knobs[1] == 1 or knobs[1] == 'on' or knobs[1] == '1':
                        cqd.write('INSERT INTO "_MD_".defaults VALUES' + " ('HASH_JOINS', 'ON', '1', '0');\n"); 
                    else:
                        cqd.write('INSERT INTO "_MD_".defaults VALUES' + " ('HASH_JOINS', 'OFF', '0', '0');\n");
                elif knobs[0] == cqd_conf_catalog[1]:
                    cqd.write('DELETE FROM "_MD_".defaults' + " WHERE ATTRIBUTE = 'MERGE_JOINS';\n")                                                                                
                    if knobs[1] == 1 or knobs[1] == 'on' or knobs[1] == '1':
                        cqd.write('INSERT INTO "_MD_".defaults VALUES' + " ('MERGE_JOINS', 'ON', '1', '0');\n");
                    else:
                        cqd.write('INSERT INTO "_MD_".defaults VALUES' + " ('MERGE_JOINS', 'OFF', '0', '0');\n");
                elif knobs[0] == cqd_conf_catalog[2]:
                    cqd.write('DELETE FROM "_MD_".defaults' + " WHERE ATTRIBUTE = 'DEFAULT_DEGREE_OF_PARALLELISM';\n")                
                    cqd.write('INSERT INTO "_MD_".defaults VALUES' + " ('DEFAULT_DEGREE_OF_PARALLELISM', '{}', 'integer', '0');\n".format(knobs[1]));
                elif knobs[0] == cqd_conf_catalog[3]:
                    cqd.write('DELETE FROM "_MD_".defaults' + " WHERE ATTRIBUTE = 'MDAM_SCAN_METHOD';\n")
                    if knobs[1] == 1 or knobs[1] == 'on' or knobs[1] == '1':
                        cqd.write('INSERT INTO "_MD_".defaults VALUES' + " ('MDAM_SCAN_METHOD', 'ON', '1', '0');\n");
                    else:
                        cqd.write('INSERT INTO "_MD_".defaults VALUES' + " ('MDAM_SCAN_METHOD', 'OFF', '0', '0');\n");
                elif knobs[0] == cqd_conf_catalog[4]:
                    cqd.write('DELETE FROM "_MD_".defaults' + " WHERE ATTRIBUTE = 'SEMIJOIN_TO_INNERJOIN_TRANSFORMATION';\n")                
                    if knobs[1] == 1 or knobs[1] == 'on' or knobs[1] == '1':
                        cqd.write('INSERT INTO "_MD_".defaults VALUES' + " ('SEMIJOIN_TO_INNERJOIN_TRANSFORMATION', 'ON', '1', '0');\n");
                    else:
                        cqd.write('INSERT INTO "_MD_".defaults VALUES' + " ('SEMIJOIN_TO_INNERJOIN_TRANSFORMATION', 'OFF', '0', '0');\n");
                elif knobs[0] == cqd_conf_catalog[5]:
                    cqd.write('DELETE FROM "_MD_".defaults' + " WHERE ATTRIBUTE = 'TRAF_ALLOW_ESP_COLOCATION';\n")
                    if knobs[1] == 1 or knobs[1] == 'on' or knobs[1] == '1':
                        cqd.write('INSERT INTO "_MD_".defaults VALUES' + " ('TRAF_ALLOW_ESP_COLOCATION', 'ON', '1', '0');\n");
                    else:
                        cqd.write('INSERT INTO "_MD_".defaults VALUES' + " ('TRAF_ALLOW_ESP_COLOCATION', 'OFF', '0', '0');\n");
                elif knobs[0] == cqd_conf_catalog[6]:
                    cqd.write('DELETE FROM "_MD_".defaults' + " WHERE ATTRIBUTE = 'ATTEMPT_ESP_PARALLELISM';\n")
                    if knobs[1] == 1 or knobs[1] == 'on' or knobs[1] == '1':
                        cqd.write('INSERT INTO "_MD_".defaults VALUES' + " ('ATTEMPT_ESP_PARALLELISM', 'ON', '1', '0');\n");
                    else:
                        cqd.write('INSERT INTO "_MD_".defaults VALUES' + " ('ATTEMPT_ESP_PARALLELISM', 'OFF', '0', '0');\n");
                elif knobs[0] == cqd_conf_catalog[7]:
                    cqd.write('DELETE FROM "_MD_".defaults' + " WHERE ATTRIBUTE = 'PARALLEL_NUM_ESPS';\n")
                    if knobs[1] == 0 or knobs[1] == '0':
                        cqd.write('INSERT INTO "_MD_".defaults VALUES' + " ('PARALLEL_NUM_ESPS', 'SYSTEM', '0', '0');\n");
                    else:
                        cqd.write('INSERT INTO "_MD_".defaults VALUES' + " ('PARALLEL_NUM_ESPS', '{}', '{}', '0');\n".format(knobs[1],knobs[1]));
                elif knobs[0] == cqd_conf_catalog[8]:
                    cqd.write('DELETE FROM "_MD_".defaults' + " WHERE ATTRIBUTE = 'OPTIMIZATION_LEVEL';\n")                
                    if knobs[1] == 0 or knobs[1] == '0':
                        cqd.write('INSERT INTO "_MD_".defaults VALUES' + " ('OPTIMIZATION_LEVEL', '0', '0', '0');\n");
                    elif knobs[1] == 1 or knobs[1] == '1':
                        cqd.write('INSERT INTO "_MD_".defaults VALUES' + " ('OPTIMIZATION_LEVEL', '2', '1', '0');\n");
                    elif knobs[1] == 2 or knobs[1] == '2':
                        cqd.write('INSERT INTO "_MD_".defaults VALUES' + " ('OPTIMIZATION_LEVEL', '3', '2', '0');\n");
                    elif knobs[1] == 3 or knobs[1] == '3':
                        cqd.write('INSERT INTO "_MD_".defaults VALUES' + " ('OPTIMIZATION_LEVEL', '5', '3', '0');\n");
        cmd = "sudo runuser -l trafodion -c 'sqlci -i /home/trafinstall/ottertune/client/driver/change_cqd.sql'"
        local(cmd)
########################  HBASE Config ########################
        LOG.info('Change HBase Config')
        hbase_conf_catalog = ['hbase_regionserver_handler_count','hbase_regionserver_maxlogs','Java_Heap_Size_of_HBase_RegionServer_in_Bytes']
        hbase_conf_locate = [['//*[@id="main-page-content"]/div/div[3]/div[2]/div[1]/div[2]/form/div[32]/div[2]/div[1]/div[1]/div[2]/span/div/input']\
        ,['//*[@id="main-page-content"]/div/div[3]/div[2]/div[1]/div[2]/form/div[49]/div[2]/div[1]/div[1]/div[2]/span/div/input'],\
        ['//*[@id="main-page-content"]/div/div[3]/div[2]/div[1]/div[2]/form/div[264]/div[2]/div[1]/div[1]/div[2]/span/div/input',\
        '//*[@id="main-page-content"]/div/div[3]/div[2]/div[1]/div[2]/form/div[264]/div[2]/div[1]/div[1]/div[2]/span/select']]
        hbase_list = []
        for i in range(len(conf_list)):  # record which knob need to change according to next_config
            if conf_list[i][0] in hbase_conf_catalog:
                hbase_list.append(conf_list[i])    
        driver = webdriver.Firefox(options=options)
        driver.implicitly_wait(wait_time)
        #login to hbase setting page
        driver.get("http://localhost:7180/cmf/services/9/config")
        time.sleep(1)
        elem = driver.find_element_by_name("j_username")
        elem.send_keys('admin')
        time.sleep(1)
        elem = driver.find_element_by_name("j_password")
        elem.send_keys('admin')
        driver.find_element_by_xpath('//*[@name="submit"]').click()
        #display all available setting
        time.sleep(5)
        elem = driver.find_element_by_xpath('//*[@id="main-page-content"]/div/div[3]/div[2]/div[2]/span[2]/select')
        #wait until 'all' present
        locator = (By.XPATH, '//*[@id="main-page-content"]/div/div[3]/div[2]/div[2]/span[2]/span')
        WebDriverWait(driver, 999999).until(ec.text_to_be_present_in_element(locator, 'Per Page'))                                                                                   
        Select(elem).select_by_visible_text('All')
        hbase_current = []
        for i in range(len(hbase_conf_locate)):
            hbase_current.append(driver.find_element_by_xpath(hbase_conf_locate[i][0]).get_attribute("value"))
        #change config
        for knobs in hbase_list:
            if knobs[0] == hbase_conf_catalog[0] and str(knobs[1]) != hbase_current[0]:
                LOG.info("Send handler count to HBase, value = " + str(knobs[1]))
                elem = driver.find_element_by_xpath(hbase_conf_locate[0][0])
                elem.clear()
                time.sleep(5)            
                elem.send_keys(knobs[1])
                time.sleep(5) 
                driver.find_element_by_xpath('//*[@id="main-page-content"]/div/div[3]/div[3]/div/button').click()
            elif knobs[0] == hbase_conf_catalog[1] and str(knobs[1]) != hbase_current[1]:
                LOG.info("Send maxlog to HBase, value = " + str(knobs[1]))
                elem = driver.find_element_by_xpath(hbase_conf_locate[1][0])
                elem.clear()
                time.sleep(5)
                elem.send_keys(knobs[1])
                time.sleep(5) 
                driver.find_element_by_xpath('//*[@id="main-page-content"]/div/div[3]/div[3]/div/button').click()
            elif knobs[0] == hbase_conf_catalog[2] and str(knobs[1]) != hbase_current[2]:
                LOG.info("Send java heap size to HBase, value = " + str(knobs[1]))
                elem = driver.find_element_by_xpath(hbase_conf_locate[2][1])
                Select(elem).select_by_visible_text('B')
                elem = driver.find_element_by_xpath(hbase_conf_locate[2][0])
                elem.clear()
                time.sleep(5)           
                elem.send_keys(knobs[1])
                time.sleep(5) 
                driver.find_element_by_xpath('//*[@id="main-page-content"]/div/div[3]/div[3]/div/button').click()
        driver.quit()
                            
    else:
        raise Exception("Database Type {} Not Implemented !".format(CONF['database_type']))


@task
def load_oltpbench():
    cmd = "./oltpbenchmark -b {} -c {} --create=true --load=true".\
          format(CONF['oltpbench_workload'], CONF['oltpbench_config'])
    with lcd(CONF['oltpbench_home']):  # pylint: disable=not-context-manager
        local(cmd)


@task
def find_master_node():
    cmd = "sudo runuser -l trafodion -c 'dcscheck|grep" + ' "Active"' + "'"
    master = local(cmd,capture=True)
    master = master[28:34]
    LOG.info("Master Node is " + master)
    master = "jdbc:t4jdbc://" + master + ":23400/:schema=TPCC"
    tree = et.parse("/home/trafinstall/oltpbench/config/esgyn_tpcc_config.xml")
    tree.find('.//DBUrl').text = master 
    tree.write("/home/trafinstall/oltpbench/config/esgyn_tpcc_config.xml")
    cmd = "grep 'DBUrl' ~/oltpbench/config/esgyn_tpcc_config.xml"
    current_setting = local(cmd,capture=True)
    LOG.info("Current setting is " + current_setting)

@task
def bench_time(time=60):
    tree = et.parse("/home/trafinstall/oltpbench/config/esgyn_tpcc_config.xml")
    tree.find('.//time').text = str(time) 
    tree.write("/home/trafinstall/oltpbench/config/esgyn_tpcc_config.xml")
 
@task
def bench_warmup(warmup=30):
    tree = et.parse("/home/trafinstall/oltpbench/config/esgyn_tpcc_config.xml")
    tree.find('.//warmup').text = str(warmup) 
    tree.write("/home/trafinstall/oltpbench/config/esgyn_tpcc_config.xml")

@task
def run_oltpbench():
    if CONF['database_type'] == 'ESGYNDB': 
        find_master_node() 
    cmd = "./oltpbenchmark -b {} -c {} --execute=true -s 5 -o outputfile".\
          format(CONF['oltpbench_workload'], CONF['oltpbench_config'])
    with lcd(CONF['oltpbench_home']):  # pylint: disable=not-context-manager
        local(cmd)


@task
def run_oltpbench_bg():
    if CONF['database_type'] == 'ESGYNDB': 
        find_master_node() 
    cmd = "./oltpbenchmark -b {} -c {} --execute=true -s 5 -o outputfile > {} 2>&1 &".\
          format(CONF['oltpbench_workload'], CONF['oltpbench_config'], CONF['oltpbench_log'])
    with lcd(CONF['oltpbench_home']):  # pylint: disable=not-context-manager
        local(cmd)

@task
def check_oltp_error(): #check if the oltpbench run successfully
    file = open('oltp.log', 'r').read()
    error_line = "does not exist or is inaccessible."
    conflict_line = "A conflict was detected during commit processing"
    count_error = file.count(error_line)
    count_conflicts = file.count(conflict_line)
    if count_error == 0:
        LOG.info('oltpbench run successfully with {} conflicts'.format(count_conflicts))
        cmd = 'head -n 27 oltp.log'
        local(cmd)
        cmd = 'tail -n 12 oltp.log'
        local(cmd)        
    else:
        cmd = 'head -n 27 oltp.log'
        local(cmd)
        cmd = 'tail -n 12 oltp.log'
        local(cmd) 
        raise Exception('SQL Exceptions error occured {} times'.format(count_error))     

@task
def run_controller():
    if CONF['database_type'] == 'postgres':
          cmd = 'sudo gradle run -PappArgs="-c {} -d output/" --no-daemon > {}'.\
              format(CONF['controller_config'], CONF['controller_log'])
    elif CONF['database_type'] == 'ESGYNDB':
          cmd = 'sudo gradle run -PappArgs="-c {} -d output/" --no-daemon > {}'.\
              format(CONF['controller_config'], CONF['controller_log'])
    else:
        raise Exception("Database Type {} Not Implemented !".format(CONF['database_type']))
    with lcd("../controller"):  # pylint: disable=not-context-manager
        local(cmd)


@task
def signal_controller():
    pid = int(open('../controller/pid.txt').read())
    cmd = 'sudo kill -2 {}'.format(pid)
    with lcd("../controller"):  # pylint: disable=not-context-manager
        local(cmd)


@task
def save_dbms_result():
    t = int(time.time())
    files = ['knobs.json', 'metrics_after.json', 'metrics_before.json', 'summary.json']
    for f_ in files:
        f_prefix = f_.split('.')[0]
        cmd = 'cp ../controller/output/{} {}/{}__{}.json'.\
              format(f_, CONF['save_path'], t, f_prefix)
        local(cmd)


@task
def free_cache():
    cmd = 'sync; sudo bash -c "echo 1 > /proc/sys/vm/drop_caches"'
    local(cmd)


@task
def upload_result():
    cmd = 'python3 ../../server/website/script/upload/upload.py \
           ../controller/output/ {} {}/new_result/'.format(CONF['upload_code'],
                                                           CONF['upload_url'])
    local(cmd)


@task
def get_result():
    cmd = 'python3 ../../script/query_and_get.py {} {} 5'.\
          format(CONF['upload_url'], CONF['upload_code'])
    local(cmd)


@task
def add_udf():
    cmd = 'sudo python3 ./LatencyUDF.py ../controller/output/'
    local(cmd)


@task
def upload_batch():
    cmd = 'python3 ./upload_batch.py {} {} {}/new_result/'.format(CONF['save_path'],
                                                                  CONF['upload_code'],
                                                                  CONF['upload_url'])
    local(cmd)


@task
def dump_database():
    db_file_path = '{}/{}.dump'.format(CONF['database_save_path'], CONF['database_name']) #for esgyndb the save_path is the tag
    if os.path.exists(db_file_path):
        LOG.info('%s already exists ! ', db_file_path)
        return False
    else:
        if CONF['database_type'] == 'postgres':
            LOG.info('Dump database %s to %s', CONF['database_name'], db_file_path)
            cmd = 'PGPASSWORD={} pg_dump -U {} -F c -d {} > {}'.format(CONF['password'],
                                                                       CONF['username'],
                                                                       CONF['database_name'],
                                                                       db_file_path)
        elif CONF['database_type'] == 'ESGYNDB':
            LOG.info('Dump database %s to tag %s', CONF['database_name'], CONF['database_save_path'])
            with open('dump.sql', 'w') as dumpq:
                dumpq.write("BACKUP trafodion, tag '"+CONF['database_save_path']+"', schemas ("+CONF['database_name']+'), override;')
            cmd = "sudo runuser -l trafodion -c 'sqlci -i /home/trafinstall/ottertune/client/driver/dump.sql'"
        else:
            raise Exception("Database Type {} Not Implemented !".format(CONF['database_type']))
        local(cmd)
        return True


@task
def restore_database():
    LOG.info('Start restoring database')                                   
    db_file_path = '{}/{}.dump'.format(CONF['database_save_path'], CONF['database_name'])
    drop_database()
    create_database()
    if CONF['database_type'] == 'postgres':
        cmd = 'PGPASSWORD={} pg_restore -U {} -j 8 -F c -d {} {}'.format(CONF['password'],
                                                                         CONF['username'],
                                                                         CONF['database_name'],
                                                                         db_file_path)
        local(cmd)         
    elif CONF['database_type'] == 'ESGYNDB':
        with open('restore.sql', 'w') as restoreq:
            restoreq.write("RESTORE trafodion, tag '"+CONF['database_save_path']+"', schemas ("+CONF['database_name']+');')
        cmd = "sudo runuser -l trafodion -c 'sqlci -i /home/trafinstall/ottertune/client/driver/restore.sql'"
        local(cmd)                  
    else:
        raise Exception("Database Type {} Not Implemented !".format(CONF['database_type']))
    LOG.info('Finish restoring database')

@task
def collector_helper_before_bench():
    if CONF['database_type'] == 'ESGYNDB':
        cmd = "sudo cp /home/trafodion/esgynDB_server-2.7.0/tmp/ms.env /home/trafinstall/knobs_dir/"
        local(cmd)
        ######## read config from HBASE #########
        LOG.info('Read HBase config from cloudera website')
        hbase_conf_catalog = ['hbase_regionserver_handler_count','hbase_regionserver_maxlogs','Java_Heap_Size_of_HBase_RegionServer_in_Bytes']
        hbase_conf_locate = [['//*[@id="main-page-content"]/div/div[3]/div[2]/div[1]/div[2]/form/div[32]/div[2]/div[1]/div[1]/div[2]/span/div/input']\
        ,['//*[@id="main-page-content"]/div/div[3]/div[2]/div[1]/div[2]/form/div[49]/div[2]/div[1]/div[1]/div[2]/span/div/input'],\
        ['//*[@id="main-page-content"]/div/div[3]/div[2]/div[1]/div[2]/form/div[264]/div[2]/div[1]/div[1]/div[2]/span/div/input',\
        '//*[@id="main-page-content"]/div/div[3]/div[2]/div[1]/div[2]/form/div[264]/div[2]/div[1]/div[1]/div[2]/span/select']] 
        hbase_conf_default = [30,32,1073741824]
        driver = webdriver.Firefox(options=options)
        driver.implicitly_wait(wait_time)
        #login to hbase setting page
        driver.get("http://localhost:7180/cmf/services/9/config")
        time.sleep(1)
        elem = driver.find_element_by_name("j_username")
        elem.send_keys('admin')
        time.sleep(1)
        elem = driver.find_element_by_name("j_password")
        elem.send_keys('admin')
        driver.find_element_by_xpath('//*[@name="submit"]').click()
        #display all available setting
        time.sleep(5)
        elem = driver.find_element_by_xpath('//*[@id="main-page-content"]/div/div[3]/div[2]/div[2]/span[2]/select')
        time.sleep(5)             
        Select(elem).select_by_visible_text('All')
        hbase_current = []
        hbase_unit = []
        for i in range(len(hbase_conf_locate)):
            hbase_current.append(driver.find_element_by_xpath(hbase_conf_locate[i][0]).get_attribute("value"))
        # blank means default settings
        for i in range(len(hbase_current)):
            if hbase_current[i] == '':
                hbase_current[i] = hbase_conf_default[i]       
        # unit conversion for GB MB KB B
        hbase_unit.append(driver.find_element_by_xpath(hbase_conf_locate[2][1]).get_attribute("value"))
        print("unit is"+hbase_unit[0])
        hbase_current[2] = int(hbase_current[2]) * int(hbase_unit[0])
        # quit browser
        driver.quit()
        LOG.info('Read esgyn and linux knobs and write all configs into ESGYNDB knob_ table')
        with open('create_knob_table.sql','w') as ckt:   # Create tables in esgyndb to store the knob configurations
            with open('/home/trafinstall/knobs_dir/ms.env','r') as knobw: #ms.conf
                lines = knobw.readlines()
                # ms.env KNOBS
                setting1_idx = 295
                setting2_idx = 296
                keepalive = int(re.findall('\d+',lines[setting1_idx])[0])
                keepidle = int(re.findall('\d+',lines[setting2_idx])[0])
                ckt.write("DROP TABLE IF EXISTS KNOB_;\
                    CREATE TABLE KNOB_ (name varchar(64),value varchar(64));\n\
                    INSERT INTO KNOB_ (name, value) VALUES ('KEEP_ALIVE'," + str(keepalive) + ");\n\
                    INSERT INTO KNOB_ (name, value) VALUES ('KEEP_IDLE'," + str(keepidle) + ");\n")
                # HBASE KNOB
                ckt.write("INSERT INTO KNOB_ (name, value) VALUES ('" + hbase_conf_catalog[0] + "'," + str(hbase_current[0]) + ");\n")
                ckt.write("INSERT INTO KNOB_ (name, value) VALUES ('" + hbase_conf_catalog[1] + "'," + str(hbase_current[1]) + ");\n")
                ckt.write("INSERT INTO KNOB_ (name, value) VALUES ('" + hbase_conf_catalog[2] + "'," + str(hbase_current[2]) + ");\n")
                # CQD KNOBS
                ckt.write('insert into KNOB_ (name, value) select ATTRIBUTE, ATTR_COMMENT from "_MD_".defaults' +" where ATTRIBUTE = 'HASH_JOINS';\n")
                ckt.write('insert into KNOB_ (name, value) select ATTRIBUTE, ATTR_COMMENT from "_MD_".defaults' +" where ATTRIBUTE = 'MERGE_JOINS';\n")
                ckt.write('insert into KNOB_ (name, value) select ATTRIBUTE, ATTR_VALUE from "_MD_".defaults' +" where ATTRIBUTE = 'DEFAULT_DEGREE_OF_PARALLELISM';\n")
                ckt.write('insert into KNOB_ (name, value) select ATTRIBUTE, ATTR_COMMENT from "_MD_".defaults' +" where ATTRIBUTE = 'MDAM_SCAN_METHOD';\n")
                ckt.write('insert into KNOB_ (name, value) select ATTRIBUTE, ATTR_COMMENT from "_MD_".defaults' +" where ATTRIBUTE = 'SEMIJOIN_TO_INNERJOIN_TRANSFORMATION';\n")
                ckt.write('insert into KNOB_ (name, value) select ATTRIBUTE, ATTR_COMMENT from "_MD_".defaults' +" where ATTRIBUTE = 'TRAF_ALLOW_ESP_COLOCATION';\n")
                ckt.write('insert into KNOB_ (name, value) select ATTRIBUTE, ATTR_COMMENT from "_MD_".defaults' +" where ATTRIBUTE = 'ATTEMPT_ESP_PARALLELISM';\n")
                ckt.write('insert into KNOB_ (name, value) select ATTRIBUTE, ATTR_COMMENT from "_MD_".defaults' +" where ATTRIBUTE = 'PARALLEL_NUM_ESPS';\n")
                #ckt.write('insert into KNOB_ (name, value) select ATTRIBUTE, ATTR_COMMENT from "_MD_".defaults' +" where ATTRIBUTE = 'OPTIMIZATION_LEVEL';\n")                                                      
            with open('/proc/sys/vm/swappiness','r') as swap: #linux swap conf
                swappiness = swap.readlines()[0]
                ckt.write("INSERT INTO KNOB_ (name, value) VALUES ('VM_SWAPPINESS'," + str(swappiness) + ");\n")
            #lower cases
            ckt.write("UPDATE KNOB_ SET VALUE = LOWER(VALUE);")
        cmd = "sudo runuser -l trafodion -c 'sqlci -i /home/trafinstall/ottertune/client/driver/create_knob_table.sql'"
        local(cmd)
        with open('create_metrics_table.sql','w') as cmt: # Create tables in esgyndb to store the metrics configurations
            cmt.write("DROP TABLE IF EXISTS METRICS_;\n\
                CREATE TABLE METRICS_ (name char(16),value  float);\n\
                INSERT INTO METRICS_ (name, value) VALUES ('throughput', 0);\n\
                INSERT INTO METRICS_ (name, value) VALUES ('latency', 0);\n")
        cmd = "sudo runuser -l trafodion -c 'sqlci -i /home/trafinstall/ottertune/client/driver/create_metrics_table.sql'"
        local(cmd)

@task
def collector_helper_after_bench():
    if CONF['database_type'] == 'ESGYNDB':
        list_of_summary = glob.glob('/home/trafinstall/oltpbench/results/*.summary') # * means all if need specific format then *.csv
        latest_summary = max(list_of_summary, key=os.path.getctime)
        with open(latest_summary,'r') as sum:
            with open("import_sum.sql",'w') as imp:
                 throughput = 0
                 lines = sum.readlines()
                 throughput = 60 * float(re.findall('[+-]?([0-9]+([.][0-9]*)?|[.][0-9]+)',lines[16])[0][0]) #convert tps to tpm
                 latency = float(re.findall('\d+',lines[13])[1])
                 imp.write("UPDATE METRICS_ SET value = " + str(throughput) + " where name = 'throughput' ;\n")
                 imp.write("UPDATE METRICS_ SET value = " + str(latency) + " where name = 'latency' ;\n")
        cmd = "sudo runuser -l trafodion -c 'sqlci -i /home/trafinstall/ottertune/client/driver/import_sum.sql'"
        local(cmd)


def _ready_to_start_oltpbench():
    return (os.path.exists(CONF['controller_log']) and
            'Output the process pid to'
            in open(CONF['controller_log']).read())


def _ready_to_start_controller():
    return (os.path.exists(CONF['oltpbench_log']) and
            'Warmup complete, starting measurements'
            in open(CONF['oltpbench_log']).read())


def _ready_to_shut_down_controller():
    pid_file_path = '../controller/pid.txt'
    return (os.path.exists(pid_file_path) and os.path.exists(CONF['oltpbench_log']) and
            'Output throughput samples into file' in open(CONF['oltpbench_log']).read())


def clean_logs():
    # remove oltpbench log
    cmd = 'rm -f {}'.format(CONF['oltpbench_log'])
    local(cmd)

    # remove controller log
    cmd = 'rm -f {}'.format(CONF['controller_log'])
    local(cmd)


@task
def lhs_samples(count=10):
    cmd = 'python3 lhs.py {} {} {}'.format(count, CONF['lhs_knob_path'], CONF['lhs_save_path'])
    local(cmd)

@task
def wait_until_benchmarkfinish(extra_time=120):
    if CONF['database_type'] == 'ESGYNDB':
        tree = et.parse("/home/trafinstall/oltpbench/config/esgyn_tpcc_config.xml")
        waittime = int(tree.find('.//warmup').text) + int(tree.find('.//time').text) + int(extra_time)
        LOG.info('OLTPBenchmark will be finished in {} secs'.format(waittime))
        time.sleep(waittime)
        LOG.info('Finished')            

@task
def loop():

    # free cache
    free_cache()

    # remove oltpbench log and controller log
    clean_logs()

    # restart database
    restart_database()
    
    # create knob and metrics tables in ESGYNDB, help collector
    LOG.info('Start the collector helper, only works for esgyn, create the tables to store knobs and metrics in DB')
    collector_helper_before_bench()

    # check disk usage
    if check_disk_usage() > MAX_DISK_USAGE:
        LOG.WARN('Exceeds max disk usage %s', MAX_DISK_USAGE)

    # run controller from another process
    p = Process(target=run_controller, args=())
    p.start()
    LOG.info('Run the controller')

    # run oltpbench as a background job
    while not _ready_to_start_oltpbench():
        pass
    run_oltpbench_bg()
    LOG.info('Run OLTP-Bench')
    # wait until the benchmark finished
    wait_until_benchmarkfinish()

    # the controller starts the first collection
    while not _ready_to_start_controller():
        pass
    check_oltp_error()
    signal_controller()
    LOG.info('Start the first collection')

    # ccollector helper after bench
    LOG.info('Start the collector helper again, only works for esgyn, update the metrics in DB')
    collector_helper_after_bench()

    # stop the experiment
    while not _ready_to_shut_down_controller():
        pass
    signal_controller()
    LOG.info('Start the second collection, shut down the controller')

    p.join()

    # add user defined target objective
    # add_udf()

    # save result
    save_dbms_result()

    # upload result
    upload_result()

    # get result
    get_result()

    # change config
    change_conf()


@task
def run_lhs():
    datadir = CONF['lhs_save_path']
    samples = glob.glob(os.path.join(datadir, 'config_*'))

    # dump database if it's not done before.
    dump = dump_database()

    for i, sample in enumerate(samples):
        LOG.info('\n\n Start %s-th sample %s \n\n', i, sample)

        # free cache
        free_cache()

        if RELOAD_INTERVAL > 0:
            if i % RELOAD_INTERVAL == 0:
                if i == 0 and dump is False:
                    restore_database()
                elif i > 0:
                    restore_database()

        # check memory usage
        check_memory_usage()

        # check disk usage
        if check_disk_usage() > MAX_DISK_USAGE:
            LOG.WARN('Exceeds max disk usage %s', MAX_DISK_USAGE)

        cmd = 'cp {} next_config'.format(sample)	
        local(cmd)

        # remove oltpbench log and controller log
        clean_logs()

        # change config
        change_conf()

        # restart database
        restart_database()
        
        # create knob and metrics tables in ESGYNDB, help collector
        LOG.info('Start the collector helper, only works for esgyn, create the tables to store knobs and metrics in DB')
        collector_helper_before_bench()

        # run controller from another process
        p = Process(target=run_controller, args=())
        p.start()

        # run oltpbench as a background job
        while not _ready_to_start_oltpbench():
            pass
        run_oltpbench_bg()
        LOG.info('Run OLTP-Bench')
        # wait until the benchmark finished
        wait_until_benchmarkfinish()

        while not _ready_to_start_controller():
            pass
        
        LOG.info('Print oltp log file')
        check_oltp_error()
            
        signal_controller()
        LOG.info('Start the first collection')
        
        # ccollector helper after bench
        LOG.info('Start the collector helper again, only works foe esgyn, update the metrics in DB')
        collector_helper_after_bench()


        while not _ready_to_shut_down_controller():
            pass
        # stop the experiment
        signal_controller()
        LOG.info('Start the second collection, shut down the controller')

        p.join()
        # save result
        save_dbms_result()

        # upload result
        upload_result()
@task
def oltp_stable_tests(loops = 10):
    
    oltpresults = []
    
    # give a random setting 
    lhs_samples(5)
    cmd = 'cp configs/config_0 next_config'	
    local(cmd)
    change_conf()

    # dump database if it's not done before.
    dump = dump_database()

    for i in range(int(loops)):
        LOG.info('Start {}th loop'.format(i+1))

        # free cache
        free_cache()

        if RELOAD_INTERVAL > 0:
            if i % RELOAD_INTERVAL == 0:
                if i == 0 and dump is False:
                    restore_database()
                elif i > 0:
                    restore_database()

        # check memory usage
        check_memory_usage()

        # check disk usage
        if check_disk_usage() > MAX_DISK_USAGE:
            LOG.WARN('Exceeds max disk usage %s', MAX_DISK_USAGE)

        # remove oltpbench log and controller log
        clean_logs()

        # restart database
        restart_database()

        # run oltpbench as a background job
        run_oltpbench_bg()
        LOG.info('Run OLTP-Bench')
        
        # wait until the benchmark finished
        wait_until_benchmarkfinish()
                
        LOG.info('Print oltp log file')
        check_oltp_error()
        
        with open("oltp.log","r") as log:
            lines = log.readlines()
            oltpresults.append(float(re.findall('[\d]+\.[\d]+',lines[-9])[0]))
            LOG.info("In {}th loop, the throughput is {} TPS".format(i,oltpresults[-1]))
    
    mean_oltp =  statistics.mean(oltpresults)
    std_oltp = statistics.stdev(oltpresults)
    std_oltp_ratio = std_oltp/mean_oltp*100
    max_diff_ratio = (max(oltpresults)-min(oltpresults))/mean_oltp*100
    LOG.info("The STD is {} TPS and STD ratio is {} %".format(std_oltp,std_oltp_ratio))
    LOG.info("The average result is {} TPS".format(mean_oltp))
    LOG.info("The min/max difference ratio is {} %".format(max_diff_ratio))

@task
def run_loops(max_iter=1):
    # dump database if it's not done before.
    dump = dump_database()

    for i in range(int(max_iter)):
        if RELOAD_INTERVAL > 0:
            if i % RELOAD_INTERVAL == 0:
                if i == 0 and dump is False:
                    restore_database()
                elif i > 0:
                    restore_database()

        LOG.info('The %s-th Loop Starts / Total Loops %s', i + 1, max_iter)
        loop()
        LOG.info('The %s-th Loop Ends / Total Loops %s', i + 1, max_iter)
