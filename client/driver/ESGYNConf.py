#
# OtterTune - PostgresConf.py
#
# Copyright (c) 2017-18, Carnegie Mellon University Database Group
#
'''
Created on Aug 2nd, 2019
@author: Chenyang Li
'''

import sys
import json
from collections import OrderedDict


def main():
    if (len(sys.argv) != 3):
        raise Exception("Usage: python confparser.py [Next Config] [Current Config]")

    with open(sys.argv[1], "r") as f:
        conf = json.load(f,
                         encoding="UTF-8",
                         object_pairs_hook=OrderedDict)
    conf = conf['recommendation']
    conf_list = list(conf.items())
    # ms.env file 
    ms_env_catalog = ['keep_alive','keep_idle'] # knob names
    ms_env_index = [295,296] # index of the lines
    # hbase config on cloudera
    hbase_conf_catalog = ['hbase.regionserver.handler.count','hbase.regionserver.maxlogs'\
                      ,'Java Heap Size of HBase RegionServer in Bytes']   
    ms_list = []
    hbase_list = []
   
    for i in range(len(conf_list)):
        if conf_list[i][0] in ms_env_catalog:
            ms_list.append(conf_list[i])
        elif conf_list[i][0] in hbase_conf_catalog:
            hbase_list.append(conf_list[i])           
    
    if len(ms_list) != 0:
        with open(sys.argv[2], "r+") as ESGYNconf:
            lines = ESGYNconf.readlines()
        for knobs in ms_list:
            if knobs[0] == ms_env_catalog[0]:
                if knobs[1] == 'on':
                    lines[ms_env_index[0]] = 'SQ_SB_KEEPALIVE'+'='+'1'+'\n'
                else:
                    lines[ms_env_index[0]] = 'SQ_SB_KEEPALIVE'+'='+'0'+'\n'
            elif knobs[0] == ms_env_catalog[1]:
                lines[ms_env_index[1]] = 'SQ_SB_KEEPIDLE'+'='+str(knobs[1])+'\n'
        with open(sys.argv[2], 'w') as file:
            file.writelines(lines)



if __name__ == "__main__":
    main()
