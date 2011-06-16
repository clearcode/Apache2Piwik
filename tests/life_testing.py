#-*- coding: utf-8 -*-

import os, time

f = open('./examplelogs/mobile.customerXYZ.com_access.log.01022011.portal7','r')

for line in f:
    comm = 'echo \''+line.replace('\n','')+'\' >> life_log'
    os.popen(comm) 
    time.sleep(0.1)
