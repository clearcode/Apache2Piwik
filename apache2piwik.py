#-*- coding: utf-8 -*-

# Apache2Piwik - importing data to Piwik from apache logs 
# 
# @link http://clearcode.cc/	
# @license http://www.gnu.org/licenses/gpl-3.0.html GPL v3 or later


import sys
from src.daemons import Daemon
#import daemon
from optparse import OptionParser
import os
from src import apache_log_format_parser as p
import settings as s
import re
import MySQLdb
import bsddb
import warnings
import httpagentparser
from datetime import datetime, timedelta
from socket import inet_aton, inet_ntoa
from struct import unpack, pack
import time
from hashlib import md5
import pygeoip
from src.uasparser import UASparser  


#############################################################################
## helpers

CONTINUE = s.CONTINUE
ID_SITE = s.ID_SITE
APACHE_LOG_FILES = s.APACHE_LOG_FILES
CHRONOLOGICAL_ORDER = s.CHRONOLOGICAL_ORDER

if not 'data' in os.listdir('.'):
    os.mkdir('./data')
if not 'cache'+str(ID_SITE) in os.listdir('./data/'):
    os.mkdir('./data/cache'+str(ID_SITE))

ARCHIVES_TO_DELETE = set([])
db = './data/cache'+str(ID_SITE)+'/still_visiting.db'
uas_parser = UASparser('./data/')  
#script_values = bsddb.btopen('./data/script_values.db', 'c') 
link_visit_action_to_commit = []
SV_ON_DISK = False
if SV_ON_DISK:
    STILL_VISITING = bsddb.btopen(db, 'c')
else:
    if not 'still_visiting.db' in os.listdir('./data/cache'+str(ID_SITE)):
        STILL_VISITING = {}
    else:
        STILL_VISITING = {}
        SV = bsddb.btopen(db, 'c')
        for k in SV.keys():
            STILL_VISITING[k] = SV[k]
        os.remove(db)

def f(regex):
    regex = re.compile(regex)
    return regex
IGNORED_LOGS = map(f,s.IGNORED_LOGS)
IGNORED_USER_AGENTS = map(f,s.IGNORED_USER_AGENTS)

def check_if_ignored(line,dic): 
    for il in dic:
        try:
            il.search(line).group(0)
            return True
        except AttributeError:
            pass
    return False


DBC = False
actions = {}
ignored_browsers = re.compile('(spider|bot|Bot|Spider)')
gi = pygeoip.GeoIP('lib/GeoIP.dat', pygeoip.MEMORY_CACHE)
ignored_extensions = re.compile( ('('+'|'.join(s.IGNORED_EXTENSIONS)+')').replace('.','\.').replace('.js','.js[^p]'))
downloaded_extensions = re.compile(('('+'|'.join(s.DOWNLOADED_EXTENSIONS)+')').replace('.','\.'))
url_regexpr = re.compile(s.URL_REGEXPR)
jsessionid = re.compile(';jsessionid=(?P<jsessionid>[^\.]*)\.')
regexpr = re.compile(p.create_regexpr(s.LOG_FORMAT))
last_server_time = None
continents = { 'a1' : ('unk',13), 'a2' : ('unk',13), 'ad' : ('eur',2), 'ae' : ('asi',4), 'af' : ('asi',4.5), 'ag' : ('amn',-4), 'ai' : ('amn',-4), 'al' : ('eur',+2), 'am' : ('asi',+5), 'an' : ('amn',-4), 'ao' : ('afr',1), 'ap' : ('asi',13), 'aq' : ('unk',0), 'ar' : ('amn',-3), 'as' : ('oce',-11), 'at' : ('eur',2), 'au' : ('oce',10), 'aw' : ('amn',-4), 'ax' : ('eur',13), 'az' : ('asi',5), 'ba' : ('eur',2), 'bb' : ('amn',-4), 'bd' : ('asi',6), 'be' : ('eur',2), 'bf' : ('afr',0), 'bg' : ('eur',3), 'bh' : ('asi',3), 'bi' : ('afr',2), 'bj' : ('afr',1), 'bl' : ('amn',13), 'bm' : ('amn',-3), 'bn' : ('asi',8), 'bo' : ('amn',-4), 'br' : ('amn',-3), 'bs' : ('amn',-4), 'bt' : ('asi',6), 'bv' : ('unk',13), 'bw' : ('afr',2), 'by' : ('eur',3), 'bz' : ('amn',-6), 'ca' : ('amn',-5), 'cc' : ('asi',6.5), 'cd' : ('afr',2), 'cf' : ('afr',1), 'cg' : ('afr',1), 'ch' : ('eur',2), 'ci' : ('afr',0), 'ck' : ('oce',-10), 'cl' : ('amn',-4), 'cm' : ('afr',1), 'cn' : ('asi',8), 'co' : ('amn',-5), 'cr' : ('amn',-6), 'cu' : ('amn',-4), 'cv' : ('afr',-1), 'cx' : ('asi',7), 'cy' : ('asi',3), 'cz' : ('eur',2), 'de' : ('eur',2), 'dj' : ('afr',3), 'dk' : ('eur',2), 'dm' : ('amn',-4), 'do' : ('amn',-4), 'dz' : ('afr',1), 'ec' : ('amn',-5), 'ee' : ('eur',3), 'eg' : ('afr',2), 'eh' : ('afr',0), 'er' : ('afr',3), 'es' : ('eur',2), 'et' : ('afr',3), 'eu' : ('eur',13), 'fi' : ('eur',3), 'fj' : ('oce',12), 'fk' : ('amn',-4), 'fm' : ('oce',11), 'fo' : ('eur',1), 'fr' : ('eur',2), 'fx' : ('eur',13), 'ga' : ('afr',1), 'gb' : ('eur',1), 'gd' : ('amn',-4), 'ge' : ('asi',4), 'gf' : ('amn',-3), 'gg' : ('eur',13), 'gh' : ('afr',0), 'gi' : ('eur',2), 'gl' : ('amn',-2), 'gm' : ('afr',0), 'gn' : ('afr',0), 'gp' : ('amn',-4), 'gq' : ('afr',1), 'gr' : ('eur',13), 'gs' : ('unk',-2), 'gt' : ('amn',-6), 'gu' : ('oce',10), 'gw' : ('afr',0), 'gy' : ('amn',-4), 'hk' : ('asi',8), 'hm' : ('unk',13), 'hn' : ('amn',-6), 'hr' : ('eur',2), 'ht' : ('amn',-5), 'hu' : ('eur',2), 'id' : ('asi',8), 'ie' : ('eur',1), 'il' : ('asi',3), 'im' : ('eur',13), 'in' : ('asi',5.5), 'io' : ('asi',13), 'iq' : ('asi',3), 'ir' : ('asi',4.5), 'is' : ('eur',0), 'it' : ('eur',2), 'je' : ('eur',13), 'jm' : ('amn',-5), 'jo' : ('asi',3), 'jp' : ('asi',9), 'ke' : ('afr',3), 'kg' : ('asi',6), 'kh' : ('asi',7), 'ki' : ('oce',13), 'km' : ('afr',3), 'kn' : ('amn',-4), 'kp' : ('asi',9), 'kr' : ('asi',9), 'kw' : ('asi',3), 'ky' : ('amn',-5), 'kz' : ('asi',5), 'la' : ('asi',7), 'lb' : ('asi',3), 'lc' : ('amn',-4), 'li' : ('eur',2), 'lk' : ('asi',5.5), 'lr' : ('afr',0), 'ls' : ('afr',2), 'lt' : ('eur',3), 'lu' : ('eur',2), 'lv' : ('eur',3), 'ly' : ('afr',2), 'ma' : ('afr',0), 'mc' : ('eur',2), 'md' : ('eur',3), 'me' : ('eur',13), 'mf' : ('amn',13), 'mg' : ('afr',3), 'mh' : ('oce',12), 'mk' : ('eur',2), 'ml' : ('afr',0), 'mm' : ('asi',6.5), 'mn' : ('asi',8), 'mo' : ('asi',8), 'mp' : ('oce',10), 'mq' : ('amn',-4), 'mr' : ('afr',13), 'ms' : ('amn',-4), 'mt' : ('eur',2), 'mu' : ('afr',4), 'mv' : ('asi',5), 'mw' : ('afr',2), 'mx' : ('amn',-5), 'my' : ('asi',8), 'mz' : ('afr',2), 'na' : ('afr',1), 'nc' : ('oce',11), 'ne' : ('afr',1), 'nf' : ('oce',11.5), 'ng' : ('afr',1), 'ni' : ('amn',-6), 'nl' : ('eur',2), 'no' : ('eur',2), 'np' : ('asi',5.5), 'nr' : ('oce',12), 'nu' : ('oce',-11), 'nz' : ('oce',12), 'o1' : ('unk',13), 'om' : ('asi',4), 'pa' : ('amn',-5), 'pe' : ('amn',-5), 'pf' : ('oce',-9.5), 'pg' : ('oce',10), 'ph' : ('asi',8), 'pk' : ('asi',5), 'pl' : ('eur',2), 'pm' : ('amn',-2), 'pn' : ('oce',-8), 'pr' : ('amn',-4), 'ps' : ('asi',3), 'pt' : ('eur',1), 'pw' : ('oce',9), 'py' : ('amn',-4), 'qa' : ('asi',3), 're' : ('afr',4), 'ro' : ('eur',3), 'rs' : ('eur',13), 'ru' : ('eur',4), 'rw' : ('afr',2), 'sa' : ('asi',3), 'sb' : ('oce',11), 'sc' : ('afr',4), 'sd' : ('afr',3), 'se' : ('eur',2), 'sg' : ('asi',8), 'sh' : ('afr',0), 'si' : ('eur',2), 'sj' : ('eur',2), 'sk' : ('eur',2), 'sl' : ('afr',0), 'sm' : ('eur',2), 'sn' : ('afr',0), 'so' : ('afr',3), 'sr' : ('amn',-3), 'st' : ('afr',0), 'sv' : ('amn',-6), 'sy' : ('asi',3), 'sz' : ('afr',2), 'tc' : ('amn',-4), 'td' : ('afr',1), 'tf' : ('unk',5), 'tg' : ('afr',0), 'th' : ('asi',7), 'tj' : ('asi',5), 'tk' : ('oce',-10), 'tl' : ('asi',9), 'tm' : ('asi',5), 'tn' : ('afr',1), 'to' : ('oce',13), 'tr' : ('eur',3), 'tt' : ('amn',-4), 'tv' : ('oce',12), 'tw' : ('asi',8), 'tz' : ('afr',3), 'ua' : ('eur',3), 'ug' : ('afr',3), 'um' : ('oce',-10), 'us' : ('amn',-5), 'uy' : ('amn',-3), 'uz' : ('asi',5), 'va' : ('eur',2), 'vc' : ('amn',-4), 've' : ('amn',-4.5), 'vg' : ('amn',-4), 'vi' : ('amn',-4), 'vn' : ('asi',7), 'vu' : ('oce',11), 'wf' : ('oce',12), 'ws' : ('oce',-11), 'ye' : ('asi',3), 'yt' : ('afr',3), 'za' : ('afr',2), 'zm' : ('afr',2), 'zw' : ('afr',2) }

referers = {}
referers['Google'] = {'regexpr' : re.compile('.google.'), 'keyword' : re.compile('(&|\?)q=(?P<keywords>[^&]*)(&|$)'), 'type' : 2}
referers['Bing'] = {'regexpr' : re.compile('bing.'), 'keyword' : re.compile('(&|\?)q=(?P<keywords>[^&]*)(&|$)'), 'type' : 2}
referers['Yahoo!'] = {'regexpr' : re.compile('.yahoo.'), 'keyword' : re.compile('(&|\?)p=(?P<keywords>[^&]*)(&|$)'), 'type' : 2}
referers['Ask'] = {'regexpr' : re.compile('ask.'), 'keyword' : re.compile('web?q=(?P<keywords>[^&]*)(&|$)'), 'type' : 2}
referers_key = referers.keys()

browsers = {"Firefox" : "FF",
            "Opera" : "OP",
            "Opera Mini" : "OP",
            "Opera Mobile" : "OP",
            "IE Mobile" : "IE",
            "Internet Explorer" : "IE",
            "Microsoft" : "IE",
            "Microsoft Internet Explorer" : "IE",
            "IE 8.0 (Compatibility View)" : "IE",
            "Safari" : "SF",
            "Mobile Safari" : "SF",
            "Chrome" : "CH",
            "Camino" : "CA",	
            "Konqueror" : "KO",
            "Mozilla" : "MO",
            "Netscape Navigator" : "NS",
            "SeaMonkey" : "SM",
            "Firebird" : "FB",
            "BlackBerry" : "BB",
            "Palm OS" : "POS",
            "unknown" : "UNK",
            "Android Webkit" : "AND",
}
browsers_keys = browsers.keys()
oss = {"BlackBerry" : "BLB",
       "iPod" : "IPD",
       "iPhone" : "IPH",
       "iPhone OS" : "IPH",
       "iPad" : "IPA",
       "FreeBSD" : "BSD",
       "Linux" : "LIN",
       "Macintosh" : "MAC",
       "Mac OS X" : "MAC",
       "Palm OS" : "POS",
       "Android" : "AND",
       "Android Webkit" : "AND",
       "Symbian OS" : "SYM",
       "Windows" : "WXP",
       "unknown" : "UNK",
}
oss_keys = oss.keys()
o = set([])
b = set([])

def ip2long(ip_addr):
    """
        Returns encoding of ip in long type
    """
    ip_packed = inet_aton(ip_addr)
    ip = unpack("!L", ip_packed)[0]
    return ip

def long2ip(ip):
    """
        Returns ip from long type
    """
    return inet_ntoa(pack("!L", ip))

def parse_datetime(d):
    """
        Change server time from '20/Dec/2011:12:21:12' to '2011-12-20 12:23:12'
    """
    e = d
    offset = timedelta(minutes=int((-60)*parse_server_datetime_zone(d)))
    
    d = d[:-6]
    Y = d[7:11]
    m = {'Jan' : '01', 'Feb' : '02', 'Mar' : '03', 'Apr' : '04', 'May' : '05', 'Jun' : '06', 'Jul' : '07', 'Aug' : '08', 'Sep' : '09', 'Oct' : '10', 'Nov' : '11', 'Dec' : '12'}[d[3:6]]
    #da = d[:2]
    #H = d[12:14]
    #M = d[15:17]
    #S = d[18:20]
    global ARCHIVES_TO_DELETE
    ARCHIVES_TO_DELETE.add((Y,m)) # pair (year,month)
    #return '%s-%s-%s %s:%s:%s' % (Y,m,da,H,M,S)
    d = datetime.strptime(d,"%d/%b/%Y:%H:%M:%S") + offset
    return d.strftime("%Y-%m-%d %H:%M:%S")

def parse_server_datetime_zone(d):
    result = int(d[-2:])==50 and float(d[-4:-2])+0.5 or float(d[-4:-2])
    return d[-5:-4]=='+' and result or (-1.0)*result

def parse_datetime_zone(d):
    result = int(d[-3:-1])==50 and float(d[-5:-3])+0.5 or float(d[-5:-3])
    return d[-6:-5]=='+' and result or (-1.0)*result

def to_time(server_time):
    """
        Change time from string in format '2011-12-20 12:23:12' to datetime object
    """
    try:
        #zone = parse_datetime_zone(server_time)
        Y = int(server_time[:4])
        m = int(server_time[5:7])
        d = int(server_time[8:10])
        H = int(server_time[11:13])
        M = int(server_time[14:16])
        S = int(server_time[17:19])
        #return datetime.strptime(server_time,"%Y-%m-%d %H:%M:%S")
        return datetime(year=Y, month=m, day=d, hour=H, minute=M, second=S)
    except TypeError:
        return datetime.now()
    except ValueError:
        return datetime.now() 

def take_server_time(s):
    """
        Take server time from value of still_visiting directory
    """
    return s.split('|')[1]

def take_index(s):
    """
        Take index of visitor from log_visit from value of still_visiting directory
    """
    return s.split('|')[0]

def take_visit_first_action_time(s):
    """
        Take index of visitor from log_visit from value of still_visiting directory
    """
    return s.split('|')[2]

def to_list_callback(option, opt, value, parser):
  setattr(parser.values, option.dest, value.split(';'))


#############################################################################
## functions returning information about visitor 

def define_visit(match,line):
    """
        Returns ip, datetime, idvisitor, idaction_url and user_agent of visit from apache logs
    """
    visitor = {}
    match = match.groupdict()
    bb = True

    try:
        visitor['user_agent_original'] = match['user_agent']
    except KeyError:
        visitor['user_agent_original'] = ''

    try:
        visitor['location_ip'] = str(ip2long(match['ip']))
    except KeyError:
        visitor['location_ip'] = '0'

    try:
        visitor['server_time'] = parse_datetime(match['datetime'])
    except KeyError:
        visitor['server_time'] = ''

    global last_server_time
    try:
        last_server_time = parse_datetime(match['datetime'])
    except KeyError:
        last_server_time = ''

    visitor['config_os'] = visitor['config_browser_name'] = 'UNK'
    visitor['config_browser_version'] = ''

    if bb:
        user_agent = httpagentparser.detect(visitor['user_agent_original'])
        try:
            visitor['config_browser_name'] = user_agent['browser']['name']
        except (KeyError, TypeError):
            visitor['config_browser_name'] = ''
        try:
            visitor['config_browser_version'] = user_agent['browser']['version'] 
        except (KeyError, TypeError):
            visitor['config_browser_version'] = ''

        try:
            visitor['config_os'] = user_agent['os']['name']
        except (KeyError, TypeError):
            visitor['config_os'] = ''

    else:
        ua = uas_parser.parse(visitor['user_agent_original'])  
        visitor['config_os'] = ua['os_family']
        visitor['config_browser_name'] = ua['ua_family']
        visitor['config_browser_version'] = ua['ua_name'].replace(ua['ua_family'],'').replace(' ','')

    if visitor['config_os'] not in oss_keys:
        o.add(visitor['config_os'])
    else:
        visitor['config_os'] = oss[visitor['config_os']]
    if visitor['config_browser_name'] not in browsers_keys:
        if not ignored_browsers.search(visitor['config_browser_name']):
            b.add(visitor['config_browser_name'])
    else:
        visitor['config_browser_name'] = browsers[visitor['config_browser_name']]

    visitor['config_browser_version'] = '.'.join(visitor['config_browser_version'].split(".")[:2])
    visitor['user_agent'] = "+".join([visitor['config_os'], visitor['config_browser_name'], visitor['config_browser_version']])

    try:
        visitor['idaction_url'] = match['url']
    except KeyError:
        visitor['idaction_url'] = ''

    m = md5()
    m.update(';'.join([visitor['location_ip'],visitor['config_browser_name'],visitor['config_browser_version'],visitor['config_os']]))
    visitor['idvisitor'] = m.hexdigest()

    return visitor


def analize_visit(match,visitor):
    """
        Returns every other possible information about visit from apache logs
    """
    match = match.groupdict()

    try:
        visitor['server_time_zone'] = parse_datetime_zone(match['datetime'])
    except KeyError:
        visitor['server_time_zone'] = 0

    try:
        visitor['idaction_url_ref'] = match['referer']
    except KeyError:
        visitor['idaction_url_ref'] = ''

    #m.update(visitor['idvisitor'])
    #visitor['config_id'] = m.hexdigest()
    visitor['config_id'] = visitor['idvisitor']
    country = gi.country_code_by_addr(long2ip(long(visitor['location_ip'])))
    visitor['location_country'] =  country and country.lower() or 'unk'
    try:
        visitor['location_continent'] = continents[visitor['location_country']][0]
        visitor['visitor_localtime'] = (to_time(visitor['server_time']) + timedelta(seconds=int(continents[visitor['location_country']][1]-visitor['server_time_zone']*3600) )).time()
    except KeyError:
        visitor['location_continent'] = 'unk'
        visitor['visitor_localtime'] = '00:00:00'


    visitor['referer_type'] = 3
    visitor['referer_name'] = visitor['referer_keyword'] = ''   
    visitor['referer_url'] = match['referer']
    if visitor['referer_url']=='-':
        visitor['referer_type'] = 1

    for key in referers_key:
        if referers[key]['regexpr'].search(match['referer'] ):
            visitor['referer_name'] = key
            visitor['referer_type'] = referers[key]['type']
            try:
                visitor['referer_keyword'] = referers[key]['keyword'].search(match['referer'] ).group('keywords')
                if visitor['referer_keyword']=='':
                    pass
            except AttributeError:
                pass

    return visitor


#############################################################################
## functions inserting data to database

def commit_link_visit_action(cursor):
    global link_visit_action_to_commit
    if len(link_visit_action_to_commit)>0:
        sql = 'INSERT INTO '+ s.PIWIK_PREFIX +'log_link_visit_action (idsite, idvisitor, server_time, idvisit, idaction_url, idaction_url_ref, idaction_name, idaction_name_ref, time_spent_ref_action) VALUES' + ', '.join(link_visit_action_to_commit) + ';'
        cursor.execute(sql)
        link_visit_action_to_commit = []

def insert_to_db(cursor,visitor):
    """
        Inserts link action to piwik_log_link_visit_action for that visitor.
    """
    global link_visit_action_to_commit

    idaction_url = int(index_of_action_dict(cursor,visitor['idaction_url']))
    idaction_url_ref = int(index_of_action_dict(cursor,visitor['idaction_url_ref']))
    idvisit = visitor['idvisit']
    server_time = visitor['server_time']
    idvisitor = visitor['idvisitor']
    (idsite,idaction_name,idaction_name_ref,time_spent_ref_action)=(ID_SITE,0,0,visitor['time_spent_ref_action'])
    #cursor.execute('INSERT INTO '+ s.PIWIK_PREFIX +'log_link_visit_action (idsite, idvisitor, server_time, idvisit, idaction_url, idaction_url_ref, idaction_name, idaction_name_ref, time_spent_ref_action) VALUES (%s,BINARY(UNHEX(SUBSTRING(%s,1,16))),%s,%s,%s,%s,%s,%s,%s)',(idsite,idvisitor,server_time,idvisit,idaction_url,idaction_url_ref,idaction_name,idaction_name_ref,time_spent_ref_action))
    value = '(%s,BINARY(UNHEX(SUBSTRING(\'%s\',1,16))),\'%s\',%s,%s,%s,%s,%s,%s)' % (idsite, idvisitor, server_time, idvisit, idaction_url, idaction_url_ref, idaction_name, idaction_name_ref, time_spent_ref_action)
    link_visit_action_to_commit.append(value)
    if len(link_visit_action_to_commit)>10000:
       commit_link_visit_action(cursor)


def insert_new_to_db(cursor,visitor,visit_last_action_time_prev=None):
    """
        Create visitor in piwik_log_visit and return index of visitor. Insert also link action to piwik_log_link_visit_action for that visitor.
    """
    idsite = ID_SITE
    idvisitor = visitor['idvisitor']
    visitor_localtime = visitor['visitor_localtime']

    location_ip = visitor['location_ip']
    location_country = visitor['location_country'] and visitor['location_country'] or 'unk'
    location_continent = visitor['location_continent']

    config_id = visitor['config_id']
    config_os = visitor['config_os']
    config_browser_name = visitor['config_browser_name']
    config_browser_version = visitor['config_browser_version']

    visit_first_action_time = visitor['server_time']
    visit_last_action_time = visitor['server_time'] 

    visit_entry_idaction_url = index_of_action_dict(cursor,visitor['idaction_url'])
    visit_entry_idaction_name = 0
    visit_exit_idaction_name = 0

    referer_type = visitor['referer_type'] 
    referer_name = visitor['referer_name'] 
    referer_url = visitor['referer_url'] 
    referer_keyword = visitor['referer_keyword']
    #cursor.execute('SELECT visitor_days_since_first, visit_last_action_time FROM '+ s.PIWIK_PREFIX +'log_visit WHERE location_ip=%s AND config_os=%s AND config_browser_name=%s AND config_browser_version=%s ORDER BY idvisit DESC LIMIT 1', (location_ip, config_os, config_browser_name, config_browser_version))
    cursor.execute('SELECT visitor_days_since_first, visit_last_action_time FROM '+ s.PIWIK_PREFIX +'log_visit WHERE idvisitor=BINARY(UNHEX(SUBSTRING(%s,1,16))) AND idsite=%s ORDER BY idvisit DESC LIMIT 1', (idvisitor,idsite))
    last_visit = cursor.fetchone()
    if last_visit: # visitor was here
        visitor_returning = 1
        try:
            visitor_days_since_last =  (to_time(visit_first_action_time) - last_visit[1]).seconds/60
            visitor_days_since_first = visitor_days_since_last + last_visit[0]
        except TypeError:
            visitor_days_since_last = 0
            visitor_days_since_first = 0            
    else:  
        visitor_returning = 0
        visitor_days_since_last = 0
        visitor_days_since_first = 0

    cursor.execute('INSERT INTO '+ s.PIWIK_PREFIX +'log_visit (idsite, idvisitor, visitor_localtime, visitor_returning, visitor_days_since_last, visitor_days_since_first, visit_first_action_time, visit_last_action_time, visit_exit_idaction_name, visit_entry_idaction_url, visit_entry_idaction_name, referer_type, referer_name, referer_url, referer_keyword, config_id, config_os, config_browser_name, config_browser_version, location_ip, location_country, location_continent) VALUES (%s,BINARY(UNHEX(SUBSTRING(%s,1,16))),%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,UNHEX(LPAD(HEX(CONVERT(%s, UNSIGNED)), 8, \'0\')),%s,%s)', (idsite, idvisitor, visitor_localtime, visitor_returning, visitor_days_since_last, visitor_days_since_first, visit_first_action_time, visit_last_action_time, visit_exit_idaction_name, visit_entry_idaction_url, visit_entry_idaction_name, referer_type, referer_name, referer_url, referer_keyword, config_id, config_os, config_browser_name, config_browser_version, location_ip, location_country, location_continent))

    index = visitor['idvisit'] = cursor.lastrowid
    insert_to_db(cursor,visitor)

    return str(index)

def index_of_action(cursor,url):
    """
        Return index of url into piwik_log_action table and if it doesn't exists, it inserts new one.
    """
    url = url_regexpr.match(url).group('url')
    h = url #hash(url)
    action_type = downloaded_extensions.search(url) and 3 or 1

    cursor.execute('SELECT count(idaction), idaction FROM '+ s.PIWIK_PREFIX +'log_action WHERE hash=CRC32(%s) AND type=%s LIMIT 1',(h,action_type))  #AND name LIKE %s LIMIT 1',(h,action_type,url))
    action = cursor.fetchone()
    number = int(action[0])

    if number>0:
        return action[1] 
    else:
        sql = 'INSERT INTO '+ s.PIWIK_PREFIX +'log_action (name,hash,type) VALUES (%s,CRC32(%s),%s)'
        cursor.execute(sql,(url,h,action_type))
        index = cursor.lastrowid
        #global actions
        #actions[url]=index
        return index

def action_to_dict(cursor):
    """
        Create dictionary from piwik_log_action
    """
    cursor.execute('SELECT name, idaction FROM '+s.PIWIK_PREFIX+'log_action')
    global actions
    actions = {}
    for a in cursor.fetchall():
        #global actions 
        actions[a[0]]=a[1]

def index_of_action_dict(cursor,url):
   # try:
   #     r = actions[url]
   #     return r
   # except KeyError:
   return index_of_action(cursor, url)


def update_site(cursor,d):
    """
        If datetime d is before datetime from piwik_site for same id_site as in settings, it updates this value.
    """
    cursor.execute('SELECT ts_created FROM '+ s.PIWIK_PREFIX +'site WHERE idsite=%s',(ID_SITE,))
    try:
        ts_created = cursor.fetchone()[0]
    except TypeError:
        print "Check your %ssite table in piwik - probably you don't have row for idsite=%s" % (s.PIWIK_PREFIX, ID_SITE)
        sys.exit(0)
    d = to_time(d)
    if d < ts_created:
        cursor.execute('UPDATE '+ s.PIWIK_PREFIX +'site SET ts_created=%s WHERE idsite=%s',(d,ID_SITE))        

def add_goals():
    """
    Inserts data to log_conversion
    """
    conn = MySQLdb.connect (host = s.DB_HOST, user = s.DB_USER, passwd = s.DB_PASSWORD, db = s.DB_DATABASE_NAME)
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM '+s.PIWIK_PREFIX+'goal WHERE idsite=%s',(ID_SITE,))
    goals = cursor.fetchall()
    for goal in goals:
        if goal[3]=='url':
            if goal[5]=='contains':
                searchin = "%"+goal[4]+"%"
            elif goal[5] == 'exact':
                searchin = goal[4]
            else:
                print 'Sorry, in this version we cannot add data for goal with pattern urls. Goal with pattern `'+goal[4]+'\' was not processed.'
                continue
            cursor.execute('SELECT idaction FROM '+s.PIWIK_PREFIX+'log_action WHERE name LIKE %s', (searchin,))
            idactions = cursor.fetchall()
            idactions = '('+",".join(map(lambda x: str(int(x[0])),idactions))+')'
            cursor.execute('SELECT lv.idvisit, lva.idsite, lv.idvisitor, lva.server_time, lva.idaction_url, lva.idlink_va, lva.server_time, lv.referer_type, lv.referer_name, lv.referer_keyword, lv.visitor_returning, lv.visitor_count_visits, lv.visitor_days_since_first, lv.location_country, lv.location_continent, la.name FROM '+s.PIWIK_PREFIX+'log_link_visit_action AS lva JOIN '+s.PIWIK_PREFIX+'log_visit AS lv ON (lva.idvisit=lv.idvisit) JOIN '+s.PIWIK_PREFIX+'log_action AS la ON (la.idaction=lva.idaction_url) WHERE idaction_url IN '+idactions+' AND lva.idvisit NOT IN (SELECT idvisit FROM '+s.PIWIK_PREFIX+'log_conversion WHERE idgoal = %s)',(int(goal[1]),))
            conversions = cursor.fetchall()
            for c in conversions:
                idvisit = c[0]
                idsite = c[1]
                idvisitor = c[2]
                server_time = c[3]
                idaction_url = c[4]
                idlink_va = c[5]
                referer_visit_server_date = c[6].strftime("%Y-%m-%d") # trzeba to przenieść z localtime na server time
                referer_type = c[7]
                referer_name = c[8]
                referer_keyword = c[9]
                visitor_returning = c[10]
                visitor_count_visits = c[11]
                visitor_days_since_first = c[12]
                location_country = c[13]
                location_continent = c[14]
                url = c[15]
                idgoal = goal[1]
                revenue = 1.0
                buster = 1
                try:
                    cursor.execute('INSERT INTO '+s.PIWIK_PREFIX+'log_conversion (idvisit, idsite, idvisitor, server_time, idaction_url, idlink_va, referer_visit_server_date, referer_type, referer_name, referer_keyword, visitor_returning, visitor_count_visits, visitor_days_since_first, location_country, location_continent, url, idgoal, revenue, buster) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)', (idvisit, idsite, idvisitor, server_time, idaction_url, idlink_va, referer_visit_server_date, referer_type, referer_name, referer_keyword, visitor_returning, visitor_count_visits, visitor_days_since_first, location_country, location_continent, url, idgoal, revenue, buster))
                except MySQLdb.Error: # if allowed multiple conversions per visit, add it here
                    if goal[7]:
                        cursor.execute('SELECT MAX(buster) FROM '+s.PIWIK_PREFIX+'log_conversion WHERE idvisit=%s AND idgoal=%s',(idvisit,idgoal))
                        buster = int(cursor.fetchone()[0])+1
                        cursor.execute('INSERT INTO '+s.PIWIK_PREFIX+'log_conversion (idvisit, idsite, idvisitor, server_time, idaction_url, idlink_va, referer_visit_server_date, referer_type, referer_name, referer_keyword, visitor_returning, visitor_count_visits, visitor_days_since_first, location_country, location_continent, url, idgoal, revenue, buster) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)', (idvisit, idsite, idvisitor, server_time, idaction_url, idlink_va, referer_visit_server_date, referer_type, referer_name, referer_keyword, visitor_returning, visitor_count_visits, visitor_days_since_first, location_country, location_continent, url, idgoal, revenue, buster))

def drop_archives(cursor):
    global ARCHIVES_TO_DELETE
    for p in ARCHIVES_TO_DELETE:
        try:
            cursor.execute("DROP TABLE "+s.PIWIK_PREFIX+"archive_blob_"+p[0]+"_"+p[1])
        except:
            pass
        try:
            cursor.execute("DROP TABLE "+s.PIWIK_PREFIX+"archive_numeric_"+p[0]+"_"+p[1])
        except:
            pass

#############################################################################
## clearing helpers

def clear_still_visiting(cursor):
    """
        Clear dictionary still_visiting from visitors who exeeded s.VISIT_LENGTH
    """
    #commit_link_visit_action(cursor)
    for k in STILL_VISITING.keys():
        if to_time(last_server_time) - to_time(take_server_time(STILL_VISITING[k])) >= timedelta(seconds=s.VISIT_LENGTH):
            # add visit_last_action_time
            v = STILL_VISITING[k]
            index = take_index(v)
            visit_first_action_time = take_visit_first_action_time(v)
            visit_last_action_time = take_server_time(v)
            cursor.execute('SELECT idaction_url, count(idaction_url), count(idlink_va) FROM '+s.PIWIK_PREFIX+'log_link_visit_action WHERE idvisit=%s ORDER BY idlink_va ASC',(index,))
            row = cursor.fetchone()
            visit_exit_idaction_url = row[0]
            visit_total_actions = row[1]
            visitor_count_visits = row[2]

            #cursor.execute('SELECT visit_first_action_time FROM '+s.PIWIK_PREFIX+'log_visit WHERE idvisit=%s',(index,))
            #row = cursor.fetchone()
            try:
                #visit_total_time = (to_time(visit_last_action_time)-row[0]).seconds
                visit_total_time = (to_time(visit_last_action_time)-to_time(visit_first_action_time)).seconds
            except TypeError:
                visit_total_time = 0
            cursor.execute('UPDATE '+s.PIWIK_PREFIX+'log_visit SET visit_last_action_time=%s, visit_exit_idaction_url=%s, visit_total_actions=%s, visitor_count_visits=%s,  visit_total_time=%s WHERE idvisit=%s',(visit_last_action_time, visit_exit_idaction_url, visit_total_actions, visitor_count_visits, visit_total_time, index))
            del STILL_VISITING[k]


def remove_still_visiting(cursor):
    """
        Remove all data from still_visiting.
    """
    #commit_link_visit_action(cursor)
    for k in STILL_VISITING.keys():
        if to_time(last_server_time) - to_time(take_server_time(STILL_VISITING[k])) < timedelta(seconds=s.VISIT_LENGTH):
            # add visit_last_action_time
            v = STILL_VISITING[k]
            index = take_index(v)
            visit_first_action_time = take_visit_first_action_time(v)
            visit_last_action_time = take_server_time(v)
            cursor.execute('SELECT idaction_url, count(idaction_url), count(idlink_va) FROM '+s.PIWIK_PREFIX+'log_link_visit_action WHERE idvisit=%s ORDER BY idlink_va ASC LIMIT 1',(index,))
            row = cursor.fetchone()
            visit_exit_idaction_url = row[0]
            visit_total_actions = row[1]
            visitor_count_visits = row[2]

            #cursor.execute('SELECT visit_first_action_time FROM '+s.PIWIK_PREFIX+'log_visit WHERE idvisit=%s',(index,))
            #row = cursor.fetchone()
            try:
                #visit_total_time = (to_time(visit_last_action_time)-row[0]).seconds
                visit_total_time = (to_time(visit_last_action_time)-to_time(visit_first_action_time)).seconds
            except TypeError:
                visit_total_time = 0
            cursor.execute('UPDATE '+s.PIWIK_PREFIX+'log_visit SET visit_last_action_time=%s, visit_exit_idaction_url=%s, visit_total_actions=%s, visitor_count_visits=%s,  visit_total_time=%s WHERE idvisit=%s',(visit_last_action_time, visit_exit_idaction_url, visit_total_actions, visitor_count_visits, visit_total_time, index))

def store_still_visiting():
    sv = bsddb.btopen(db, 'c')
    for k in STILL_VISITING.keys():
        sv[k]=STILL_VISITING[k]

def remove_cache(d):
    conn = MySQLdb.connect (host = s.DB_HOST, user = s.DB_USER, passwd = s.DB_PASSWORD, db = s.DB_DATABASE_NAME)
    cursor = conn.cursor()
    remove_still_visiting(cursor)
    try:
        if SV_ON_DISK:
            os.remove(db)
    except OSError:
        pass
    try:
        os.rmdir(d+'/data/cache'+str(ID_SITE))
    except OSError:
        pass


#############################################################################
## main functions

def apache2piwik_process_line(cursor, line, start, g):
    match = regexpr.search(line)
    match_ignored=ignored_extensions.search(line)
    if match and (not match_ignored) and (not check_if_ignored(line,IGNORED_LOGS)): # if line from apache log is ok
        visitor = define_visit(match,line)
        
        if check_if_ignored(visitor['user_agent_original'],IGNORED_USER_AGENTS):
            return start
        
        if start:
            start = False
            update_site(cursor,visitor['server_time'])
        visitor_key = visitor['idvisitor'] #visitor['location_ip']+'|'+visitor['user_agent'] 
        visitor = analize_visit(match,visitor)
        if ignored_browsers.search(visitor['config_browser_name']):
            return start
        #if visitor_key in STILL_VISITING.keys(): # we know that visitor
        try:
            v = STILL_VISITING[visitor_key]
            if to_time(visitor['server_time']) - to_time(take_server_time(v)) < timedelta(seconds=s.VISIT_LENGTH): # he is still visiting
                visitor['time_spent_ref_action'] = (to_time(visitor['server_time']) - to_time(take_server_time(v))).seconds
                index = visitor['idvisit'] = take_index(v)
                visit_first_action_time = take_visit_first_action_time(v)
                STILL_VISITING[visitor_key]=index+'|'+visitor['server_time']+'|'+visit_first_action_time
                insert_to_db(cursor,visitor)
    
            else: # he was visiting a long time ago
                # adding visit_last_action_time and visit_exit_idaction_url
                index = take_index(v)
                visit_first_action_time = take_visit_first_action_time(v)
                visit_last_action_time = take_server_time(v)
                commit_link_visit_action(cursor)
                cursor.execute('SELECT idaction_url, count(idaction_url), count(idlink_va) FROM '+s.PIWIK_PREFIX+'log_link_visit_action WHERE idvisit=%s ORDER BY idlink_va ASC',(index,))
                row = cursor.fetchone()
                visit_exit_idaction_url = row[0]
                visit_total_actions = row[1]
                visitor_count_visits = row[2]
                visitor['time_spent_ref_action'] = 0

                #cursor.execute('SELECT visit_first_action_time FROM '+s.PIWIK_PREFIX+'log_visit WHERE idvisit=%s',(index,))
                #row = cursor.fetchone()
                try:
                #    visit_total_time = (to_time(visit_last_action_time)-row[0]).seconds
                    visit_total_time = (to_time(visit_last_action_time)-to_time(visit_first_action_time)).seconds
                except TypeError:
                   return start

                cursor.execute('UPDATE '+s.PIWIK_PREFIX+'log_visit SET visit_last_action_time=%s, visit_exit_idaction_url=%s, visit_total_actions=%s, visitor_count_visits=%s,  visit_total_time=%s WHERE idvisit=%s',(visit_last_action_time, visit_exit_idaction_url, visit_total_actions, visitor_count_visits, visit_total_time, index))

                index = insert_new_to_db(cursor,visitor,visit_last_action_time)
                STILL_VISITING[visitor_key]=str(index)+'|'+visitor['server_time']+'|'+visitor['server_time']
 
        #else: # we have a new visitor (only by time)
        except KeyError:
            visitor['time_spent_ref_action'] = 0
            index = insert_new_to_db(cursor,visitor)
            STILL_VISITING[visitor_key]=str(index)+'|'+visitor['server_time']+'|'+visitor['server_time']
    else:
        if not (match_ignored or re.search('(HEAD| 404 | 303 )',line)) or (not check_if_ignored(line,IGNORED_LOGS)):
            g.write(line)

    return start



def apache2piwik(d):
    conn = MySQLdb.connect (host = s.DB_HOST, user = s.DB_USER, passwd = s.DB_PASSWORD, db = s.DB_DATABASE_NAME, port=s.DB_PORT)
    cursor = conn.cursor()
    warnings.filterwarnings("ignore", category=MySQLdb.Warning)
    #action_to_dict(cursor)
    g = open(d+'/data/not_matched_lines','a')
    for apache_log_file in APACHE_LOG_FILES:
        if apache_log_file.startswith('./'):
            apache_log_file = d+apache_log_file[1:]
        print 'Started processing '+d+apache_log_file+' file...'
        tstart = datetime.now()
        try:
            f = open(apache_log_file,'r')
        except IOError, e:
            print str(e).replace('[Errno 2] ','')
            sys.exit(0)

        global DBC
        if DBC:
            global STILL_VISITING
            if SV_ON_DISK:
                STILL_VISITING = bsddb.btopen(db, 'c')
            else:
                STILL_VISITING = {}
        if not DBC and not CHRONOLOGICAL_ORDER:
            DBC = True

        start = True
        if s.LIFE:
            while True:
                where = f.tell()
                line = f.readline()
                if len(line)==0:
                    time.sleep(s.FREQUENCY_OF_READING)
                    size_of_file = os.path.getsize(d+apache_log_file)
                    if size_of_file >= where:
                        f.seek(where)
                    else:
                        f.seek(0)
                else:
                    start = apache2piwik_process_line(cursor, line, start,g)
                tend = datetime.now()
                if tend - tstart > 2*timedelta(seconds=s.VISIT_LENGTH):
                    clear_still_visiting(cursor)
                    tstart = datetime.now()

        else:
            for line in f:
                start = apache2piwik_process_line(cursor, line, start,g)

        commit_link_visit_action(cursor)
        clear_still_visiting(cursor) 
        if not CHRONOLOGICAL_ORDER:
            remove_still_visiting(cursor)     
            if SV_ON_DISK:      
                os.remove(db)
        tend = datetime.now()
        diff = tend - tstart
        print 'Finished in %sm%ss.' % (int(diff.seconds/60), diff.seconds % 60 )

    #print o, b
    if not CHRONOLOGICAL_ORDER and len(APACHE_LOG_FILES)>1:
        try:
            os.rmdir(d+'/data/cache'+str(ID_SITE))
        except OSError:
            pass
    add_goals()
    if not CONTINUE:
        remove_still_visiting(cursor)
        if CHRONOLOGICAL_ORDER:
            try:
                if SV_ON_DISK:
                    os.remove(db)
            except OSError:
                pass
        try:
            os.rmdir(d+'/data/cache'+str(ID_SITE))
        except OSError:
            pass
    if CONTINUE and (CHRONOLOGICAL_ORDER or len(APACHE_LOG_FILES)==1):
        if not SV_ON_DISK:
            store_still_visiting()
            print '\nNot finished visits (by server time) are in '+'`./data/cache'+str(ID_SITE)+'\' file. If you change your mind with continueing, please run `python apache2piwik -r\' to remove that folder.'
    drop_archives(cursor)


class MyDaemon(Daemon):
    def run(self,d):
        apache2piwik(d)


if __name__ == "__main__":
    parser = OptionParser(usage="%prog [start|stop] [OPTIONS]", version="Apache2Piwik 1.0")
    parser.add_option("-f", "--file", type='string', dest="APACHE_LOG_FILES",
                      action='callback', callback=to_list_callback,
                      help="apache log files, files names should be ;-seperated, (overrides APACHE_LOG_FILES)", metavar="FILES")
    parser.add_option("-b", "--chronological_order", type="int", dest='CHRONOLOGICAL_ORDER', 
                      help="if files are chronologicaly orderd, (overrides CHRONOLOGICAL_ORDER)", metavar="{0 or 1}")
    parser.add_option("-c", "--continue", type="int", dest="CONTINUE",
                      help="if you want to run script more than one time on one set of data (overrides CONTINUE)", metavar="{0 or 1}")
    parser.add_option("-i", "--id_site", type="int", dest='ID_SITE', metavar="INT", help="Piwik id site (overrides ID_SITE)")
    parser.add_option("-r", "--remove_cache", action="store_true", dest='REMOVE', help="Remove cache files")
    parser.add_option("-g", "--goal", action="store_true", dest='GOAL', help="Create data for goals")
    (options, args) = parser.parse_args()

    DIR = os.getcwd()

    if options.REMOVE:
        remove_cache(DIR) 
        sys.exit(0)

    if options.GOAL:
        add_goals()
        sys.exit(0)

    CONTINUE = options.CONTINUE is None and CONTINUE or bool(options.CONTINUE)
    ID_SITE = options.ID_SITE is None and ID_SITE or options.ID_SITE
    APACHE_LOG_FILES = options.APACHE_LOG_FILES and options.APACHE_LOG_FILES or APACHE_LOG_FILES
    CHRONOLOGICAL_ORDER = options.CHRONOLOGICAL_ORDER is None and CHRONOLOGICAL_ORDER or bool(options.CHRONOLOGICAL_ORDER) 

    if s.LIFE:
        CHRONOLOGICAL_ORDER = True
        CONTINUE = False

        daemon = MyDaemon(DIR+'/data/apache2piwik_daemon.pid')
        if len(sys.argv) >= 2:
            if 'start' == sys.argv[1]:
                daemon.start(DIR)
            elif 'stop' == sys.argv[1]:
                daemon.stop()
                conn = MySQLdb.connect (host = s.DB_HOST, user = s.DB_USER, passwd = s.DB_PASSWORD, db = s.DB_DATABASE_NAME)
                cursor = conn.cursor()
                commit_link_visit_action(cursor)
                clear_still_visiting(cursor)
                if CONTINUE:
                    if not SV_ON_DISK:
                        store_still_visiting()
                else:
                    remove_still_visiting(cursor)
                try:
                    if SV_ON_DISK:
                        os.remove(db)
                except OSError:
                    pass
                try:
                    os.rmdir(DIR+'/data/cache'+str(ID_SITE))
                except OSError:
                    pass

            else:
                print "Unknown command"
                sys.exit(2)
            sys.exit(0)
        else:
                print "usage: %s [start|stop] [OPTIONS]" % sys.argv[0]
                sys.exit(2)

    else:
        apache2piwik(DIR)


