#-*- coding: utf-8 -*-

# Apache2Piwik - importing data to Piwik from apache logs 
# 
# @link http://clearcode.cc/	
# @license http://www.gnu.org/licenses/gpl-3.0.html GPL v3 or later


import re
from datetime import datetime

#default_log_string = '^(?P<ip>[.\d]+) - (?P<u>.*?) \[(?P<datetime>.*?)\] "GET (?P<url>.*?) HTTP/1.\d" (200|201|202|203|204|205|206|301|302|304) (-|\w+) "(?P<referer>.*?)" "(?P<user_agent>.*?)"\n$'
#default_log = re.compile(default_log_string)

ip = "(\d{1,4}.){3}\d{1,4}"
url = "((http|https)://)?(\w+.)*(\w+)"
url_c = re.compile(url)

# %v - The canonical ServerName of the server serving the request.
v = '(?P<url>.*?)'

# %p - The canonical Port of the server serving the request
p = "\d{1,5}"

# %h - remote host
# problem: co, jeśli HostnameLookups = On (wtedy zamiast numerów ip mamy hostnames)
h = "(?P<ip>"+ip+")"

# %l - Remote logname (from identd, if supplied)
l = "(-|[\w\.]*)"

# %u - remote user, same as REMOTE_USER from CGI scripts
# trzeba tu dodac jeszcze . i &, ale albo się zapętla, albo działa okropnie wolno
u = "(?P<idvisitor>-|(\w|%|=|,| )+)"
#u = '(?P<idvisitor>.*?)'

# %t - Time, in common log format time format (standard english format)
# [day/month/year:hour:minute:second zone]
t = "\[(?P<datetime>\d{2}/[a-zA-Z]{3}/\d{4}:\d{2}:\d{2}:\d{2} (\+|-)\d{4})\]"

# %m - The request method
# Nas interesują tylko GET
m = "(GET|POST)"

# %U - The URL path requested, not including any query string.
U = '(?P<url>.*?)'

# %q - The query string (prepended with a ? if a query string exists, otherwise an empty string)
q = ''

# %H - The request protocol
H = "(HTTP/0.9|HTTP/1.0|HTTP/1.1|)"

# "%r"=="%m %U%q %H"  - First line of request
r = m+' '+U+q+' ?'+H

# %s, %>s - Status.  For requests that got internally redirected, this is the status of the *original* request %>s for the last.
s = "(200|201|202|203|204|205|206)"

# %O - Bytes sent, including headers, cannot be zero.
O = "(-|\d+)"

# Size of response in bytes, excluding HTTP headers. In CLF format, i.e. a '-' rather than a 0 when no bytes are sent.
b = "(-|\d+)"

# %{Foobar}i - The contents of Foobar: header line(s) in the request sent to the server.
referer = "(?P<referer>.*?)"
user_agent = '(?P<user_agent>.*?)'

def create_regexpr(format):
    """
        Change apache log format to regexpr
    """
    regexpr='^'
    while format!='':
        if format.startswith(' '):
            format = format[1:]
            regexpr = regexpr+' '
        elif format.startswith(':'):
            format = format[1:]
            regexpr = regexpr+':'
        elif format.startswith('"'):
            format = format[1:]
            regexpr = regexpr+'"'            
        elif format.startswith('%h'):
            format = format[2:]
            regexpr = regexpr+h 
        elif format.startswith('%l'):
            format = format[2:]
            regexpr = regexpr+l
        elif format.startswith('%H'):
            format = format[2:]
            regexpr = regexpr+H
        elif format.startswith('%U'):
            format = format[2:]
            regexpr = regexpr+U
        elif format.startswith('%u'):
            format = format[2:]
            regexpr = regexpr+u
        elif format.startswith('%t'):
            format = format[2:]
            regexpr = regexpr+t
        elif format.startswith('%r'):
            format = format[2:]
            regexpr = regexpr+r
        elif format.startswith('%s'):
            format = format[2:]
            regexpr = regexpr+s
        elif format.startswith('%>s'):
            format = format[3:]
            regexpr = regexpr+s
        elif format.startswith('%O'):
            format = format[2:]
            regexpr = regexpr+O
        elif format.startswith('%b'):
            format = format[2:]
            regexpr = regexpr+b
        elif format.startswith('%m'):
            format = format[2:]
            regexpr = regexpr+m
        elif format.startswith('%q'):
            format = format[2:]
            regexpr = regexpr+q
        elif format.startswith('%p'):
            format = format[2:]
            regexpr = regexpr+p
        elif format.startswith('%v'):
            format = format[2:]
            regexpr = regexpr+v
        elif format.startswith('%{User-Agent}i'):
            format = format[14:]
            regexpr = regexpr+user_agent
        elif format.startswith('%{Referer}i'):
            format = format[11:]
            regexpr = regexpr+referer
        else:
            #print "ERROR unknown format: "+format 
            return None
    return regexpr+'\n$'


