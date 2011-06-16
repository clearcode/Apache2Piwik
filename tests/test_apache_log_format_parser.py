#-*- coding: utf-8 -*-

# Apache2Piwik - importing data to Piwik from apache logs 
# 
# @link http://clearcode.cc/	
# @license http://www.gnu.org/licenses/gpl-3.0.html GPL v3 or later


import unittest
import re

import sys
sys.path.append('../src/') 
import apache_log_format_parser as p


class TestCreateRegexprSimple(unittest.TestCase):

    def setUp(self):
        pass

    def test_space(self):
        self.assertEqual('^ \n$', p.create_regexpr(' '))

    def test_1(self):
        self.assertEqual('^"\n$', p.create_regexpr("\""))

    def test_v(self):
        regexpr = '^'+p.v+'\n$'
        self.assertEqual(regexpr, p.create_regexpr('%v'))
        g = re.match(regexpr,"server.domain.com\n")
        self.assertNotEqual(None,g)

    def test_p(self):
        regexpr = '^'+p.p+'\n$'
        self.assertEqual(regexpr, p.create_regexpr('%p'))
        g = re.match(regexpr,"1234\n")
        self.assertNotEqual(None,g)

    def test_h(self):
        regexpr = '^'+p.h+'\n$'
        self.assertEqual(regexpr, p.create_regexpr('%h'))
        g = re.match(regexpr,"127.0.0.1\n")
        self.assertNotEqual(None,g)

    def test_l(self):
        regexpr = '^'+p.l+'\n$'
        self.assertEqual(regexpr, p.create_regexpr('%l'))
        g = re.match(regexpr,"-\n")
        self.assertNotEqual(None,g)

    def test_u(self):
        regexpr = '^'+p.u+'\n$'
        self.assertEqual(regexpr, p.create_regexpr('%u'))
        g = re.match(regexpr,"-\n")
        self.assertNotEqual(None,g)

    def test_t(self):
        regexpr = '^'+p.t+'\n$'
        self.assertEqual(regexpr, p.create_regexpr('%t'))
        g = re.match(regexpr,"[04/Apr/2011:14:09:39 +0200]\n")
        self.assertNotEqual(None,g)

    def test_m(self):
        regexpr = '^'+p.m+'\n$'
        self.assertEqual(regexpr, p.create_regexpr('%m'))
        g = re.match(regexpr,"GET\n")
        self.assertNotEqual(None,g)

    def test_U(self):
        regexpr = '^'+p.U+'\n$'
        self.assertEqual(regexpr, p.create_regexpr('%U'))
        g = re.match(regexpr,"http://www.ii.uni.wroc.pl/cms\n")
        self.assertNotEqual(None,g)
        g = re.match(regexpr,"/latest/piwik/themes/default/images/link.gif\n")
        self.assertNotEqual(None,g)
        g = re.match(regexpr,"http://localhost/latest/piwik/libs/jquery/themes/base/jquery-ui.css?cb=6201415e7edf4afd0c2ea94c64467bed\n")
        self.assertNotEqual(None,g)
        g = re.match(regexpr,"/latest/piwik/libs/jquery/themes/base/images/ui-bg_highlight-soft_75_cccccc_1x100.png\n")
        self.assertNotEqual(None,g)

    def test_q(self):
        regexpr = '^'+p.q+'\n$'
        self.assertEqual(regexpr, p.create_regexpr('%q'))
        g = re.match(regexpr,"\n")
        self.assertNotEqual(None,g)

    def test_H(self):
        regexpr = '^'+p.H+'\n$'
        self.assertEqual(regexpr, p.create_regexpr('%H'))
        g = re.match(regexpr,"HTTP/1.0\n")
        self.assertNotEqual(None,g)

    def test_r(self):
        regexpr = '^'+p.r+'\n$'
        self.assertEqual(regexpr, p.create_regexpr('%r'))
        g = re.match(regexpr,"GET /latest/piwik/themes/default/images/link.gif HTTP/1.1\n")
        self.assertNotEqual(None,g)
        g = re.match(regexpr,"OPTIONS * HTTP/1.0\n")
        self.assertEqual(None,g)

    def test_s1(self):
        regexpr = '^'+p.s+'\n$'
        self.assertEqual(regexpr, p.create_regexpr('%s'))
        g = re.match(regexpr,"200\n")
        self.assertNotEqual(None,g)

    def test_s2(self):
        self.assertEqual('^'+p.s+'\n$', p.create_regexpr('%>s'))

    def test_O(self):
        regexpr = '^'+p.O+'\n$'
        self.assertEqual(regexpr, p.create_regexpr('%O'))
        g = re.match(regexpr,"123\n")
        self.assertNotEqual(None,g)

    def test_i_referer(self):
        regexpr = '^'+p.referer+'\n$'
        self.assertEqual(regexpr, p.create_regexpr('%{Referer}i'))
        g = re.match(regexpr,"http://localhost/latest/piwik/index.php?action=systemCheck\n")
        self.assertNotEqual(None,g)

    def test_i_user_agent(self):
        regexpr = '^'+p.user_agent+'\n$'
        self.assertEqual(regexpr, p.create_regexpr('%{User-Agent}i'))
        g = re.match(regexpr,"Mozilla/5.0 (X11; Linux i686; rv:2.0) Gecko/20100101 Firefox/4.0\n")
        self.assertNotEqual(None,g)

    def test_i_user_agent_add(self):
        regexpr = '^\"'+p.user_agent+'\"\n$'
        self.assertEqual(regexpr, p.create_regexpr('\"%{User-Agent}i\"'))
        g = re.match(regexpr,"\"Mozilla/5.0 (X11; Linux i686; rv:2.0) Gecko/20100101 Firefox/4.0\"\n")
        self.assertNotEqual(None,g)

    def test_unknown(self):
        self.assertEqual(None, p.create_regexpr('%aasdfadfafasf'))

class TestCreateRegexprComplex(unittest.TestCase):

    def setUp(self):
        pass

    def test_easy(self):
        regexpr = '^'+p.u+' '+p.t+'\n$'
        self.assertEqual(regexpr, p.create_regexpr("%u %t"))
        g = re.match(regexpr,"- [04/Apr/2011:14:09:39 +0200]\n")  
        self.assertNotEqual(None,g)       

    def test1(self):
        regexpr = '^'+p.v+':'+p.p+' '+p.h+' '+p.l+' '+p.u+' '+p.t+' \"'+p.r+'\" '+p.s+' '+p.O+'\n$'
        self.assertEqual(regexpr, p.create_regexpr("%v:%p %h %l %u %t \"%r\" %>s %O"))

    def test2(self):
        regexpr = '^'+p.v+':'+p.p+' '+p.h+' '+p.l+' '+p.u+' '+p.t+' \"'+p.r+'\" '+p.s+' '+p.O+' \"'+p.referer+'\" \"'+p.user_agent+'\"'+'\n$'
        self.assertEqual(regexpr, p.create_regexpr("%v:%p %h %l %u %t \"%r\" %>s %O \"%{Referer}i\" \"%{User-Agent}i\""))

    def test3(self):
        regexpr = '^'+p.h+' '+p.l+' '+p.u+' '+p.t+'\n$'
        self.assertEqual(regexpr, p.create_regexpr("%h %l %u %t"))
        g = re.match(regexpr,"127.0.0.1 - - [04/Apr/2011:14:09:39 +0200]\n")
        self.assertNotEqual(None,g)

    def test4(self):
        regexpr = '^'+p.h+' '+p.l+' '+p.u+' '+p.t+' \"'+p.r+'\" '+p.s+' '+p.O+' \"'+p.referer+'\"'+'\n$'
        self.assertEqual(regexpr, p.create_regexpr("%h %l %u %t \"%r\" %>s %O \"%{Referer}i\""))
        g = re.match(regexpr,"127.0.0.1 - - [04/Apr/2011:14:09:39 +0200] \"GET /latest/piwik/themes/default/images/link.gif HTTP/1.1\" 200 186 \"http://localhost/latest/piwik/index.php?action=systemCheck\"\n")
        self.assertNotEqual(None,g)

    def test5(self):
        regexpr = '^'+p.h+' '+p.l+' '+p.u+' '+p.t+' \"'+p.r+'\" '+p.s+' '+p.O+' \"'+p.referer+'\" \"'+p.user_agent+'\"'+'\n$'
        self.assertEqual(regexpr, p.create_regexpr("%h %l %u %t \"%r\" %>s %O \"%{Referer}i\" \"%{User-Agent}i\""))
        g = re.match(regexpr,"127.0.0.1 - - [04/Apr/2011:14:09:39 +0200] \"GET /latest/piwik/themes/default/images/link.gif HTTP/1.1\" 200 186 \"http://localhost/latest/piwik/index.php?action=systemCheck\" \"Mozilla/5.0 (X11; Linux i686; rv:2.0) Gecko/20100101 Firefox/4.0\"\n")
        self.assertNotEqual(None,g)

    def test6(self):
        regexpr = '^'+p.h+' '+p.l+' '+p.u+' '+p.t+' \"'+p.r+'\" '+p.s+' '+p.O+' \"'+p.referer+'\"'+'\n$'
        self.assertEqual(regexpr, p.create_regexpr("%h %l %u %t \"%r\" %>s %O \"%{Referer}i\""))
        g = re.match(regexpr,"127.0.0.1 - - [04/Apr/2011:13:57:29 +0200] \"OPTIONS * HTTP/1.0\" 200 152 \"-\"\n")
        self.assertEqual(None,g)

    def test7(self):
        regexpr = '^'+p.h+' '+p.l+' '+p.u+' '+p.t+' \"'+p.r+'\" '+p.s+' '+p.O+'\n$'
        self.assertEqual(regexpr, p.create_regexpr("%h %l %u %t \"%r\" %>s %O"))
        g = re.match(regexpr,"127.0.0.1 - - [04/Apr/2011:13:57:29 +0200] \"GET * HTTP/1.0\" 200 152\n")
        self.assertNotEqual(None,g)

    def test8(self):
        regexpr = '^'+p.h+' '+p.l+' '+p.u+' '+p.t+' \"'+p.r+'\" '+p.s+' '+p.O+' \"'+p.referer+'\"'+'\n$'
        self.assertEqual(regexpr, p.create_regexpr("%h %l %u %t \"%r\" %>s %O \"%{Referer}i\""))
        g = re.match(regexpr,"127.0.0.1 - - [04/Apr/2011:14:06:49 +0200] \"GET /latest/piwik/themes/default/styles.css?cb=6201415e7edf4afd0c2ea94c64467bed HTTP/1.1\" 200 210 \"http://localhost/latest/piwik/index.php?action=systemCheck\"\n")
        self.assertNotEqual(None,g)

    def test9(self):
        regexpr = '^'+p.h+' '+p.l+' '+p.u+' '+p.t+' \"'+p.r+'\" '+p.s+' '+p.O+' \"'+p.referer+'\"'+'\n$'
        self.assertEqual(regexpr, p.create_regexpr("%h %l %u %t \"%r\" %>s %O \"%{Referer}i\""))
        g = re.match(regexpr,"127.0.0.1 - - [04/Apr/2011:14:06:42 +0200] \"GET /latest/piwik/libs/jquery/themes/base/images/ui-bg_highlight-soft_75_cccccc_1x100.png HTTP/1.1\" 200 391 \"http://localhost/latest/piwik/libs/jquery/themes/base/jquery-ui.css?cb=6201415e7edf4afd0c2ea94c64467bed\"\n")
        self.assertNotEqual(None,g)

    def test10(self):
        regexpr = '^'+p.h+' '+p.l+' '+p.u+' '+p.t+' \"'+p.r+'\" '+p.s+' '+p.O+'\n$'
        self.assertEqual(regexpr, p.create_regexpr("%h %l %u %t \"%r\" %>s %O"))
        g = re.match(regexpr,"127.0.0.1 - - [04/Apr/2011:14:06:42 +0200] \"GET /latest/piwik/libs/jquery/themes/base/images/ui-bg_highlight-soft_75_cccccc_1x100.png HTTP/1.1\" 200 391\n")
        self.assertNotEqual(None,g)


    

if __name__ == '__main__':
    unittest.main()

