.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2017 Cask Data, Inc.
    :description: The CDAP User Guide

.. _user-guide-data-preparation-parsers-log:

==========
Log Parser
==========

Parser HTTPD or NGINX Logs

PARSE-AS-LOG is a directive for parsing Apache HTTPD and NGINX access log files.

Syntax
======
::

  parse-as-log <column> <format>


Usage Notes
===========

The PARSE-AS-LOG directive provides a generic log parser that you can construct by specifying the ``format`` of
the log line or the format in which the file was written. The format which specifies the configuration options of
the log line are schema of the access log lines as written by the service.

Directive uses the ``LogFormat`` format that wrote the file as the input parameter.
In addition to the config options specified in the Apache HTTPD manual under `Custom Log Formats <http://httpd.apache.org/docs/current/mod/mod_log_config.html>`__
the following are also recognized:

* common
* combined
* combinedio
* referer
* agent

So, if you are looking to parse combined log format or common log format you can do the following::

  parse-as-log body combined
  parse-as-log body common


If you have logs that's not supported, you can specify the format.

For Nginx the log_format tokens are specified at `Log Format <http://nginx.org/en/docs/http/ngx_http_log_module.html#log_format>`__
and `Variables <http://nginx.org/en/docs/http/ngx_http_core_module.html#variables>`__.

Examples
========

Let's take a real-life example with the common log format. The format for common log is as follows::

  %h %l %u %t "%r" %>s %b


and the corresponding log line as a record needs to be parsed into it's components::

  {
    "body" : "127.0.0.1 - frank [10/Oct/2000:13:55:36 -0700] "GET /apache_pb.gif HTTP/1.0" 200 2326"
  }


Using the directive as follows::

  parse-as-log body %h %l %u %t "%r" %>s %b


Would result in following record::

  {
    "IP_connection.client.host" : "127.0.0.1",
    "IP_connection.client.host.last" : "127.0.0.1"
    "NUMBER_connection.client.logname" : null,
    "NUMBER_connection.client.logname.last" : null,
    ...
    ...
    "HTTP.PATH_request.firstline.uri.path" : "/apache_pb.gif",
    "HTTP.REF_request.firstline.uri.ref" : null
  }


Another example with Combined Log Format::

  %h %l %u %t \"%r\" %>s %b \"%{Referer}i\" \"%{User-agent}i\


and the corresponding log line::


  127.0.0.1 - frank [10/Oct/2000:13:55:36 -0700] "GET /apache_pb.gif HTTP/1.0" 200 2326 "http://www.example.com/start.
  html" "Mozilla/4.08 [en] (Win98; I ;Nav)"


Can also parse complex formats like the one shown below::

  %t %u [%D %h %{True-Client-IP}i %{UNIQUE_ID}e %r] %{Cookie}i %s \"%{User-Agent}i\" \"%{host}i\" %l %b %{Referer}i


Log line::

  [03/Dec/2013:10:53:59 +0000] - [32002 10.102.4.254 195.229.241.182 Up24RwpmBAwAAA1LWJsAAAAR GET
  /content/dam/Central_Library/Street_Shots/Youth/2012/09sep/LFW/Gallery_03/LFW_SS13_SEPT_12_777.jpg.
  image.W0N539E3452S3991w313.original.jpg HTTP/1.1] __utmc=94539802; dtCookie=EFD9D09B6A2E1789F1329FC1
  381A356A|_default|1; dtPC=471217988_141#_load_; Carte::KerberosLexicon_getdomain=6701c1320dd96688b2e
  40b92ce748eee7ae99722; UserData=Username%3ALSHARMA%3AHomepage%3A1%3AReReg%3A0%3ATrialist%3A0%3ALangua
  ge%3Aen%3ACcode%3Aae%3AForceReReg%3A0; UserID=1375493%3A12345%3A1234567890%3A123%3Accode%3Aae; USER_D
  ATA=1375493%3ALSharma%3ALokesh%3ASharma%3Alokesh.sharma%40landmarkgroup.com%3A0%3A1%3Aen%3Aae%3A%3Ado
  main%3A1386060868.51392%3A6701c1320dd96688b2e40b92ce748eee7ae99722; MODE=FONTIS; __utma=94539802.9110
  97326.1339390457.1386060848.1386065609.190; __utmz=94539802.1384758205.177.38.utmcsr=google|utmccn=(o
  rganic)|utmcmd=organic|utmctr=(not%20provided); __kti=1339390460526,http%3A%2F%2Fwww.domain.com%2F,;
  __ktv=28e8-6c4-be3-ce54137d9e48271; WT_FPC=id=2.50.27.157-3067016480.30226245:lv=1386047044279:ss=138
  6046439530; _opt_vi_3FNG8DZU=42880957-D2F1-4DC5-AF16-FEF88891D24E; __hstc=145721067.750d315a49c642681
  92826b3911a4e5a.1351772962050.1381151113005.1381297633204.66; hsfirstvisit=http%3A%2F%2Fwww.domain.co
  m%2F|http%3A%2F%2Fwww.google.co.in%2Furl%3Fsa%3Dt%26rct%3Dj%26q%3Ddomain.com%26source%3Dweb%26cd%3D1%
  26ved%3D0CB0QFjAA%26url%3Dhttp%3A%2F%2Fwww.domain.com%2F%26ei%3DDmuSULW3AcTLhAfJ24CoDA%26usg%3DAFQjCN
  GvPmmyn8Bk67OUv-HwjVU4Ff3q1w|1351772962000; hubspotutk=750d315a49c64268192826b3911a4e5a; __ptca=14572
  1067.jQ7lN5U3C4eN.1351758562.1381136713.1381283233.66; __ptv_62vY4e=jQ7lN5U3C4eN; __pti_62vY4e=jQ7lN5
  U3C4eN; __ptcz=145721067.1351758562.1.0.ptmcsr=google|ptmcmd=organic|ptmccn=(organic)|ptmctr=domain.
  com; RM=Lsharma%3Ac163b6097f90d2869e537f95900e1c464daa8fb9; wcid=Up2cRApmBAwAAFOiVhcAAAAH%3Af32e5e5f5
  b593175bfc71af082ab26e4055efeb6; __utmb=94539802.71.9.1386067462709; edge_auth=ip%3D195.229.241.182~
  expires%3D1386069280~access%3D%2Fapps%2F%2A%21%2Fbin%2F%2A%21%2Fcontent%2F%2A%21%2Fetc%2F%2A%21%2Fho
  me%2F%2A%21%2Flibs%2F%2A%21%2Freport%2F%2A%21%2Fsection%2F%2A%21%2Fdomain%2F%2A~md5%3D5b47f341723924
  87dcd44c1d837e2e54; has_js=1; SECTION=%2Fcontent%2Fsection%2Finspiration-design%2Fstreet-shots.html;
  JSESSIONID=b9377099-7708-45ae-b6e7-c575ffe82187; WT_FPC=id=2.50.27.157-3067016480.30226245:lv=138605
  3618209:ss=1386053618209; USER_GROUP=LSharma%3Afalse; NSC_wtfswfs_xfcgbsn40-41=ffffffff096e1a1d45525
  d5f4f58455e445a4a423660 200 "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0)"
  "www.domain.com" - 24516 http://www.domain.com/content/report/Street_Shots/Youth/Global_round_up/201
  3/01_Jan/mens_youth_stylingglobalround-up1.html

