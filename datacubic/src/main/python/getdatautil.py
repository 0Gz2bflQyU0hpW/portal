#!/usr/bin/env python
# encoding=utf-8

import sys
reload(sys)
sys.setdefaultencoding( "utf-8" )
import time
import json
import urllib
import urllib2
import hmac
import hashlib

DIP_API = "api.dip.sina.com.cn"

SECRET_KEY = "a65e052392c14d10a0a5c20f986491230sSYnCvu"
ACCESS_KEY = "oa2fVAFFA39023F47FCB"

def dipssig(secretkey,stringtosign):
    return hmac.new(secretkey, stringtosign, hashlib.sha1).digest().encode('base64')[5:15]


def selectJobData(jobName,selectName,jobTimestamp):

    uri = "/rest/v2/job/selectjob/getSelectJobResultData/%s/%s/%s" % (jobName,selectName,jobTimestamp)
    timestamp = int(time.time())
    stringtosign = "GET\n\n\n\n%s?accesskey=%s&timestamp=%d" % (uri,ACCESS_KEY,timestamp)
    ssig = dipssig(SECRET_KEY,stringtosign)
    ssig = urllib.quote(ssig)
    uri = urllib.quote(uri)
    uri = "http://%s%s?accesskey=%s&timestamp=%d&ssig=%s" % (DIP_API,uri,ACCESS_KEY,timestamp,ssig)
    #print "request uri: %s" % uri
    resp = urllib2.urlopen( uri , timeout = 3000 )

    if not resp.code == 200:
        print "ERROR: call rest api failed" 
        raise Exception("ERROR: call rest api failed") 
    result = json.loads(resp.read())
    inputs = result['data']
    return inputs

