# -*- coding: utf-8 -*-
"""
Created on Tue May 03 15:29:54 2016

@author: Maharnab
"""

from twython import TwythonStreamer
import MySQLdb
import time
import json

#use your own user_id & password
db = MySQLdb.connect("localhost","user_id","password","testdb")
cursor = db.cursor()
cursor.execute("DROP TABLE IF EXISTS tweetTable")
sql = """CREATE TABLE tweetTable (time INT(13),
         username VARCHAR(191) CHARACTER SET utf8mb4,
         tweet VARCHAR(191) CHARACTER SET utf8mb4)"""
cursor.execute(sql)

#Use your own consumer key, consumer secret, access token, access secret.
APP_KEY = "*************"
APP_SECRET = "*************"
OAUTH_TOKEN = "*************"
OAUTH_TOKEN_SECRET = "*************"

class MyStreamer(TwythonStreamer):

    def on_success(self, data):
        all_data = json.dumps(data)
        a = json.loads(all_data)

        # check to ensure there is text in 
        # the json data
        if 'text' in a:
            tweet = a["text"]
            username = a["user"]["screen_name"]    
            insrtcmd = "INSERT INTO tweetTable (time, username, tweet)"
            insrtcmd+= "VALUES (%s,%s,%s)"
            try:
                cursor.execute(insrtcmd, (time.time(), username, tweet))        
                db.commit()
            except:
                db.rollback()
              
#            print((username,tweet))
            return True
        else:
          return True

    def on_error(self, status, data):
        print(status)

stream = MyStreamer(APP_KEY, APP_SECRET, OAUTH_TOKEN, OAUTH_TOKEN_SECRET)
stream.statuses.filter(track=["India"], languages=["en"])
