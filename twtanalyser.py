# -*- coding: utf-8 -*-
"""
Created on Sun Aug 21 16:08:33 2016

@author: Maharnab
"""

import MySQLdb
from nltk.tokenize import TweetTokenizer

#use your own user_id & password
db = MySQLdb.connect("localhost","root","3369","testdb")
cursor = db.cursor()

# Prepare SQL query to INSERT a record into the database.
sql = "SELECT * FROM tweetTable LIMIT 10" 
unarr = []
try:
    # Execute the SQL command
    cursor.execute(sql)
    # Fetch all the rows in a list of lists.
    results = cursor.fetchall()
    for row in results:
        time = row[0]
        uname = row[1]
        tweet = row[2]

        tknzr = TweetTokenizer(strip_handles=True, reduce_len=True)
        print tknzr.tokenize(tweet)
#        print tweet
        if uname not in unarr:
            unarr.append(uname)
            
        
#        if len(unarr) > 150:
#            break
        
except:
   print "Error: unable to fetch data"

print unarr
print len(unarr)
# disconnect from server
db.close()
