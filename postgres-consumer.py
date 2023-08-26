#!/bin/python3

#           DO WHAT THE FUCK YOU WANT TO PUBLIC LICENSE
#                   Version 2, December 2004
# 
# Copyright (C) 2023 William Welna
#
# Everyone is permitted to copy and distribute verbatim or modified
# copies of this license document, and changing it is allowed as long
# as the name is changed.
# 
#           DO WHAT THE FUCK YOU WANT TO PUBLIC LICENSE
#  TERMS AND CONDITIONS FOR COPYING, DISTRIBUTION AND MODIFICATION
#
# 0. You just DO WHAT THE FUCK YOU WANT TO.

import json
import redis
import pg8000

def main():
    r = redis.Redis()
    c = pg8000.connect(user='bluesky', password='toomanysecrets', database='bluesky')

    while True:
        topush = []
        quit_flag = False

        for weeee in range(100000):
            t = r.lpop("BlueSky-Firehose")
            if t is not None:
                topush.append(json.loads(t.decode('utf-8')))
        
        if len(topush) < 1: quit(0)
        if len(topush) < 1000: quit_flag = True

        with c.cursor() as cursor:
            for y in topush:

                if 'record' in y.keys():
                    if 'createdAt' in y['record'].keys():
                        if y['record']['createdAt'].startswith('0000-'):
                            r.lpush("BlueSky-Firehose-Unprocessed", json.dumps(y))
                            continue

                if y['type'] == "app.bsky.graph.follow":
                    cursor.execute(
                        'INSERT INTO follow (date,cid,uri,repo,subject,createdAt) VALUES (%s,%s,%s,%s,%s,%s)',
                        (y['date'], y['cid'], y['uri'], y['repo'], y['record']['subject'], y['record']['createdAt'])
                    )
                elif y['type'] == "app.bsky.graph.block":
                    cursor.execute(
                        'INSERT INTO block (date,cid,uri,repo,subject,createdAt) VALUES (%s,%s,%s,%s,%s,%s)',
                        (y['date'], y['cid'], y['uri'], y['repo'], y['record']['subject'], y['record']['createdAt'])
                    )
                elif y['type'] == "app.bsky.feed.like":
                    cursor.execute(
                        'INSERT INTO feed_like (date,cid,uri,repo,subject,createdAt) VALUES (%s,%s,%s,%s,%s,%s)',
                        (y['date'], y['cid'], y['uri'], y['repo'], y['record']['subject'], y['record']['createdAt'])
                    )
                elif y['type'] == "app.bsky.feed.repost":
                    cursor.execute(
                        'INSERT INTO feed_repost (date,cid,uri,repo,subject_cid,subject_uri,createdAt) VALUES (%s,%s,%s,%s,%s,%s, %s)',
                        (y['date'], y['cid'], y['uri'], y['repo'], y['record']['subject']['cid'],y['record']['subject']['uri'], y['record']['createdAt'])
                    )
                elif y['type'] == "app.bsky.feed.post":
                    cursor.execute(
                        'INSERT INTO feed_post (date,cid,uri,repo,record_text,createdAt,record) VALUES (%s,%s,%s,%s,%s,%s,%s)',
                        (y['date'], y['cid'], y['uri'], y['repo'], y['record']['text'], y['record']['createdAt'], json.dumps(y['record']))
                    )
                elif y['type'] == "app.bsky.actor.profile": r.lpush("BlueSky-Firehose-Unprocessed", json.dumps(y))
                elif y['type'] == "app.bsky.feed.generator": r.lpush("BlueSky-Firehose-Unprocessed", json.dumps(y))
                elif y['type'] == "app.bsky.graph.listitem": r.lpush("BlueSky-Firehose-Unprocessed", json.dumps(y))
                elif y['type'] == "app.bsky.graph.list": r.lpush("BlueSky-Firehose-Unprocessed", json.dumps(y))
                else: pass
            
            c.commit()
        if quit_flag == True: quit(0)

main()
