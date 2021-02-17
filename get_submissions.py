#Usage: python get_submissions.py [Subreddit name]
#Authors: JackLee9355
#Created: 2/16/2021

#The scope of this file is solely fetching submissions and saving them to a sql database for further usage elsewhere.

import requests
import sqlite3
import sys
import os
import time

#A maximum of ten million submissions can be retrived with this depth
sys.setrecursionlimit(100000)

PUSHSHIFT_URL = "https://api.pushshift.io/reddit/submission/search/?"
DATABASES_PATH = "databases/"

total_fetched = 0

def get_json_from_url(url):
    request = None
    attempts = 5
    while (request is None or request.status_code != 200) and attempts > 0:
        request = requests.get(url)
        if request.status_code != 200:
            print("Wrong status code. Retrying/sleeping.")
            time.sleep(60 * (5 - attempts))
            attempts -= 1

    if request.status_code != 200:
        raise Exception("Request wouldn't get status 200")
    return request.json()

def build_url(params):
    first = True
    to_return = PUSHSHIFT_URL
    for param in params:
        if not first:
            to_return += "&"
        to_return += param + "=" + params[param]
        first = False
    return to_return

def save_submission_json(submission_json, database):
    cursor = database.cursor()
    for item in submission_json:
        cursor.execute("INSERT INTO submissions VALUES (?, ?, ?)", (str(item["id"]), int(item["created_utc"]), str(item)))
    database.commit()

def save_all_before(subreddit, utc, database):
    global total_fetched
    #Pushshift caps size to 100
    url = build_url({"subreddit": subreddit, "before": str(utc), "size": "100"})
    submission_json = get_json_from_url(url)["data"]
    if len(submission_json) > 0:
        total_fetched += len(submission_json)
        print(total_fetched)
        save_submission_json(submission_json, database)
        final_time = submission_json[len(submission_json) - 1]["created_utc"]
        save_all_before(subreddit, final_time, database)

if __name__ == "__main__":

    if len(sys.argv) != 2:
        print("Wrong number of arguments. Quiting")
        quit()
    subreddit = sys.argv[1]

    if not os.path.exists(DATABASES_PATH):
        os.makedirs(DATABASES_PATH)
    
    database = sqlite3.connect(DATABASES_PATH + subreddit + ".db")
    cursor = database.cursor()
    #Checks if the table exists. If it doesn't creates it.
    cursor.execute('''SELECT name FROM sqlite_master WHERE type='table' AND name='submissions';''')
    if len(cursor.fetchall()) == 0:
        cursor.execute('''CREATE TABLE submissions (
                                id TEXT UNIQUE,
                                created_utc INTEGER,
                                json TEXT);''')
        database.commit()
    min_utc = cursor.execute('SELECT MIN(created_utc) FROM submissions').fetchone()
    before = min_utc[0] if min_utc[0] is not None else int(time.time())
    save_all_before(subreddit, before, database)




