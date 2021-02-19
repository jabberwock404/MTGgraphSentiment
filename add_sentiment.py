# Usage: python addsentiment.py [Subreddit] [Field]
# Authors: JackLee9355
# Created 2/19/2021

# The scope of this file is concerned solely with taking a TEXT column and adding a corresponding sentiment column.
# Using nltk as a pretrained model for now.
# ASSUMES vader_lexicon IS DOWNLOADED BEFORE USE.
# To download:
# python -i
# import nltk
# nltk.download
# Find and download vader_lexicon

import sqlite3
import add_column
from config import DATABASES_PATH
from nltk.sentiment import SentimentIntensityAnalyzer 
import sys
import time
import json

def calculate_sentiment_before(database, field, before):
    sia = SentimentIntensityAnalyzer()
    cursor = database.cursor()
    cursor.execute("SELECT id, " + field + " FROM submissions WHERE created_utc < " + str(before) + " AND " + field + " IS NOT NULL;")
    rows = cursor.fetchall()
    for row in rows:
        scores = sia.polarity_scores(row[1])
        # Storing the dictionary dump here is lazy, but works. 
        cursor.execute("UPDATE submissions SET " + field + "_sentiment = (?) WHERE id IS '" + str(row[0]) + "';", (json.dumps(scores),))
    database.commit()


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Wrong number of arguments. Quiting")
        quit()
    subreddit = sys.argv[1]
    field = sys.argv[2]

    database = sqlite3.connect(DATABASES_PATH + subreddit + ".db")
    add_column.create_column(database, field + "_sentiment", "TEXT")

    calculate_sentiment_before(database, field, int(time.time()))

