'''
    COUNTING EMAIL WITH ORGANIZATIONS DOMAIN NAME

This application will read the mailbox data (mbox.txt) count up the number email messages per organization 
(i.e. domain name of the email address) using a database with the following schema to maintain the counts:

CREATE TABLE Counts (org TEXT, count INTEGER)

When you have run the program on mbox.txt upload the resulting database file above for grading.'''

import sqlite3
import re

# Connect to database
database = sqlite3.connect('countingEmailsDB.db')
obj = database.cursor()

# Create counts table in database
obj.execute('DROP TABLE IF EXISTS Counts')
obj.execute('CREATE TABLE Counts (org TEXT, count INTEGER)')

# Open email text file (mbox-short.txt) for parsing
fhand = open('mbox-short.txt')
lst = list()
for line in fhand:
    
    if not line.startswith('From: '): continue
    
    # Find email domain from file line
    words = line.split()
    email = words[1]
    domain = email.find('@')
    org = email[domain+1:len(email)]
    
    # Select row to find the count of emails from the provided org name
    obj.execute('SELECT count FROM Counts WHERE org = ? ', (org,))
    row = obj.fetchone()
    
    # If there does not exist a row for the org, create it and insert a default count 1 for it, else increment the count of the org
    if row is None:
        obj.execute('INSERT INTO Counts (org, count) VALUES (?, 1)', (org,))
    else:
        obj.execute('UPDATE Counts SET count = count + 1 WHERE org = ?', (org,))
        
    database.commit()

# Displays the number of emails total from each organization in descending order
for row in obj.execute('SELECT org, count FROM Counts ORDER BY count DESC LIMIT 10'):
    print(row)
    
fhand.close()
obj.close()
