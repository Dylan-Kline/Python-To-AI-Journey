import sqlite3 as sq
import json

# Create database file and execution object
database = sq.connect('roster_db.db')
obj = database.cursor()

# Create Database structure
obj.executescript('''
    DROP TABLE IF EXISTS User;
    DROP TABLE IF EXISTS Course;
    DROP TABLE IF EXISTS Member;
    
    CREATE TABLE User (
        id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
        name TEXT UNIQUE
        );
    
    CREATE TABLE Course (
        id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
        title TEXT UNIQUE
        );
        
    CREATE TABLE Member (
        user_id INTEGER,
        course_id INTEGER,
        role INTEGER,
        PRIMARY KEY (user_id, course_id)
        );
        ''')

# Get file name or set it to default
fName = input("Enter the file name or hit enter: ")
if len(fName) < 1: fName = 'roster_data.json'

# Open file and retrieve json data
file = open(fName)
rawData = file.read()
data = json.loads(rawData)

# Find name, title, and role from recieved JSON data, and input into the database
for entry in data:
    
    name = entry[0]
    title = entry[1]
    role = entry[2]
    
    # Insert name into User Table
    obj.execute('INSERT OR IGNORE INTO User (name) VALUES (?)', (name,))
    obj.execute('SELECT id FROM User WHERE name = ?', (name,))
    user_id = obj.fetchone()[0]
    
    # Insert course title into Course Table
    obj.execute('INSERT OR IGNORE INTO Course (title) VALUES (?)', (title,))
    obj.execute('SELECT id FROM Course WHERE title = ?', (title,))
    course_id = obj.fetchone()[0]
    
    # Insert user_id, course_id, and role into Member Table
    obj.execute('''INSERT OR IGNORE INTO Member (user_id, course_id, role)
                   VALUES (?, ?, ?)''', (user_id, course_id, role))
    
    database.commit()

# SQL Test Command 1
obj.execute('''SELECT User.name, Course.title, Member.role 
               FROM User JOIN Member JOIN Course
               ON User.id = Member.user_id AND Course.id = Member.course_id
               ORDER BY User.name DESC, Course.title DESC, Member.role DESC LIMIT 2''')
Course = obj.fetchall()

print("\nTest 1:")
for user in Course:
    print(user)
    
# SQL Test Command 2
obj.execute('''SELECT 'XYZZY' || hex(User.name || Course.title || Member.role ) AS X 
               FROM User JOIN Member JOIN Course 
               ON User.id = Member.user_id AND Member.course_id = Course.id
               ORDER BY X LIMIT 1''')
hexa = obj.fetchone()[0]
print("\nTest 2:")
print("Should be XYZZY416172726F6E736933333430: ", hexa)

file.close()
obj.close()
database.close()
