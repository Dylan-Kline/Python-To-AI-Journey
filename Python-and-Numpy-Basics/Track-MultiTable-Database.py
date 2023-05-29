"""Musical Track Database

This application will read an iTunes export file in XML and produce a properly 
normalized database with this structure:

CREATE TABLE Artist (
    id  INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
    name    TEXT UNIQUE
);

CREATE TABLE Genre (
    id  INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
    name    TEXT UNIQUE
);

CREATE TABLE Album (
    id  INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
    artist_id  INTEGER,
    title   TEXT UNIQUE
);

CREATE TABLE Track (
    id  INTEGER NOT NULL PRIMARY KEY 
        AUTOINCREMENT UNIQUE,
    title TEXT  UNIQUE,
    album_id  INTEGER,
    genre_id  INTEGER,
    len INTEGER, rating INTEGER, count INTEGER
);

If you run the program multiple times in testing or with different files, make
sure to empty out the data before each run.
"""

import sqlite3 as sq
import xml.etree.ElementTree as ET

# Create database and database exectution object
database = sq.connect('multiTableDB.db')
obj = database.cursor()

# Creates Tracks, ALbum, Genre, and Artist tables
obj.executescript('''
    DROP TABLE IF EXISTS Track;
    DROP TABLE IF EXISTS Album;
    DROP TABLE IF EXISTS Genre;
    DROP TABLE IF EXISTS Artist;
    
    CREATE TABLE Track (
        id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE, 
        title TEXT UNIQUE,
        album_id INTEGER,
        genre_id INTEGER,
        len INTEGER, rating INTEGER, count INTEGER
    );
    
    CREATE TABLE Artist (
        id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
        name TEXT UNIQUE
    );
    
    CREATE TABLE Genre (
        id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
        name TEXT UNIQUE
    );
    
    CREATE TABLE Album (
        id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
        artist_id INTEGER,
        title TEXT UNIQUE
    );
    ''')

# Asks for file name and if no filename is entered set it to default
fName = input('Enter a file name or hit enter: ')
if len(fName) < 1: fName = 'Library.xml'

# Lookup function to find the text from a given xml parent node
def lookup(dct, key):
    found = False
    for child in dct:
        if found: return child.text
        if child.tag == 'key' and child.text == key:
            found = True
    return None
    
# Create ELement tree for xml file
tree = ET.parse(fName)
allTracks = tree.findall('dict/dict/dict')

# Find 
for entry in allTracks:
    if lookup(entry, 'Track ID') == None: continue
    
    title = lookup(entry, 'Name')
    artist = lookup(entry, 'Artist')
    album = lookup(entry, 'Album')
    genre = lookup(entry, 'Genre')
    length = lookup(entry, 'Total Time')
    rating = lookup(entry, 'Rating')
    count = lookup(entry, 'Play Count')
    
    if title is None or artist is None or album is None or genre is None: continue
    
    # Insert artist name and get the artist_id
    obj.execute('INSERT OR IGNORE INTO Artist (name) VALUES (?)', (artist,))
    obj.execute('SELECT id FROM Artist WHERE name = ?', (artist,))
    artist_id = obj.fetchone()[0]
    
    # Insert genre and get genre_id
    obj.execute('INSERT OR IGNORE INTO Genre (name) VALUES (?)', (genre,))
    obj.execute('SELECT id FROM Genre WHERE name = ?', (genre,))
    genre_id = obj.fetchone()[0]
    
    # Insert album name into Album table and get album_id
    obj.execute('INSERT OR IGNORE INTO Album (artist_id, title) VALUES (?, ?)', (artist_id, album))
    obj.execute('SELECT id FROM Album WHERE title = ?', (album,))
    album_id = obj.fetchone()[0]
 
    # Insert Track info into Track Table
    obj.execute('''INSERT OR REPLACE INTO Track 
                    (title, album_id, genre_id, len, rating, count)
                    VALUES (?, ?, ?, ?, ?, ?)''',
                    (title, album_id, genre_id, length, rating, count))
    
    # Force database update
    database.commit()

# Tests the program for the correct database 
obj.execute('''SELECT Track.title, Artist.name, Album.title, Genre.name 
               FROM Track JOIN Genre JOIN Album JOIN Artist 
               ON Track.genre_id = Genre.ID and Track.album_id = Album.id 
                    AND Album.artist_id = Artist.id
               ORDER BY Artist.name LIMIT 3
               ''')

# Displays test query output
tracks = obj.fetchall()
for song in tracks:
    print(song)
    
obj.close()
