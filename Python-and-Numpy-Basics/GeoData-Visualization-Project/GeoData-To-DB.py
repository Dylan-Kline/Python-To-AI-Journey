import json
import urllib.parse, urllib.request, urllib.error
import ssl
import sqlite3
import time

# If you have a google places API key put it here
api_key = False

if api_key == False:
    api_key = 0
    service_url = "http://py4e-data.dr-chuck.net/json?"
else:
    service_url = "https://maps.googleapis.com/maps/api/geocode/json?"
    
# Create the database
database = sqlite3.connect('Geodata.sqlite')
exObj = database.cursor()
exObj.execute('''CREATE TABLE IF NOT EXISTS Locations (address TEXT, geodata TEXT)''')

# Ignore SSL certificate errors
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

# Read where.data for locations to send to Google API
fhand = open('where.data')
count = 0 # Counts the number of locations recieved
for line in fhand:
    # Set limit of retrievals 
    if count > 200:
        print("Recieved 200 locations, restart the program to recieve more.")
        break

    # Check database to see if it already has the location
    address = line.strip()
    print('')
    
    exObj.execute('SELECT geodata FROM Locations WHERE address = (?)', (memoryview(address.encode()),))
    try:
        data = exObj.fetchone()[0]
        print('Found in database ', address)
        continue
    except:
        pass
    
    # Generate absolute url
    parms = dict()
    parms['address'] = address
    if api_key is not False: parms['key'] = api_key
    url = service_url + urllib.parse.urlencode(parms)

    # Open API url and recieve the generated data
    print("Retrieving ", url)
    openUrl = urllib.request.urlopen(url, context=ctx)
    rawData = openUrl.read().decode()
    print('Retrieved', len(rawData), 'characters', rawData[:20].replace('\n', ' '))
    count += 1
    
    # Convert JSON
    try:
        data = json.loads(rawData)
    except:
        print(rawData)
        continue

    # Check if data was recieved properly
    if 'status' not in data or (data['status'] != 'OK' and data['status'] != 'ZERO_RESULTS') :
        print('==== Failure To Retrieve ====')
        print(data)
        break
    
    # Now insert the retrieved data into the database
    exObj.execute('INSERT INTO Locations (address, geodata) VALUES (?, ?)', 
                  (memoryview(address.encode()), memoryview(rawData.encode())))
    database.commit()
    
    if count % 10 == 0:
        print('Pausing for a bit...')
        time.sleep(2)
        
fhand.close()
exObj.close()
database.close()
