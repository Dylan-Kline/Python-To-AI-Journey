import sqlite3
import json
import codecs

# Connect to Geodata database
database = sqlite3.connect('Geodata.sqlite')
ex = database.cursor()

ex.execute("SELECT * FROM Locations")
fhand = codecs.open('where.js', 'w', "utf-8")
fhand.write("myData = [\n")
count = 0

for row in ex:
    
    # Grab Lat, lng, and formatted address, output as list
    data = str(row[1].decode())
    try:
        js = json.loads(str(data))
    except:
        continue
    
    #Check status of data
    if not('status' in js and js['status'] == 'OK') : continue
    
    # Retrieve Latitude and Longitude
    lat = js['results'][0]['geometry']['location']['lat']
    lng = js['results'][0]['geometry']['location']['lng']
    if lat == 0 or lng == 0: continue
    
    # Retrieve address
    address = js['results'][0]['formatted_address']
    address = address.replace("'", "")
    
    try:
        print(lat, lng, address)
        
        # Write gathered geodata in list format to where.js
        count += 1
        if count > 1: fhand.write(',\n')
        output = f"[{str(lat)},{str(lng)}, '{address}']"
        fhand.write(output)
        
    except:
        continue

fhand.write("\n];\n")

fhand.close()
ex.close()
database.close()

print(count, "records written to where.js")
print("Open where.html to view the data in a browser")
