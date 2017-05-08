import sqlite3
import os.path
import csv
import iptools

conn = sqlite3.connect('ipData.db')
conn.text_factory = str
countryFiles = ['GeoLite2-Country-Blocks-IPv4.csv', 'GeoLite2-Country-Locations-en.csv']
cityFiles = ['GeoLite2-City-Blocks-IPv4.csv', 'GeoLite2-City-Locations-en.csv']
tables = ['ip_blocks', 'country_locations', 'city_locations']

def safeInt(x):
    if x:
        return int(x)
    else:
        return 'NULL'

def populateCountryData():
    with open('GeoLite2-Country-Locations-en.csv', 'r') as csvFile:
        reader = csv.reader(csvFile)
        inserts = list()
        for row in reader:
            if row[0] and row[0].startswith('geoname_id'):
                pass
            else:
                inserts.append((int(row[0]), row[1], row[2], row[3], row[4], row[5]))
            if len(inserts) <= 2000:
                pass
            else:
                conn.executemany('INSERT INTO country_locations(geoname_id, locale_code, continent_code, continent_name, country_iso_code, country_name) VALUES (?,?,?,?,?,?)', inserts)
                conn.commit()
                del inserts[:]
        if len(inserts) > 0:
            conn.executemany('INSERT INTO country_locations(geoname_id, locale_code, continent_code, continent_name, country_iso_code, country_name) VALUES (?,?,?,?,?,?)', inserts)
            conn.commit()
            del inserts[:]
        else:
            pass
    print 'INFO: Created the country_locations table.'

def populateCityData():
    pass

def populateIpData():
    with open('GeoLite2-Country-Blocks-IPv4.csv', 'r') as csvFile:
        reader = csv.reader(csvFile)
        inserts = list()
        for row in reader:
            if row[0].startswith('network'):
                pass
            else:
                range = iptools.ipv4.cidr2block(row[0])
                start = iptools.ipv4.ip2long(range[0])
                stop = iptools.ipv4.ip2long(range[1])
                inserts.append((start, stop, safeInt(row[1]), safeInt(row[2])))
            if len(inserts) <= 2000:
                pass
            else:
                conn.executemany('INSERT INTO ip_blocks(start, stop, geoname_id, registered_country_geoname_id) VALUES (?,?,?,?)', inserts)
                conn.commit()
                del inserts[:]
        if len(inserts) > 0:
            conn.executemany('INSERT INTO ip_blocks(start, stop, geoname_id, registered_country_geoname_id) VALUES (?,?,?,?)', inserts)
            del inserts[:]
    print 'INFO: Created the ip_blocks table.'

def buildTables(resolution='country'):
    for table in tables:
        conn.execute('DROP TABLE IF EXISTS %s;' % table)    
    if resolution == 'country':
        conn.execute('CREATE TABLE country_locations (geoname_id INTEGER PRIMARY KEY, locale_code VARCHAR, continent_code VARCHAR, continent_name VARCHAR, country_iso_code VARCHAR, country_name VARCHAR);')
        conn.execute('CREATE TABLE ip_blocks (block_id INTEGER PRIMARY KEY AUTOINCREMENT, start INTEGER, stop INTEGER, geoname_id INTEGER, registered_country_geoname_id INTEGER, FOREIGN KEY(geoname_id) REFERENCES country_locations(geoname_id));')        
        print 'INFO: Created table schemas.'
        populateCountryData()
        populateIpData()
    elif resolution == 'city':
        pass
    else:
        'ERROR: Unsupported resolution of %s. Must set to "city" or "country" only.' % str(resolution)


def fileCheck(resolution='country'):
    if resolution == 'country':
        for f in countryFiles:
            try:
                pass
            except:
                print 'ERROR: Missing file %s which is required to build database.' % str(f)
                return False
        return True
    elif resolution == 'city':
        for f in cityFiles:
            if os.path.isfile(f):
                pass
            else:
                print 'ERROR: Missing file %s which is required to build database.' % str(f)
                return False
        return True
    else:
        'ERROR: Unsupported resolution of %s. Must set to "city" or "country" only.' % str(resolution)

def exportcsv():
    results = conn.execute('SELECT * FROM ip_blocks, country_locations WHERE ip_blocks.geoname_id = country_locations.geoname_id;')
    with open('ipData.csv', 'wb') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['start', 'stop', 'continent_code', 'continent_name', 'country_iso_code', 'country_name'])
        for row in results:
            row = (row[1], row[2], row[7], row[8], row[9], row[10])
            writer.writerow(row)

if __name__ == '__main__':
    exportcsv()
    conn.close()
    # if fileCheck():
    #    buildTables()
    # else:
    #    print 'INFO: Exiting application...'
    # conn.close()
