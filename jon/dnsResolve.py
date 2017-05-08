import threading
import socket
import csv
import pandas as pd
import re
import iptools
import sqlite3
import itertools

conn = sqlite3.connect('ipData.db')
addressPattern = re.compile(r'^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$')            

def createTable():
    conn.execute('CREATE TABLE IF NOT EXISTS enriched (ip_addr VARCHAR PRIMARY KEY, name_record VARCHAR, block_id INTEGER, FOREIGN KEY (block_id) REFERENCES ip_blocks(block_id));')

def isIP(record):
    if addressPattern.match(record):
        return True
    else:
        return False

def trimName(record):
    segmented = record.split('.')
    # Preserve reverse DNS records
    if record[-1] == 'arpa':
        pass
    else:
        pass
    

def getBlockId(ip):
    addr = iptools.ipv4.ip2long(ip)
    results = conn.execute('SELECT block_id FROM ip_blocks WHERE start <= %i AND stop >= %i;' % (addr, addr))
    try:
        return results.fetchone()[0]
    except:
        return None

if __name__ == '__main__':
    createTable()
    
    with open('hosts.csv', 'r') as csvfile:
        reader = csv.reader(csvfile)
        inserts = list()
        for row in reader:
    	    addr = row[0].strip()
	    if isIP(addr):
                block_id = getBlockId(addr)
	        if block_id:
	            inserts.append((addr, block_id))
	            if len(inserts) <= 2000:
	                pass
	            else:
	                conn.executemany('INSERT OR REPLACE INTO enriched (ip_addr, block_id) VALUES (?,?)', inserts)
	                conn.commit()
	                del inserts[:]
	    else:
	        pass
	if len(inserts) > 0:
	    conn.executemany('INSERT OR REPLACE INTO enriched (ip_addr, block_id) VALUES (?,?)', inserts)
	    conn.commit()
	    del inserts[:]
            
