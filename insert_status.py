import psycopg2, P, re
from decimal import *

conn = psycopg2.connect(database="GroceryGourmetFood", user="postgres", password="root", host="localhost", port="5432")

print("Opened database successfully")

cur = conn.cursor()


cur.execute(''' SELECT productid, rating, reviewdate, reviewid
                FROM review
                ORDER BY productid, reviewdate''')

print("Get data successfully")

result = cur.fetchall()

scores = [0,0,0,0,0]
product = ''
rcount = 0
pcount = 0
p = 0

for row in result:
    pcount += 1
    if product != row[0]:
        product = row[0]
        rcount = 0
        scores = [0,0,0,0,0]
        
    rcount += 1
    scores[int(row[1]) -1] += 1
    cur.execute('''INSERT INTO status (productid, reviewid, one, two, three, four, five, reviewcount, reviewdate)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) 
                   ''', (row[0], row[3], scores[0], scores[1], scores[2], scores[3], scores[4],rcount,row[2]))
    
    if pcount == 129717:
        p +=10
        print("%d/%% concluído)" % p)
        pcount = 0
        
    
conn.commit()
print("Records stored sucessfully")
conn.close()


