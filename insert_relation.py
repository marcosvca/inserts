import psycopg2, P, re
import psycopg2.extras

conn = psycopg2.connect(database="GroceryGourmetFood", user="postgres", password="root", host="localhost", port="5432")

print("Opened database successfully")

#cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
cur  = conn.cursor()


data = P.parse("C:\\Users\\Marcos\\Downloads\\meta_Grocery_and_Gourmet_Food.json.gz")

l = list(data)

print("Data converted into a list")

cur.execute("SELECT productid FROM product")

result = cur.fetchall()


prod_list = []

for row in result:
    prod_list.append(row[0])

count = 0

for product in l:
    if 'related' in product:
        if 'also_viewed' in product['related']:
            for related_av in product['related']['also_viewed']:
                #Discarting products with no meta data registred
                if related_av in prod_list:
                    cur.execute(""" WITH upsert AS (UPDATE relation 
                                            SET also_viewed = TRUE
                                            WHERE selected_product= %s AND related_product= %s
                                            RETURNING *) 
                                    INSERT INTO relation(selected_product, related_product, also_viewed) 
                                    SELECT %s, %s, TRUE 
                                    WHERE NOT EXISTS (SELECT * FROM upsert);""", (product['asin'], related_av,product['asin'], related_av))

        if 'also_bought' in product['related']:
            for related_ab in product['related']['also_bought']:
                #Discarting products with no meta data registred
                if related_ab in prod_list:
                    cur.execute(""" WITH upsert AS (UPDATE relation 
                                            SET also_bought = TRUE
                                            WHERE selected_product= %s AND related_product= %s
                                            RETURNING *) 
                                    INSERT INTO relation(selected_product, related_product, also_bought) 
                                    SELECT %s, %s, TRUE 
                                            WHERE NOT EXISTS (SELECT * FROM upsert);""", (product['asin'], related_ab,product['asin'], related_ab))
                            
        if 'bought_together' in product['related']:
            for related_bt in product['related']['bought_together']:
                #Discarting products with no meta data registred
                if related_bt in prod_list:
                    cur.execute(""" WITH upsert AS (UPDATE relation 
                                            SET bought_together = TRUE
                                            WHERE selected_product= %s AND related_product= %s
                                            RETURNING *) 
                                    INSERT INTO relation(selected_product, related_product, bought_together) 
                                    SELECT %s, %s, TRUE 
                                    WHERE NOT EXISTS (SELECT * FROM upsert);""", (product['asin'], related_bt,product['asin'], related_bt))
                            
        if 'buy_after_viewing' in product['related']:
            for related_av in product['related']['buy_after_viewing']:
                #Discarting products with no meta data registred
                if related_av in prod_list:
                    cur.execute(""" WITH upsert AS (UPDATE relation 
                                            SET buy_after_viewing = TRUE
                                            WHERE selected_product= %s AND related_product= %s
                                            RETURNING *) 
                                    INSERT INTO relation(selected_product, related_product, buy_after_viewing) 
                                    SELECT %s, %s, TRUE 
                                    WHERE NOT EXISTS (SELECT * FROM upsert);""", (product['asin'], related_av,product['asin'], related_av))
    count = count+1
    if count == 10000:
        print("Ten Thousand products processed")
        count = 0
conn.commit()
print("Records stored sucessfully")
conn.close()


