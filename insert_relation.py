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


for product in l[1:10]:
    if 'related' in product:
        if 'also_viewed' in product['related']:
            for av in product['related']['also_viewed']:
                #Discarting products with no meta data registred
                if av in prod_list:
                    print(av)




