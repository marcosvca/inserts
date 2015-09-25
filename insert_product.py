import psycopg2, P

conn = psycopg2.connect(database="GroceryGourmetFood", user="postgres", password="root", host="localhost", port="5432")

print("Opened database successfully")

cur = conn.cursor()

data = P.parse("C:\\Users\\Marcos\\Downloads\\meta_Grocery_and_Gourmet_Food.json.gz")

l = list(data)

print("Data converted into a list")

for p in l:
    title = None
    price = None
    image = None
    description = None
    brand = None
    asin = None
    if 'title' in p:
        title = p['title']
    if 'price' in p:
        price = p['price']
    if 'imUrl' in p:
        image = p['imUrl']
    if 'description' in p:
        description = p['description']
    if 'brand' in p:
        brand = p['brand']
    if 'asin' in p:
        asin  = p['asin']
        cur.execute("""INSERT INTO PRODUCT (PRODUCTID, TITLE, PRICE, IMAGE, BRAND, DESCRIPTION)
                                   VALUES (%s, %s, %s, %s, %s, %s)""", (asin, title, price, image, brand, description,))

conn.commit()
print("Records stored sucessfully")
conn.close()

    
