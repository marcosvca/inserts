import psycopg2, P, re

conn = psycopg2.connect(database="GroceryGourmetFood", user="postgres", password="root", host="localhost", port="5432")

print("Opened database successfully")

cur = conn.cursor()

data = P.parse("C:\\Users\\Marcos\\Downloads\\meta_Grocery_and_Gourmet_Food.json.gz")

l = list(data)

print("Data converted into a list")
count = 0 
for product in l:
    if ('categories' in product) and (len(product['categories'])!= 0):
        for c in product['categories']:
            for e in c:
                cat = re.sub('[^a-zA-Z0-9 \n\.,]', 'and', e)
                string = """INSERT INTO product_category (productid, categoryid)
                            SELECT productid, categoryid
                            FROM product, category
                            WHERE productid = \'""" + product['asin'] + """\' AND categoryid = (select categoryid
                                                                                    from category
                                                                                    where description = \'"""+ cat + """\')
                            AND NOT EXISTS (select productid, categoryid
                                            from product_category
                                            where productid = \'""" + product['asin'] + """\' AND categoryid = (select categoryid
                                                                                             from category
                                                                                             where description = \'""" + cat + """\'))"""
                cur.execute(string)
    count = count+1
    if count == 10000:
        print("Ten Tousand products already processed")
        count = 0
                
conn.commit()
print("Records stored sucessfully")
conn.close()

                         


