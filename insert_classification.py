import psycopg2, P, re


conn = psycopg2.connect(database="GroceryGourmetFood", user="postgres", password="root", host="localhost", port="5432")

print("Opened database successfully")

cur = conn.cursor()

data = P.parse("C:\\Users\\Marcos\\Downloads\\meta_Grocery_and_Gourmet_Food.json.gz")

l = list(data)

print("Data converted into a list")

for p in l:
    if ('categories' in p) and (len(p['categories'])!= 0):
        for c in p['categories']:
            for e in c:
                cat = re.sub('[^a-zA-Z0-9 \n\.]', 'and', e)
                string = "INSERT INTO CATEGORY (DESCRIPTION) SELECT \'" +cat+ "\' WHERE NOT EXISTS(SELECT description FROM category where description = \'" +cat+ " \')"
                cur.execute(string)

conn.commit()
print("Records stored sucessfully")
conn.close()

