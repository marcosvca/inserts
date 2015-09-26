import psycopg2, P, re

conn = psycopg2.connect(database="GroceryGourmetFood", user="postgres", password="root", host="localhost", port="5432")
cur  = conn.cursor()

print("Opened database successfully")

data = P.parse("C:\\Users\\Marcos\\Downloads\\reviews_Grocery_and_Gourmet_Food.json.gz")

l = list(data)

print("Data converted into a list")

count = 0 
for review in l:
    if 'reviewerName' in review:
        name = re.sub('[^a-zA-Z0-9 \n\.,]', '', review['reviewerName'])
    else:
        name = ""
    text = re.sub('[^a-zA-Z0-9 \n\.,]', '', review['reviewText'])
    summary = re.sub('[^a-zA-Z0-9 \n\.,]', '', review['summary'])
    helpful = re.sub('[^a-zA-Z0-9 \n\.,]', '', str(review['helpful']))
    cur.execute('''
                    INSERT INTO REVIEW (reviewerid, productid, reviewername, helpful, rating, summary, unixtime, reviewtext)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ''', [review['reviewerID'], review['asin'], name, helpful, review['overall'], summary, review['unixReviewTime'], text])
    count = count+1
    if count == 10000:
        print("Ten Tousand reviews already processed")
        count = 0
conn.commit()
print("Records stored sucessfully")
conn.close()
