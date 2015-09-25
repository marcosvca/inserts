INSERT INTO product_category (productid, categoryid) 
	SELECT productid, categoryid
	FROM product, category
	WHERE productid = '0657745316' AND categoryid = (select categoryid
								from category
								where description = 'Coffee')
	AND NOT EXISTS (select productid, categoryid
			from product_category
			where productid = '0657745316' AND categoryid = (select categoryid
									 from category
									 where description = 'Coffee'))

SELECt ID.CATEGORYID
FROM 
(select categoryid
from category
where description = 'Coffee') AS ID


"INSERT INTO product_category (productid, categoryid) 
	SELECT productid, categoryid
	FROM product, category
	WHERE productid = \'" + product['asin'] + "\' AND categoryid = (select categoryid
								from category
								where description = \'"+ cat + "\')
	AND NOT EXISTS (select productid, categoryid
			from product_category
			where productid = \'" + product['asin'] + "\' AND categoryid = (select categoryid
									 from category
									 where description = \'" + cat + "\'))"



