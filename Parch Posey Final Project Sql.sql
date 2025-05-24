SELECT * FROM web_events LIMIT 2

SELECT * FROM orders LIMIT 2

SELECT * FROM accounts LIMIT 2

SELECT * FROM region LIMIT 2

SELECT * FROM sales_reps LIMIT 2

---------------------------------------DATA------------------------
---------------------------------------------------SubQuery---------------------------------------------------------------------
--Find the names of all accounts (accounts.name) that have placed at least one order.
SELECT accounts.name, count(*) FROM accounts
WHERE accounts.id IN (SELECT account_id FROM orders)
GROUP BY 1

----List the names of all sales reps who have at least one account that made more than $1,000 in total_amt_usd in a single order.

SELECT DISTINCT(sales_reps.name) FROM accounts
LEFT JOIN sales_reps ON Sales_reps.id = accounts.sales_rep_id
WHERE accounts.id IN (SELECT account_id FROM orders
WHERE total_amt_usd > 1000)

SELECT sales_reps.name, COUNT(*), SUM(orders.total_amt_usd) FROM accounts
LEFT JOIN sales_reps ON Sales_reps.id = accounts.sales_rep_id
LEFT JOIN orders ON orders.account_id = accounts.id
WHERE accounts.id IN (SELECT account_id FROM orders
WHERE total_amt_usd > 1000)
GROUP BY 1
----------------------------------------------------SUBQUERY + CTE----------------------------------------------------------------------

--Find the account name and the total amount spent (sum of total_amt_usd) only for those accounts that have made more 
--than two web events (interactions) before placing their first order.
----------------METHOD 1
WITH cte1 AS(
SELECT account_id, MIN(occurred_at) AS first_date FROM orders
GROUP BY 1),
cte2 AS (
SELECT accounts.name, SUM(orders.total_amt_usd) FROM accounts
JOIN orders ON orders.account_id = accounts.id
JOIN web_events ON web_events.account_id = accounts.id
JOIN cte1 ON cte1.account_id = accounts.id
WHERE web_events.occurred_at < cte1.first_date
GROUP BY 1

)
SELECT * FROM cte2

------------------METHOD 2

WITH first_order AS (
SELECT accounts.id AS ids, MIN(orders.occurred_at) AS min_date FROM accounts
LEFT JOIN orders ON orders.account_id = accounts.id
GROUP BY 1
),
filters AS(
SELECT accounts.id AS acc_id, web_events.occurred_at AS date FROM accounts
LEFT JOIN web_events ON web_events.account_id = accounts.id
LEFT JOIN first_order ON first_order.ids = accounts.id
WHERE web_events.occurred_at < first_order.min_date
)

SELECT accounts.name, SUM(orders.total_amt_usd), COUNT(filters.date) FROM accounts
LEFT JOIN orders ON orders.account_id = accounts.id
RIGHT JOIN filters ON filters.acc_id = accounts.id
GROUP BY 1
HAVING COUNT(filters.date) > 2

-------------------------------------------------------CTE-----------------------------------------------------------------------
--Using a CTE, find each account and the number of orders they have placed.

WITH my_cte AS(
SELECT account_id, COUNT(*) AS orders_count FROM orders
GROUP BY 1
)
SELECT accounts.name, my_cte.orders_count FROM accounts
LEFT JOIN my_cte ON my_cte.account_id = accounts.id

---------Using a CTE, find the sales reps who have sold more than $5,000 total (SUM(total_amt_usd)) across all their accounts.

SELECT account_id, SUM(total_amt_usd) FROM orders
GROUP BY 1
HAVING SUM(total_amt_usd) > 5000

WITH table1 AS(
SELECT account_id, SUM(orders.total_amt_usd) As total FROM orders
GROUP BY 1
)

SELECT sales_reps.name FROM sales_reps
JOIN accounts ON accounts.sales_rep_id = sales_reps.id
JOIN table1 ON table1.account_id = accounts.id
GROUP BY 1

--------------Use a CTE to find all accounts where the total number of web events is more than double the total number of orders.
Output: account name, number of web events, number of orders.

WITH table1 AS(
SELECT account_id, COUNT(*) AS web_count FROM web_events
GROUP BY 1
),
table2 AS(
SELECT account_id, COUNT(*) AS order_count FROM orders
GROUP BY 1
)

SELECT accounts.name, table1.web_count AS webh_count, table2.order_count AS o_count FROM accounts
INNER JOIN table1 ON table1.account_id = accounts.id
INNER JOIN table2 ON table2.account_id = accounts.id
WHERE table1.web_count > table2.order_count*2

---------------------------------------------------------VIEW---------------------------------------------------------------------------------

--Create a View that shows each account name along with the total number of orders they have placed.
CREATE VIEW name_orders AS
SELECT accounts.name, COUNT(orders.account_id) FROM accounts
JOIN orders On orders.account_id = accounts.id
GROUP BY 1

SELECT * FROM name_orders

--------------------------------------------------------MATRIALIZES VIEW-------------------------------------------------------------------
--------------Create a View that shows each sales rep's name, the region name they belong to, and the total sales 
--(sum of total_amt_usd) made by all accounts under them.
CREATE MATERIALIZED VIEW sales_rep_pair AS
SELECT sales_reps.name AS rep_name, region.name AS region, SUM(orders.total_amt_usd) FROM orders
JOIN accounts ON accounts.id = orders.account_id
JOIN sales_reps ON sales_reps.id = accounts.sales_rep_id
JOIN region ON region.id = sales_reps.region_id
GROUP BY 1,2

SELECT * FROM sales_rep_pair

REFRESH MATERIALIZED VIEW sales_rep_pair -------------------------------------------REFRESH--

-----------------------------------------------------------------CHECK----------------------------------------------------------------
CREATE TABLE employee(
id SERIAL PRIMARY KEY,
name VARCHAR(20),
age INT,
CHECK(age > 18)
)

CREATE TABLE orders1(
id SERIAL PRIMARY KEY,
prod_name VARCHAR(50),
status VARCHAR(10),
CHECK(status IN ('Pending', 'Shipped', 'Placed', 'Delivered'))
)

-----------------------------------------------------------------ROW NUMBER()------------------------------------------------
------------Find the first order placed by each account based on the occurred_at timestamp.
WITH my_cte AS(
SELECT account_id, occurred_at, ROW_NUMBER() OVER (PARTITION BY account_id) AS Rows_number FROM orders)

SELECT * FROM my_cte
WHERE Rows_number = 1

-----------Identify the latest web event for each account using row numbers and filter only the most recent one.

SELECT account_id, MAX(occurred_at) FROM web_events --direct method
GROUP BY 1

WITH my_cte AS(
SELECT account_id, occurred_at, ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY occurred_at DESC) AS row_num FROM web_events)

SELECT * FROM my_cte
WHERE row_num = 1

----------------------------------------------------------RANK()---------------------------------------------------------------------------
---Rank accounts based on total order amount (total_amt_usd) in descending order, and display the 
---top 5 highest-spending accounts including ties.
SELECT account_id, total_amt_usd, RANK() OVER (ORDER BY total_amt_usd DESC) AS my_rank FROM orders
LIMIT 5

--Rank each account’s orders by their total_amt_usd within the account and display the top 2 per account.
WITH my_cte AS(
SELECT account_id, total_amt_usd, RANK() OVER (PARTITION BY account_id ORDER BY total_amt_usd DESC) AS my_rank FROM orders)

SELECT * FROM my_cte
WHERE my_rank < 3

--------------------------------------------------------------DENSE_RANK()-------------------------------------------------------------
----Within each region, densely rank sales reps based on the number of accounts they manage.
SELECT COUNT(DISTINCT accounts.id) AS countm, sales_rep_id, region.name, DENSE_RANK() OVER (PARTITION BY region.name ORDER BY COUNT(DISTINCT accounts.id)) FROM accounts
JOIN sales_reps ON sales_reps.id = accounts.sales_rep_id
JOIN region ON region.id = sales_reps.region_id
GROUP BY 2, 3
ORDER BY 3 DESC

-----------------------------------------------------NTILE()------------------------------------------------------
---Divide all accounts into 4 quartiles based on their total spending (total_amt_usd). List each account with its quartile.

SELECT account_id, SUM(total_amt_usd), NTILE(4) OVER(ORDER BY SUM(total_amt_usd)) FROM orders
GROUP BY 1

--Split all orders into 5 groups based on order size (total). Identify the group that has the highest average order value.
WITH my_cte AS(
SELECT orders.id, total, NTILE(100) OVER (ORDER BY total DESC) AS tile FROM orders)
SELECT tile, AVG(total) FROM my_cte
GROUP By 1
ORDER BY 2 DESC
LIMIT 1

-----------------------------------------------SUM() OVER()--------------------------------------------------
--Show each order along with the running total of total_amt_usd for its account, ordered by occurred_at.
SELECT account_id, occurred_at, SUM(total_amt_usd) OVER(PARTITION BY account_id ORDER BY occurred_at) FROM orders

-------------------------------------------------COUNT() OVER()---------------------------------------------------------------------
--Calculate the cumulative sum of web events per account over time.
SELECT account_id, occurred_at, COUNT(*) OVER(PARTITION BY account_id ORDER BY occurred_at) FROM web_events

--Show each web event with the count of all events that occurred on the same day.
SELECT occurred_at, COUNT(id) OVER(PARTITION BY DATE(occurred_at)) FROM web_events

---For each order, display the running count of how many orders that account has placed so far.
SELECT *, COUNT(*) OVER(ORDER BY id) FROM orders

-----------------------------------------------------AVG() OVER()----------------------------------------------------------------------
--Calculate the moving average of order totals for each account (ordered by occurred_at).
SELECT account_id, occurred_at, AVG(total_amt_usd) OVER(PARTITION BY account_id ORDER BY occurred_at) FROM orders

-----------------------------------------------LEAD() / LAG()---------------------------------------------------------------------------
--For each account, show each order with its total_amt_usd and the difference from the previous order's total (LAG).
SELECT account_id, total_amt_usd, LAG(total_amt_usd) OVER(PARTITION BY account_id ORDER BY occurred_at) FROM orders

--List each web event along with the time difference (in seconds) to the next web event by the same account (LEAD).
SELECT occurred_at,EXTRACT(SECOND FROM (occurred_at)) FROM orders
----------------------------------------SALER FUNCTIONS-------------------------------------------------------
------
CREATE OR REPLACE FUNCTION count_web_events(acc_id INT)
RETURNS INT AS $$
DECLARE
    result INT;
BEGIN
    SELECT COUNT(*) INTO result
    FROM web_events
    WHERE account_id = acc_id;

    RETURN result;
END;
$$ LANGUAGE plpgsql;

SELECT DISTINCT account_id, count_web_events(account_id) FROM web_events
-------------------------------------------------------------------------------------
CREATE FUNCTION tell(total_amt_usd NUMERIC)
RETURNS VARCHAR AS $$
BEGIN
RETURN CASE WHEN total_amt_usd<500 THEN 'LOW' WHEN total_amt_usd<1500 THEN 'MEDIUM' ELSE 'HIGH' END;
END;
$$ LANGUAGE plpgsql;

SELECT total_amt_usd, tell(total_amt_usd) FROM orders
----------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION get_month(occurred_at TIMESTAMP)
RETURNS INT AS $$
BEGIN
return EXTRACT (DAY FROM occurred_at);
END;
$$ LANGUAGE plpgsql;

SELECT occurred_at, get_month(occurred_at) FROM web_events;
--------------------------------------------------Table-Valued Function-------------------------------------------------------------------
--Create a table-valued function that returns all orders placed by a given account, including:
--order id, occurred_at, total_amt_usd, and total quantity (sum of standard, gloss, and poster).
--Call the function with any account_id.
CREATE OR REPLACE FUNCTION get_details(acc_id INT)
RETURNS TABLE(id INT, occurred_at TIMESTAMP, total_amt_usd NUMERIC, total INT) AS $$
BEGIN
RETURN QUERY
SELECT orders.id, orders.occurred_at, orders.total_amt_usd, orders.total FROM orders
WHERE account_id = acc_id;
END;
$$ LANGUAGE plpgsql;

SELECT * FROM get_details(1081)

-------------Create a TVF that takes a region_id and returns:
--All accounts in that region, their sales_rep name, and total number of orders they’ve placed.
--Use proper joins and grouping logic.
CREATE OR REPLACE FUNCTION get_data(my_id INT)
RETURNS TABLE(id INT, name TEXT ,total INT) AS $$
BEGIN
RETURN QUERY
SELECT accounts.id, sales_reps.name :: TEXT, orders.total FROM orders
LEFT JOIN accounts ON accounts.id = orders.account_id
LEFT JOIN sales_reps ON sales_reps.id = accounts.sales_rep_id
WHERE sales_reps.region_id = my_id;
END;
$$ LANGUAGE plpgsql;

SELECT * FROM get_data(1)
--------------------------------------------------Small Funcs--------------------------------------------------------------------
SELECT CURRENT_DATE 
SELECT CURRENT_TIME
SELECT AGE('2002/11/13', '2025/05/01')

SELECT TO_CHAR(CURRENT_DATE, 'DD/MM/YYYY')

SELECT CAST(1.23654 AS INT)

SELECT TO_DATE('01-05-2025', 'DD-MM-YYYY') AS converted_date;

SELECT TO_NUMBER('1,234.56', '9,999.99') AS converted_number;

SELECT UPPER('hello world') AS upper_text;

-- 2. LOWER
SELECT LOWER('HELLO WORLD') AS lower_text;

-- 3. TRIM
SELECT TRIM('   hello   ') AS trimmed;

-- 4. LTRIM
SELECT LTRIM('   hello') AS left_trimmed;

-- 5. RTRIM
SELECT RTRIM('hello   ') AS right_trimmed;

-- 6.Replace
SELECT REPLACE('I love apples alot', 'apples', 'bananas')

SELECT SUBSTRING('abcdef' FROM 2 FOR 3)

