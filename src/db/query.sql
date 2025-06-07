-- 1. ¿Cuál es el Promedio, mínimo, máximo y desviación estándar del limite de las cuentas de los clientes?
-- SELECT 
--     AVG(limit_budget) AS promedio_limite,
--     MIN(limit_budget) AS minimo_limite,
--     MAX(limit_budget) AS maximo_limite,
--     ROUND(SQRT(AVG(limit_budget * limit_budget) - AVG(limit_budget) * AVG(limit_budget)), 2) AS desviacion_estandar
-- FROM
--     DIM_ACCOUNTS;
-- Cabe resaltar que STDEV no está disponible en SQLITE entonces se hizo el cálculo
-- En la función de python


-- 2. ¿Cuántos clientes tienen más de una cuenta?

SELECT
    COUNT(DISTINCT customer_id) AS clientes_con_mas_de_una_cuenta
FROM(
    SELECT
        customer_id, -- ID del cliente
        COUNT(ID_ACCOUNT_UNIQUE) AS cuenta_unica -- Contar las cuentas únicas por cliente
    FROM
    DIM_ACCOUNTS
    GROUP BY
        customer_id
    HAVING
        COUNT(ID_ACCOUNT_UNIQUE) > 1
) AS subquery;

-- 3. ¿Cuál es el monto promedio y el número de transacciones del mes de junio?
SELECT
    ROUND(AVG(amount), 2) AS promedio_monto_junio,
    COUNT(ID_TRANSACTION) AS numero_transacciones_junio
FROM
    FACT_TRANSACTIONS
JOIN
    DIM_DATES ON date_id = DIM_DATES.ID_DATE
WHERE
    DIM_DATES.month = 6;

-- 4. ¿Cuál es el id de cuenta con la mayor diferencia entre sus transacciones más altas y más bajas?
SELECT
    DIM_ACCOUNTS.id_account
FROM
    FACT_TRANSACTIONS
JOIN
    DIM_ACCOUNTS ON FACT_TRANSACTIONS.account_id = DIM_ACCOUNTS.ID_ACCOUNT_UNIQUE
GROUP BY
    DIM_ACCOUNTS.id_account
ORDER BY
    MAX(FACT_TRANSACTIONS.amount) - MIN(FACT_TRANSACTIONS.amount) DESC
LIMIT 1;

-- 5 . ¿Cuántas cuentas tienen exactamente 3 productos y al menos uno de ellos es 'Commodity'?
SELECT
    COUNT(DISTINCT DIM_ACCOUNTS.ID_ACCOUNT_UNIQUE) AS cuentas_con_tres_productos_y_commodity
FROM
    DIM_ACCOUNTS
WHERE
    JSON_ARRAY_LENGTH(DIM_ACCOUNTS.products) = 3 -- Verificar que tenga exactamente 3 productos
    AND (
        JSON_EXTRACT(DIM_ACCOUNTS.products, '$[0]') = 'Commodity'
        OR JSON_EXTRACT(DIM_ACCOUNTS.products, '$[1]') = 'Commodity'
        OR JSON_EXTRACT(DIM_ACCOUNTS.products, '$[2]') = 'Commodity'
    );

-- 6. ¿Cuál es el nombre del cliente que, en total entre todas sus cuentas, ha realizado la mayor
-- cantidad de transacciones del tipo sell?
SELECT
    DIM_CUSTOMERS.name_customer
FROM
    DIM_CUSTOMERS
JOIN
    FACT_TRANSACTIONS ON DIM_CUSTOMERS.ID_CUSTOMER = FACT_TRANSACTIONS.customer_id
JOIN
    DIM_TYPE_TRANSACTIONS  ON FACT_TRANSACTIONS.type_transaction_id = DIM_TYPE_TRANSACTIONS.ID_TYPE_TRANSACTION
WHERE
    DIM_TYPE_TRANSACTIONS.name_type_transacion = 'sell'
GROUP BY
    DIM_CUSTOMERS.ID_CUSTOMER, DIM_CUSTOMERS.name_customer 
ORDER BY
    COUNT(*) DESC 
LIMIT 1; 

-- 7. ¿Cuál es el usuario del cliente cuya cuenta tiene entr 10 y 20 transacciones de tipo 'buy', y que presenta 
-- el promedio de inverisón más alto por operación de este tipo?
SELECT
    DIM_CUSTOMERS.username
FROM
    DIM_CUSTOMERS 
JOIN
    FACT_TRANSACTIONS ON DIM_CUSTOMERS.ID_CUSTOMER = FACT_TRANSACTIONS.customer_id
JOIN
    DIM_TYPE_TRANSACTIONS ON FACT_TRANSACTIONS.type_transaction_id = DIM_TYPE_TRANSACTIONS.ID_TYPE_TRANSACTION
WHERE
    DIM_TYPE_TRANSACTIONS.name_type_transacion = 'buy'
GROUP BY
    DIM_CUSTOMERS.ID_CUSTOMER, DIM_CUSTOMERS.username
HAVING
    COUNT(*) BETWEEN 10 AND 20 
ORDER BY
    AVG(COALESCE(FACT_TRANSACTIONS.total, FACT_TRANSACTIONS.amount)) DESC 
LIMIT 1; 

-- 8. ¿Cuál es el promedio de transacciones de compra (buy) y venta (sell) por accion (symbol)?
    SELECT
        DIM_SYMBOL.name_symbol,
        ROUND(AVG(CASE WHEN DIM_TYPE_TRANSACTIONS.name_type_transacion = 'buy' THEN FACT_TRANSACTIONS.amount ELSE NULL END), 2) AS promedio_compra,
        ROUND(AVG(CASE WHEN DIM_TYPE_TRANSACTIONS.name_type_transacion = 'sell' THEN FACT_TRANSACTIONS.amount ELSE NULL END), 2) AS promedio_venta
    FROM
        FACT_TRANSACTIONS
    JOIN
        DIM_SYMBOL ON FACT_TRANSACTIONS.symbol_id = DIM_SYMBOL.ID_SYMBOL
    JOIN
        DIM_TYPE_TRANSACTIONS ON FACT_TRANSACTIONS.type_transaction_id = DIM_TYPE_TRANSACTIONS.ID_TYPE_TRANSACTION
    GROUP BY
        DIM_SYMBOL.ID_SYMBOL
    ORDER BY
        DIM_SYMBOL.name_symbol;

-- 9. ¿Cuáles son los diferentes beneficios que tienen los clientes del tier 'Gold'?
SELECT DISTINCT
    json_each.value AS beneficios_gold
FROM
    DIM_CUSTOMERS,
    json_each(DIM_CUSTOMERS.benefits)
WHERE
    tier = 'Gold';
    
-- 10. Obtener la cantidad de clientes por rangos etarios ([10–19], [20–29], etc.), que hayan realizado al menos una compra de
-- acciones de “amzn”. La edad debe calcularse como la diferencia entre la fecha de corte 2025-05-16 y el campo “birthdate”.
SELECT
    CASE
        WHEN CAST(strftime('%Y.%m%d', '2025-05-16') - strftime('%Y.%m%d', DIM_CUSTOMERS.birthdate) AS INTEGER) BETWEEN 10 AND 19 THEN '[10-19]'
        WHEN CAST(strftime('%Y.%m%d', '2025-05-16') - strftime('%Y.%m%d', DIM_CUSTOMERS.birthdate) AS INTEGER) BETWEEN 20 AND 29 THEN '[20-29]'
        WHEN CAST(strftime('%Y.%m%d', '2025-05-16') - strftime('%Y.%m%d', DIM_CUSTOMERS.birthdate) AS INTEGER) BETWEEN 30 AND 39 THEN '[30-39]'
        WHEN CAST(strftime('%Y.%m%d', '2025-05-16') - strftime('%Y.%m%d', DIM_CUSTOMERS.birthdate) AS INTEGER) BETWEEN 40 AND 49 THEN '[40-49]'
        WHEN CAST(strftime('%Y.%m%d', '2025-05-16') - strftime('%Y.%m%d', DIM_CUSTOMERS.birthdate) AS INTEGER) BETWEEN 50 AND 59 THEN '[50-59]'
        WHEN CAST(strftime('%Y.%m%d', '2025-05-16') - strftime('%Y.%m%d', DIM_CUSTOMERS.birthdate) AS INTEGER) BETWEEN 60 AND 69 THEN '[60-69]'
        WHEN CAST(strftime('%Y.%m%d', '2025-05-16') - strftime('%Y.%m%d', DIM_CUSTOMERS.birthdate) AS INTEGER) BETWEEN 70 AND 79 THEN '[70-79]'
        WHEN CAST(strftime('%Y.%m%d', '2025-05-16') - strftime('%Y.%m%d', DIM_CUSTOMERS.birthdate) AS INTEGER) >= 80 THEN '[80+]'
        ELSE 'UNKNOWN'
    END AS rango_etario,
    COUNT(DISTINCT DIM_CUSTOMERS.ID_CUSTOMER) AS cantidad_clientes
FROM
    DIM_CUSTOMERS
JOIN
    FACT_TRANSACTIONS ON DIM_CUSTOMERS.ID_CUSTOMER = FACT_TRANSACTIONS.customer_id
JOIN
    DIM_SYMBOL ON FACT_TRANSACTIONS.symbol_id = DIM_SYMBOL.ID_SYMBOL
JOIN 
    DIM_TYPE_TRANSACTIONS ON FACT_TRANSACTIONS.type_transaction_id = DIM_TYPE_TRANSACTIONS.ID_TYPE_TRANSACTION
WHERE
    DIM_SYMBOL.name_symbol = 'amzn'
    AND DIM_TYPE_TRANSACTIONS.name_type_transacion = 'buy'
GROUP BY
    rango_etario
ORDER BY
    rango_etario;
