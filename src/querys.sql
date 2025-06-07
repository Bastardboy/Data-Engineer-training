-- 1. ¿Cuál es el Promedio, mínimo, máximo y desviación estándar del limite de las cuentas de los clientes?
SELECT 
    AVG(limit_budget) AS promedio_limite,
    MIN(limit_budget) AS minimo_limite,
    MAX(limit_budget) AS maximo_limite,
    ROUND(SQRT(AVG(limit_budget * limit_budget) - AVG(limit_budget) * AVG(limit_budget)), 2) AS desviacion_estandar
FROM
    DIM_ACCOUNTS;

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
    FACT_TRANSACTIONS.account_id,
    MAX(FACT_TRANSACTIONS.amount) - MIN(FACT_TRANSACTIONS.amount) AS diferencia_maxima
FROM
    FACT_TRANSACTIONS
GROUP BY
    FACT_TRANSACTIONS.account_id
ORDER BY
    diferencia_maxima DESC
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

-- 6. ¿Cuál es el nombre del cliente que, en total entre todas sus cuentas, ha realizado la mayor cantidad de transacciones del tipo sell?

-- 7. ¿Cuál es el usuario del cliente cuya cuenta tiene entr 10 y 20 transacciones de tipo 'buy', y que presenta el promedio de inverisón más alto por operación de este tipo?


-- 8. ¿Cuál es el promedio de transacciones de compra (buy) y venta (sell) por accion (symbol)?


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
