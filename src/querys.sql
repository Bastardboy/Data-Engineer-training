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

