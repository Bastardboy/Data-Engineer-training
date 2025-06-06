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