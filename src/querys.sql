-- 1. ¿Cuál es el Promedio, mínimo, máximo y desviación estándar del limite de las cuentas de los clientes?
SELECT 
    AVG(limit_budget) AS promedio_limite,
    MIN(limit_budget) AS minimo_limite,
    MAX(limit_budget) AS maximo_limite,
    STDEV(limit_budget) AS desviacion_estandar_limite
FROM
    DIM_ACCOUNTS;

-- Cabe resaltar que STDEV no está disponible en SQLITE entonces se hizo el cálculo
-- En la función de python