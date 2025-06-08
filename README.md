# Bsale Technical Test
Prueba técnica para postular al cargo Ingeniero de Datos Jr.
## Descripción
A continuación se presenta mi resolución para el desafío técnico de Bsale.
El objetivo es crear un proceso ETL que permita limpiar, transformar y cargar datos desde los datos formato JSON a una base de datos SQLite. El proceso de trabajo consta de:
- **Planteamiento y diseño del Data Warehouse**
- **Creación de la base de datos SQLite**
- **Generación de los procesos ETL**
    - Extracción de los datos mediante la lectura de la data
    - la limpieza y transformación de los datos para adaptarlos al modelo del Data Warehouse
    - la carga de los datos en la base de datos SQLite
- **Ejecución de las consultas SQL**

## ❗ Importante

Se trabajó en ingles para demostrar disponibilidad para trabajar en grupo multicultural.

## 🛠️ Tecnologías Utilizadas

- **SQLite** - Base de datos relacional para almacenar los datos transformados.
- **Python** - Lenguaje de programación utilizado para el desarrollo del proceso ETL.
- **Pandas** - Inicialmente se consideró para la manipulación de datos, debido a sus cualidades, mas se optó por JSON debido a las estrcturas anidadas que poseían los datos.
- **JSON** - Librería para la lectura y manipulación de datos en formato JSON.
- **Datetime** - Librería para el manejo de las fechas, teniendo en cuenta el formato que se encuentra en los archivos JSON.

## 🚀 Instrucciones de Ejecución Local

1. **Clonar repositorio**
```bash
git clone https://github.com/Bastardboy/Bsale-Technical-Test.git
cd Bsale-Technical-Test
```

2. **Moverse hacia la carpeta de trabajo**
```bash
cd src
```

3. **Ejecutar el script ETL**
```bash
python etl.py
```


## 📂 Estructura del Proyecto
```bash
Bsale-Technical-Test/
├── src/
│   │── db/
│   │   ├── script.sql
│   │   ├── query.sql
│   │   ├── dw_financial.db 
│   ├── sample_analytics_dataset/
│   │   ├── sample_analytics.accounts.json
│   │   ├── sample_analytics.customers.json
│   │   ├── sample_analytics.transactions.json
│   │── etl.log
│   │── etl.py
│   │── check_data.py
├── Explicación funciones.pdf
├── .gitignore
└── README.md
```

## Resolución de Preguntas
A continuación se presentarán los resultados obtenidos tras ejecutar el archivo query.sql

### ❗ Importante
Para la pregunta 1, como no se podía hacer uso de STDEV, ni la función SQRT dentro del SQLite, se realizó el cálculo de forma manual dentro del archivo utils/execute_query.py; mas se deja la consulta SQL igualmente para su referencia.
También se encuentra en el archivo query.sql

### 1. ¿Cuál es el promedio, mínimo, máximo y desviación estándar del límite de las cuentas de usuarios?
```sql
SELECT 
    AVG(limit_budget) AS promedio_limite,
    MIN(limit_budget) AS minimo_limite,
    MAX(limit_budget) AS maximo_limite,
    ROUND(SQRT(AVG(limit_budget * limit_budget) - AVG(limit_budget) * AVG(limit_budget)), 2) AS desviacion_estandar
FROM
    DIM_ACCOUNTS;
```
## 📊 Resultados

### 1. ¿Cuál es el promedio, mínimo, máximo y desviación estándar del límite de las cuentas de usuarios?

| Promedio límite | Mínimo límite | Máximo límite | Desviación estándar |
|:---------------:|:-------------:|:-------------:|:-------------------:|
| $9,955.90       | $3,000.00     | $10,000.00    | $354.75             |

---
Teniendo en cuenta el promedio de los límites salariales, observamos que la mayoría de los clientes están cerca del máximo, con una distribución sesgada. Por otro lado, la desviación estándar al ser baja, informa que los límites de las cuentas no varían mucho entre sí, lo que sugiere una política de crédito bastante uniforme. 

### 2. ¿Cuántos clientes poseen más de una cuenta?

| Clientes con más de una cuenta |
|:------------------------------:|
| 417                            |

---

Dentro de los 500 clientes que están registrados, un 83.4% de ellos poseen más de una cuenta, lo que nos lleva a pensar que tenemos clientes fieles y con activos financieros significativos. Mientras que el resto, 16.6% de los clientes, se nos presenta como una oportunidad para plantear estrategias de crecimiento.

### 3. ¿Cuál es el monto promedio y el número de transacciones del mes de junio?

| Promedio monto junio | Número de transacciones junio |
|:--------------------:|:----------------------------:|
| $5,003.42            | 7,520                        |

---

El mes de junio presenta un gran volumen de transacciones, correspondientes al 8.53% del total de transacciones analizadas.
Si bien su monto y número de transacciones son promedios, con respecto al resto de los meses, sugiere que junio es un mes activo para los clientes.

### 4. ¿Cuál es el id de cuenta con la mayor diferencia entre su transacción más alta y más baja?

| ID de cuenta |
|:------------:|
| 909802          
---

La cuenta con ID 909802 tiene una transacción con la mayor diferencia, esto puede indicar muchas cosas desde un comportamiento volatil del cliente, hasta una estrategia de inversión agresiva. Podría ser un foco de investiación para entder mejor esta transacción.

### 5. ¿Cuántas cuentas tienen exactamente 3 productos y, además, uno de esos productos es "Commodity"?

| Cuentas con 3 productos y "Commodity" |
|:-------------------------------------:|
| 202                                   |

---
El 40.4% de las cuentas que tienen 3 productos, poseen un producto del tipo "Commodity", lo que sugiere un producto bastante atractivo, dentro de la oferta que existe. Este grupo presenta potencial para aprovechar estrategias de marketing y ventas, enfocadas en este tipo de productos.

### 6. ¿Cuál es el nombre del cliente que, en total entre todas sus cuentas, ha realizado la mayor cantidad de transacciones de tipo sell?

| Nombre del cliente |
|:------------------:|
| John Williams      |
---

Podemos observar que el cliente "John Williams" es un usuario muy activo en este tipo de operaciones. Esto permite tenerlo como un cliente catalogado como "vendedor principal de acciones", teniendolo en cuenta como de alto valor y que maneja la liquidez del mercado.

### 7. ¿Cuál es el usuario del cliente cuya cuenta tiene entre 10 y 20 transacciones de tipo “buy”, y que presenta el promedio de inversión más alto por operación de este tipo?

| Usuario |
|:-------:|
| aspencer|
---

Ahora se sugiere que el cliente tiene un perfil de alto inversor, teniendo en cuenta un rango de transacciones que podrían tomarse como moderdas, con esto se podría identificar un cliente con fidelidad, debido a que tiene una cuenta de banco que le permite sus operaciones.

### 8. ¿Cuál es el promedio de transacciones de compra y de venta por acción (campo “symbol”)?

 | Promedio Compra | Promedio Venta |
|:---------------:|:--------------:|
| $4,986.17       | $4,996.42      |
---

Si bien el promedio de ventas es un poco mayor al de compra, podríamos establecer que es rentable el comportamiento, pero se mantiene conservador

### 9. ¿Cuáles son los diferentes beneficios que tienen los clientes del tier “Gold”?

- 24 hour dedicated line
- airline lounge access
- car rental insurance
- concert tickets
- concierge services
- dedicated account representative
- financial planning assistance
- shopping discounts
- sports tickets
- travel insurance
---

Podemos ver que al tener activado el estado de "Gold" en la cuenta el cliente tendrá multiples tipos beneficios, los cuales pueden ser usados para campañas de marketing, retención o promover al resto que hagan uso de esta suscripción.

### 10. Obtener la cantidad de clientes por rangos etarios ([10–19], [20–29], etc.), que hayan realizado al menos una compra de acciones de “amzn”.

| Rango etario | Cantidad de clientes |
|:------------:|:-------------------:|
| [20-29]      | 11                  |
| [30-39]      | 55                  |
| [40-49]      | 45                  |
| [50-59]      | 40                  |
----

Las acciones de "amzn" son el interes para los clientes entre 30 y 59 años, que a su vez se les puede catalogar como el grupo con mayor estabilidad económica del grupo estudiado. Esto podría verse reflejado viendo el rango anterior, donde los adultos jóvenes, al estar en sus etapas formativas, poseen menos capital, aumentando las barreras de entrada

## Desarrollado por David Pazán 
