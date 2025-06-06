-- Eliminar tablas existentes para una ejecución limpia (útil durante el desarrollo)
DROP TABLE IF EXISTS FACT_TRANSACTIONS;
DROP TABLE IF EXISTS DIM_CUSTOMERS;
DROP TABLE IF EXISTS DIM_ACCOUNTS;
DROP TABLE IF EXISTS DIM_DATES;
DROP TABLE IF EXISTS DIM_TYPE_TRANSACTIONS;
DROP TABLE IF EXISTS DIM_SYMBOL;

-- tabla para clientes
CREATE TABLE DIM_CUSTOMERS (
    ID_CUSTOMER INTEGER PRIMARY KEY AUTOINCREMENT, -- clave subrogada id único del cliente (el pk del id automático)
    name_customer TEXT,
    username TEXT NOT NULL,                -- ya no es la clave natural: Usado para identificar al cliente
    customer_natural_key TEXT UNIQUE NOT NULL, -- la clave natural ahora es username+name (habían username repetidos)
    birthdate DATE,                               -- Fecha de nacimiento
    tier TEXT,                                    -- tipo de cuenta tier_and_details { }
    benefits TEXT,                                -- Beneficios asociados al tier (guardado como JSON string)
    CHECK (username IS NOT '')                    -- Restricción para asegurar que username no esté vacío
);

-- tabla para las cuentas
CREATE TABLE DIM_ACCOUNTS (
    ID_ACCOUNT_UNIQUE INTEGER PRIMARY KEY AUTOINCREMENT, -- clave subrogada def previa (pk)
    id_account INTEGER UNIQUE NOT NULL,           -- clave natural: id de la cuenta que tiene el cliente (ej. 721914)
    customer_id INTEGER NOT NULL,                -- FK a DIM_CUSTOMERS
    limit_budget REAL,                                  -- dinero disponible en la cuenta
    products TEXT,                            -- La lista de productos en la cuenta products['name1',...]
    FOREIGN KEY (customer_id) REFERENCES DIM_CUSTOMERS(ID_CUSTOMER) -- Relación con clientes
);

-- tabla de las fechas (lo veo para resolver las preguntas de mes, birthdate etc)
CREATE TABLE DIM_DATES (
    ID_DATE INTEGER PRIMARY KEY,                -- formato YYYYMMDD para clave (transformar la fecha a entero, ej 19960913)
    full_date DATE UNIQUE NOT NULL,               -- fecha completa en formato DATE (ej. 1996-09-13) 
    day INTEGER NOT NULL,
    month INTEGER NOT NULL,
    year INTEGER NOT NULL
);

-- tabla de tipo transaccion (2 casos)
CREATE TABLE DIM_TYPE_TRANSACTIONS (
    ID_TYPE_TRANSACTION INTEGER PRIMARY KEY AUTOINCREMENT, -- clave subrogada def previa pk (1, o 2 sería para los tipos de transacción)
    name_type_transacion TEXT UNIQUE NOT NULL -- si es 'buy', 'sell',
);

-- tabla de símbolos (campo symbol)
CREATE TABLE DIM_SYMBOL (
    ID_SYMBOL INTEGER PRIMARY KEY AUTOINCREMENT, -- clave subrogada def previa pk (1, ..., n )
    name_symbol TEXT UNIQUE NOT NULL           -- los distintos tipos que hay 'amzn', 'nvda', 'msft', etc. 
);

-- tabla de las transacciones, sería la central que registra cada transacción
CREATE TABLE FACT_TRANSACTIONS (
    ID_TRANSACTION INTEGER PRIMARY KEY AUTOINCREMENT, -- Clave subrogada de la tabla de hechos
    date_id INTEGER NOT NULL,                     -- FK a DIM_DATE
    customer_id INTEGER NOT NULL,                   -- FK a DIM_CUSTOMERS
    account_id INTEGER NOT NULL,                    -- FK a DIM_ACCOUNTS
    type_transaction_id INTEGER NOT NULL,          -- FK a DIM_TYPE_TRANSACTIONS
    symbol_id INTEGER,                             -- FK a DIM_SYMBOL (opcional, puede ser NULL si no aplica)
    
    amount REAL NOT NULL,                            -- Cantidad de acciones/moneda
    price REAL,                                    -- Precio por unidad (puede ser NULL)
    total REAL,                                     -- Monto total de la operación (monto * precio, puede ser NULL)
    transaction_code TEXT NOT NULL,                 -- Código original de la transacción (redundante pero útil para traza)
    transaction_date DATETIME NOT NULL,             -- Fecha y hora completa de la transacción para granularidad
    
    -- Definición de Claves Foráneas
    FOREIGN KEY (date_id) REFERENCES DIM_DATES(ID_DATE),
    FOREIGN KEY (customer_id) REFERENCES DIM_CUSTOMERS(ID_CUSTOMER),
    FOREIGN KEY (account_id) REFERENCES DIM_ACCOUNTS(ID_ACCOUNT_UNIQUE),
    FOREIGN KEY (type_transaction_id) REFERENCES DIM_TYPE_TRANSACTIONS(ID_TYPE_TRANSACTION),
    FOREIGN KEY (symbol_id) REFERENCES DIM_SYMBOL(ID_SYMBOL)
);