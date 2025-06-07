import os
import json
from datetime import datetime, timezone
from collections import defaultdict


with open('sample_analytics_dataset/sample_analytics.customers.json', encoding='utf-8') as f:
    data = json.load(f)
    username_counts = {}
    gold_benefits = []
    usuarios_con_mas_de_una_cuenta = 0

    for customer in data:
        tier_details = customer.get('tier_and_details', {})
        for tier in tier_details.values():
            if tier.get('tier') == 'Gold':
                gold_benefits.extend(tier.get('benefits', []))

    # Eliminar duplicados si lo deseas
    gold_benefits = list(set(gold_benefits))
    # obtener una cuenta de los beneficios

    print("Beneficios Gold:", len(gold_benefits)) # 10

    for usuario in data:
        cuentas = usuario.get('accounts', [])
        if isinstance(cuentas, list) and len(cuentas) > 1:
            usuarios_con_mas_de_una_cuenta += 1

    print(f"Usuarios con más de una cuenta: {usuarios_con_mas_de_una_cuenta}") # 417

    for customer in data:
        username = customer.get('username')
        if username:
            username_counts[username] = username_counts.get(username, 0) + 1

    duplicates = [user for user, count in username_counts.items() if count > 1]
    if duplicates:
        print("Usernames repetidos:", duplicates)
    else:
        print("No hay usernames repetidos.")
        #Usernames repetidos: ['mirandajones', 'ihill', 'patrick05']



with open('sample_analytics_dataset/sample_analytics.accounts.json', encoding='utf-8') as f:
    data = json.load(f)
ids = set()
for cuenta in data:
    ids.add(cuenta["account_id"])
print(f"Cantidad de account_id únicos: {len(ids)}") # 1745


with open('sample_analytics_dataset/sample_analytics.transactions.json', 'r') as f:
    data = json.load(f)
    unique_codes = set()

    for type_transaction in data:
        for transaction in type_transaction['transactions']:
            transaction_code = transaction.get('transaction_code')
            if transaction_code:
                unique_codes.add(transaction_code)

print(f"Número de opciones: {len(unique_codes)}") # 2
print("Opciones:", unique_codes) # sell y buy

with open('sample_analytics_dataset/sample_analytics.transactions.json', 'r') as f:
    data = json.load(f)
    total_transaction_count = 0

    for type_transaction in data:
        transaction_count = type_transaction.get('transaction_count')
        total_transaction_count += transaction_count

print(f"Suma total de transaction_count: {total_transaction_count}") #88119

with open('sample_analytics_dataset/sample_analytics.accounts.json', 'r') as f:
    data = json.load(f)
    account_id_counts = {}

    for customer in data:
        acc_id = customer.get('account_id')
        if acc_id:
            account_id_counts[acc_id] = account_id_counts.get(acc_id, 0) + 1

    duplicates = [account for account, count in account_id_counts.items() if count > 1]
    if duplicates:
        print("Account id repetidos:", duplicates)
    else:
        print("No hay accounts repetidos.")
        #accountid repetidos [627788]


with open('sample_analytics_dataset/sample_analytics.accounts.json', encoding='utf-8') as f:
    data = json.load(f)

productos_unicos = set()

for cuenta in data:
    productos = cuenta.get('products', [])
    for producto in productos:
        productos_unicos.add(producto)

print(f"Productos únicos ({len(productos_unicos)}): {sorted(productos_unicos)}")

with open('sample_analytics_dataset/sample_analytics.transactions.json', encoding='utf-8') as f:
    data = json.load(f)

# Diccionarios para contar y sumar cantidades por symbol y tipo de transacción
buy_count = defaultdict(int)
buy_sum = defaultdict(float)
sell_count = defaultdict(int)
sell_sum = defaultdict(float)

for cuenta in data:
    for tx in cuenta.get('transactions', []):
        symbol = tx.get('symbol')
        code = tx.get('transaction_code')
        amount = tx.get('amount') or tx.get('quantity')  # Usa 'amount' o 'quantity' según el campo disponible
        if symbol and code and amount is not None:
            try:
                amount = float(amount)
            except Exception:
                continue
            if code == 'buy':
                buy_count[symbol] += 1
                buy_sum[symbol] += amount
            elif code == 'sell':
                sell_count[symbol] += 1
                sell_sum[symbol] += amount

symbols = set(list(buy_count.keys()) + list(sell_count.keys()))
print("Promedio de transacciones por acción (symbol):")
for symbol in sorted(symbols):
    avg_buy = buy_sum[symbol] / buy_count[symbol] if buy_count[symbol] else 0
    avg_sell = sell_sum[symbol] / sell_count[symbol] if sell_count[symbol] else 0
    print(f"  {symbol}: promedio buy = {avg_buy:.2f}, promedio sell = {avg_sell:.2f}")

# Promedio de transacciones por acción (symbol):
#   aapl: promedio buy = 4960.32, promedio sell = 5007.65
#   adbe: promedio buy = 5034.05, promedio sell = 4959.39
#   amd: promedio buy = 4943.10, promedio sell = 5036.85
#   amzn: promedio buy = 5009.69, promedio sell = 5018.30
#   bb: promedio buy = 5063.12, promedio sell = 4999.00
#   crm: promedio buy = 4959.67, promedio sell = 5047.06
#   csco: promedio buy = 4999.96, promedio sell = 5125.69
#   ebay: promedio buy = 4837.84, promedio sell = 4962.13
#   fb: promedio buy = 4957.80, promedio sell = 4975.34
#   goog: promedio buy = 4942.32, promedio sell = 4886.61
#   ibm: promedio buy = 5086.72, promedio sell = 5032.35
#   intc: promedio buy = 5036.03, promedio sell = 4887.81
#   msft: promedio buy = 5010.68, promedio sell = 4972.42
#   nflx: promedio buy = 4997.15, promedio sell = 5068.55
#   nvda: promedio buy = 4986.40, promedio sell = 4935.66
#   sap: promedio buy = 4912.92, promedio sell = 5056.24
#   team: promedio buy = 5016.18, promedio sell = 5049.71
#   znga: promedio buy = 5052.40, promedio sell = 4952.77