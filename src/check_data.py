import os
import json
from datetime import datetime

with open('sample_analytics_dataset/sample_analytics.customers.json', encoding='utf-8') as f:
    data = json.load(f)

gold_benefits = []

for customer in data:
    tier_details = customer.get('tier_and_details', {})
    for tier in tier_details.values():
        if tier.get('tier') == 'Gold':
            gold_benefits.extend(tier.get('benefits', []))

# Eliminar duplicados si lo deseas
gold_benefits = list(set(gold_benefits))

print("Beneficios Gold:", gold_benefits)



with open('sample_analytics_dataset/sample_analytics.accounts.json', encoding='utf-8') as f:
    data = json.load(f)
ids = set()
for cuenta in data:
    ids.add(cuenta["account_id"])
print(f"Cantidad de account_id únicos: {len(ids)}")


with open('sample_analytics_dataset/sample_analytics.transactions.json', 'r') as f:
    data = json.load(f)
    unique_codes = set()

    for type_transaction in data:
        for transaction in type_transaction['transactions']:
            transaction_code = transaction.get('transaction_code')
            if transaction_code:
                unique_codes.add(transaction_code)

print(f"Número de opciones: {len(unique_codes)}")
print("Opciones:", unique_codes)

with open('sample_analytics_dataset/sample_analytics.customers.json', 'r') as f:
    data = json.load(f)
    username_counts = {}

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


with open('sample_analytics_dataset/sample_analytics.accounts.json', 'r') as f:
    data = json.load(f)
    account_id = {}

    for customer in data:
        account_id = customer.get('account_id')
        if account_id:
            account_id[username] = account_id.get(username, 0) + 1

    duplicates = [account for account, count in account_id.items() if count > 1]
    if duplicates:
        print("Account id repetids:", duplicates)
    else:
        print("No hay accounts repetidos.")
        #accountid repetidos [627788]