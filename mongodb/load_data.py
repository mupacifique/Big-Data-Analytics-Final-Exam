"""Simple MongoDB Data Loader

Loads generated JSON files into MongoDB and converts ISO timestamps
to BSON datetimes where appropriate (transactions timestamps).
"""
import json
import os
from datetime import datetime
from pymongo import MongoClient


def _find_first(existing_paths):
    for p in existing_paths:
        if os.path.exists(p):
            return p
    return None


def _to_datetime(s):
    try:
        return datetime.fromisoformat(s)
    except Exception:
        return None


def load_data():
    """Load the generated JSON files into MongoDB"""
    client = MongoClient('mongodb://localhost:27017/')
    db = client.ecommerce_project

    # Clear existing data
    db.users.drop()
    db.products.drop()
    db.transactions.drop()

    # Resolve paths relative to repository root (parent of this mongodb folder)
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    candidates_users = [
        os.path.join(repo_root, 'data', 'raw', 'users.json'),
        os.path.join(repo_root, 'users.json'),
        os.path.join(os.getcwd(), 'data', 'raw', 'users.json'),
        os.path.join(os.getcwd(), 'users.json'),
        'users.json'
    ]
    candidates_products = [
        os.path.join(repo_root, 'data', 'raw', 'products.json'),
        os.path.join(repo_root, 'products.json'),
        os.path.join(os.getcwd(), 'data', 'raw', 'products.json'),
        os.path.join(os.getcwd(), 'products.json'),
        'products.json'
    ]
    candidates_transactions = [
        os.path.join(repo_root, 'data', 'raw', 'transactions.json'),
        os.path.join(repo_root, 'transactions.json'),
        os.path.join(os.getcwd(), 'data', 'raw', 'transactions.json'),
        os.path.join(os.getcwd(), 'transactions.json'),
        'transactions.json'
    ]

    users_path = _find_first(candidates_users)
    products_path = _find_first(candidates_products)
    transactions_path = _find_first(candidates_transactions)

    if not users_path or not products_path or not transactions_path:
        raise FileNotFoundError('One or more data files not found. Checked users/products/transactions in data/raw, parent and cwd.')

    # Load users
    with open(users_path, 'r', encoding='utf-8') as f:
        users = json.load(f)
        # Optionally convert registration/last_active to datetimes if present
        for u in users:
            if 'registration_date' in u and isinstance(u['registration_date'], str):
                dt = _to_datetime(u['registration_date'])
                if dt:
                    u['registration_date'] = dt
            if 'last_active' in u and isinstance(u['last_active'], str):
                dt = _to_datetime(u['last_active'])
                if dt:
                    u['last_active'] = dt
        if users:
            db.users.insert_many(users)

    # Load products
    with open(products_path, 'r', encoding='utf-8') as f:
        products = json.load(f)
        for p in products:
            if 'creation_date' in p and isinstance(p['creation_date'], str):
                dt = _to_datetime(p['creation_date'])
                if dt:
                    p['creation_date'] = dt
        if products:
            db.products.insert_many(products)

    # Load transactions (convert timestamp strings to datetimes)
    with open(transactions_path, 'r', encoding='utf-8') as f:
        transactions = json.load(f)
        for t in transactions:
            if 'timestamp' in t and isinstance(t['timestamp'], str):
                dt = _to_datetime(t['timestamp'])
                if dt:
                    t['timestamp'] = dt
        if transactions:
            db.transactions.insert_many(transactions)

    print(f"Loaded: {db.users.count_documents({})} users")
    print(f"Loaded: {db.products.count_documents({})} products")
    print(f"Loaded: {db.transactions.count_documents({})} transactions")

    return db


if __name__ == "__main__":
    load_data()
