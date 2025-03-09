import sqlite3
import requests
import time

def create_database():
    """Create SQLite database and table if not exists"""
    conn = sqlite3.connect('api_data.db')
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS user_data (
            id INTEGER PRIMARY KEY,
            name TEXT,
            email TEXT,
            username TEXT,
            phone TEXT,
            website TEXT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    conn.commit()
    conn.close()

def extract_data():
    """Extract user data from JSONPlaceholder API"""
    response = requests.get('https://jsonplaceholder.typicode.com/users')
    return response.json()

def transform_data(raw_data):
    """Transform API data to fit database schema"""
    transformed_data = []
    for user in raw_data:
        transformed_user = {
            'name': user.get('name', ''),
            'email': user.get('email', ''),
            'username': user.get('username', ''),
            'phone': user.get('phone', ''),
            'website': user.get('website', '')
        }
        transformed_data.append(transformed_user)
    return transformed_data

def load_data(transformed_data):
    """Load transformed data into SQLite database"""
    conn = sqlite3.connect('api_data.db')
    cursor = conn.cursor()
    
    for user in transformed_data:
        cursor.execute('''
            INSERT OR REPLACE INTO user_data 
            (name, email, username, phone, website) 
            VALUES (?, ?, ?, ?, ?)
        ''', (
            user['name'], 
            user['email'], 
            user['username'], 
            user['phone'], 
            user['website']
        ))
    
    conn.commit()
    conn.close()
    print(f"Loaded {len(transformed_data)} records successfully.")

def etl_pipeline():
    """Complete ETL process"""
    # Create database if not exists
    create_database()
    
    # Extract data from API
    raw_data = extract_data()
    
    # Transform data
    transformed_data = transform_data(raw_data)
    
    # Load data to SQLite
    load_data(transformed_data)

def main():
    """Continuously run ETL pipeline every 2 minutes"""
    while True:
        print("Running ETL pipeline...")
        etl_pipeline()
        
        # Wait for 2 minutes
        print("Waiting 2 minutes before next run...")
        time.sleep(120)

if __name__ == "__main__":
    main()