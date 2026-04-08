"""
Simple PostgreSQL connection test
"""

import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()


def mask_password(password: str) -> str:
    return "*" * len(password) if password else "NOT SET"


def main():
    print("\n🔍 Testing PostgreSQL Connection")
    print("=" * 60)

    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    user = os.getenv("POSTGRES_USER", "dataeng")
    password = os.getenv("POSTGRES_PASSWORD", "dataeng123")
    database = os.getenv("POSTGRES_DB", "ecommerce")

    print(f"Host:     {host}")
    print(f"Port:     {port}")
    print(f"User:     {user}")
    print(f"Password: {mask_password(password)}")
    print(f"Database: {database}\n")

    try:
        print("⏳ Attempting connection...\n")

        conn = psycopg2.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            connect_timeout=5,
        )

        print("✅ CONNECTION SUCCESSFUL!\n")

        cursor = conn.cursor()

        # PostgreSQL version
        cursor.execute("SELECT version();")
        print("📦 PostgreSQL Version:")
        print(cursor.fetchone()[0], "\n")

        # Current user + DB
        cursor.execute("SELECT current_user, current_database();")
        current_user, current_db = cursor.fetchone()
        print(f"👤 Connected as: {current_user}")
        print(f"🗄️ Database:     {current_db}\n")

        # Check schemas
        cursor.execute("""
            SELECT schema_name
            FROM information_schema.schemata
            WHERE schema_name IN ('raw', 'staging', 'analytics')
            ORDER BY schema_name;
        """)

        schemas = [row[0] for row in cursor.fetchall()]
        print(f"📂 Schemas found: {schemas}\n")

        cursor.close()
        conn.close()

        print("=" * 60)
        print("🎉 ALL TESTS PASSED - Database is ready!")
        print("=" * 60)

    except psycopg2.OperationalError as e:
        print("❌ CONNECTION FAILED!\n")
        print(f"Error: {e}\n")

        print("🔧 Troubleshooting:")
        print("1. Ensure PostgreSQL container is running:")
        print("   docker ps")
        print("2. Check port mapping (5432 exposed)")
        print("3. Verify credentials in .env")
        print("4. Try manual login:")
        print("   docker exec -it postgres psql -U dataeng -d ecommerce")

    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: {e}")


if __name__ == "__main__":
    main()