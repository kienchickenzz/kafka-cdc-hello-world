"""Module xử lý tạo bảng và insert dữ liệu vào PostgreSQL để trigger CDC events."""

import random

import psycopg2

from src.main import DatabaseConfig


_MOCK_PRODUCTS = ["Laptop", "Phone", "Tablet", "Keyboard", "Mouse", "Monitor", "Headset", "Webcam"]


def create_orders_table(db_config: DatabaseConfig) -> None:
    """Tạo bảng orders trong PostgreSQL nếu chưa tồn tại.

    Args:
        db_config (DatabaseConfig): Cấu hình kết nối PostgreSQL.
    """
    try:
        conn = psycopg2.connect(
            host=db_config.host,
            port=db_config.port,
            user=db_config.user,
            password=db_config.password,
            dbname=db_config.dbname,
        )
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS public.orders (
                id SERIAL PRIMARY KEY,
                product VARCHAR(255) NOT NULL,
                quantity INT NOT NULL,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)
        print("Table 'public.orders' created successfully.")
    except Exception as e:
        raise RuntimeError(f"Failed to create orders table: {e}") from e
    finally:
        cursor.close()
        conn.close()


def insert_order(db_config: DatabaseConfig) -> None:
    """Insert một order ngẫu nhiên vào bảng orders.

    Args:
        db_config (DatabaseConfig): Cấu hình kết nối PostgreSQL.
    """
    product = random.choice(_MOCK_PRODUCTS)
    quantity = random.randint(1, 20)

    try:
        conn = psycopg2.connect(
            host=db_config.host,
            port=db_config.port,
            user=db_config.user,
            password=db_config.password,
            dbname=db_config.dbname,
        )
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO public.orders (product, quantity) VALUES (%s, %s) RETURNING id, product, quantity",
            (product, quantity),
        )
        row = cursor.fetchone()
        print(f"Inserted order: id={row[0]}, product={row[1]}, quantity={row[2]}")
    except Exception as e:
        raise RuntimeError(f"Failed to insert order: {e}") from e
    finally:
        cursor.close()
        conn.close()
