"""Entry point của dự án, sử dụng argparse để rẽ nhánh các usecase."""

import argparse
import json

from src.main import DatabaseConfig, DebeziumManager, TableCDCConfig
from src.data_operations import create_orders_table, insert_order
from src.consumer import consume_cdc_events


def main():
    """Parse arguments và dispatch sang usecase tương ứng."""

    db_config = DatabaseConfig(
        host="localhost",
        port=5432,
        user="root",
        password="Pa55w.rd",
        dbname="db",
    )

    parser = argparse.ArgumentParser(description="Debezium CDC Manager CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser(
        "create-connector", help="Tạo connector mock với tham số mặc định"
    )
    subparsers.add_parser("get-connectors", help="Lấy danh sách các connector hiện có")
    subparsers.add_parser("create-table", help="Tạo bảng orders trong database")
    subparsers.add_parser("insert-data", help="Insert dữ liệu mẫu vào bảng orders")
    subparsers.add_parser("run-consumer", help="Chạy consumer lắng nghe CDC events")

    args = parser.parse_args()

    if args.command == "create-connector":
        manager = DebeziumManager(connect_url="http://localhost:8083")
        tables = [
            TableCDCConfig(schema="public", table="orders", topic_prefix="pgserver"),
        ]

        result = manager.create_postgres_connector(
            name="postgres-connector",
            db_config=db_config,
            tables=tables,
        )
        print(json.dumps(result, indent=2))
        
    elif args.command == "get-connectors":
        manager = DebeziumManager(connect_url="http://localhost:8083")
        connectors = manager.list_connectors()
        print("Connectors:", connectors)
    
    elif args.command == "create-table":
        create_orders_table(db_config=db_config)

    elif args.command == "insert-data":
        insert_order(db_config=db_config)

    elif args.command == "run-consumer":
        consume_cdc_events(
            broker="localhost:9092",
            topic="pgserver.public.orders",
        )


if __name__ == "__main__":
    main()
