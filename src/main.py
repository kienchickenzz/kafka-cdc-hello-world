"""
Module quản lý kết nối và cấu hình Debezium CDC connector thông qua Kafka Connect REST API.
"""

from dataclasses import dataclass

import requests


@dataclass
class TableCDCConfig:
    """Cấu hình CDC cho một bảng PostgreSQL.

    Args:
        schema (str): Tên schema của bảng.
        table (str): Tên bảng.
        topic_prefix (str): Prefix cho Kafka topic nhận CDC events.
    """

    schema: str
    table: str
    topic_prefix: str


@dataclass
class DatabaseConfig:
    """Cấu hình kết nối PostgreSQL.

    Args:
        host (str): Hostname của database server.
        port (int): Port của database server.
        user (str): Username để kết nối.
        password (str): Password để kết nối.
        dbname (str): Tên database.
    """

    host: str
    port: int
    user: str
    password: str
    dbname: str


class DebeziumManager:
    """Quản lý Debezium connector thông qua Kafka Connect REST API.

    Args:
        connect_url (str): URL của Kafka Connect REST API.
    """

    def __init__(self, connect_url: str):
        self.url = connect_url.rstrip("/")

    def create_postgres_connector(
        self,
        name: str,
        db_config: DatabaseConfig,
        tables: list[TableCDCConfig],
        slot_name: str | None = None,
        signal_table: str = "public.debezium_signal",
    ) -> dict:
        """Tạo một PostgreSQL CDC connector mới trên Kafka Connect.

        Args:
            name (str): Tên connector.
            db_config (DatabaseConfig): Cấu hình kết nối PostgreSQL.
            tables (list[TableCDCConfig]): Danh sách bảng cần theo dõi CDC.
            slot_name (str | None): Tên replication slot. Mặc định tự sinh từ name.
            signal_table (str): Tên bảng signal cho controlled snapshots.

        Returns:
            dict: Response JSON từ Kafka Connect API.
        """
        table_list = ",".join(f"{t.schema}.{t.table}" for t in tables)

        config = {
            # Cấu hình chung
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "tasks.max": "1",
            # Cấu hình kết nối DB
            "database.hostname": db_config.host,
            "database.port": str(db_config.port),
            "database.user": db_config.user,
            "database.password": db_config.password,
            "database.dbname": db_config.dbname,
            # Cấu hình CDC
            "table.include.list": table_list,
            "topic.prefix": tables[0].topic_prefix,
            "plugin.name": "pgoutput",
            "slot.name": slot_name or f"dbz_{name.replace('-', '_')}_slot",
            "publication.name": f"dbz_pub_{name.replace('-', '_')}",
            "signal.data.collection": signal_table,
            "signal.enabled.channels": "source",
        }

        try:
            response = requests.post(
                f"{self.url}/connectors",
                json={"name": name, "config": config},
            )
            return response.json()
        except Exception as e:
            raise RuntimeError(
                f"Failed to create connector {name}: {e}"
            ) from e

    def update_tables(
        self,
        connector_name: str,
        tables: list[TableCDCConfig],
    ) -> dict:
        """
        Hot-reload danh sách bảng CDC cho một connector đang chạy.

        Args:
            connector_name (str): Tên connector cần cập nhật.
            tables (list[TableCDCConfig]): Danh sách bảng mới.

        Returns:
            dict: Response JSON từ Kafka Connect API.
        """
        try:
            response = requests.get(
                f"{self.url}/connectors/{connector_name}/config"
            )
            config = response.json()

        except Exception as e:
            raise RuntimeError(
                f"Failed to get connector config {connector_name}: {e}"
            ) from e
        
        config["table.include.list"] = ",".join(
            f"{t.schema}.{t.table}" for t in tables
        )

        try:
            response = requests.put(
                f"{self.url}/connectors/{connector_name}/config",
                json=config,
            )
            return response.json()

        except Exception as e:
            raise RuntimeError(
                f"Failed to update connector {connector_name}: {e}"
            ) from e

    def list_connectors(self) -> list[str]:
        """Liệt kê tất cả connector đang có trên Kafka Connect.

        Returns:
            list[str]: Danh sách tên các connector.
        """
        try:
            response = requests.get(f"{self.url}/connectors")
            return response.json()
        except Exception as e:
            raise RuntimeError(
                f"Failed to list connectors: {e}"
            ) from e

    def get_connector_status(self, connector_name: str) -> dict:
        """Kiểm tra trạng thái của một connector và các task của nó.

        Args:
            connector_name (str): Tên connector cần kiểm tra.

        Returns:
            dict: Trạng thái connector bao gồm state và danh sách tasks.
        """
        try:
            response = requests.get(
                f"{self.url}/connectors/{connector_name}/status"
            )
            return response.json()
        except Exception as e:
            raise RuntimeError(
                f"Failed to get status for connector {connector_name}: {e}"
            ) from e

    def delete_connector(self, connector_name: str) -> None:
        """Xóa một connector khỏi Kafka Connect.

        Args:
            connector_name (str): Tên connector cần xóa.
        """
        try:
            response = requests.delete(
                f"{self.url}/connectors/{connector_name}"
            )
            response.raise_for_status()
        except Exception as e:
            raise RuntimeError(
                f"Failed to delete connector {connector_name}: {e}"
            ) from e

    def restart_connector(
        self,
        connector_name: str,
        include_tasks: bool = False,
    ) -> None:
        """Restart một connector trên Kafka Connect.

        Args:
            connector_name (str): Tên connector cần restart.
            include_tasks (bool): Restart luôn cả các task của connector.
        """
        try:
            params = {"includeTasks": str(include_tasks).lower()}
            response = requests.post(
                f"{self.url}/connectors/{connector_name}/restart",
                params=params,
            )
            response.raise_for_status()
        except Exception as e:
            raise RuntimeError(
                f"Failed to restart connector {connector_name}: {e}"
            ) from e
