from abc import ABC, abstractmethod
import pandas as pd
########################
# SQL Query generators
########################
class TableStrategy(ABC):
    @abstractmethod
    def generate_create_query(self) -> str: # pyright: ignore[reportArgumentType]
        pass 


class SalesRawTableStrategy(TableStrategy):
    def __init__(self, tablename: str):
        self.tablename = tablename

    def generate_create_query(self) -> str:
        query = f"""
        CREATE TABLE IF NOT EXISTS {self.tablename}(
            id SERIAL PRIMARY KEY,
            transaction_id TEXT,
            item_id TEXT NOT NULL,
            quantity DOUBLE PRECISION,
            transaction_timestamp TIMESTAMP NOT NULL,
            unit_price DOUBLE PRECISION,
            customer_id TEXT,
            region TEXT
        );
        """
        return query
    

class SQLQueryBuilder:
    def __init__(self, strategy: TableStrategy):
        self._strategy = strategy

    def set_strategy(self, strategy: TableStrategy):
        self._strategy = strategy

    def get_create_query(self) -> str:
        """
        Generates a SQL CREATE TABLE query.

        Returns:
            str: The generated SQL CREATE statement.
        """
        return self._strategy.generate_create_query()