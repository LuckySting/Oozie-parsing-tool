import sqlite3
from typing import Set, List, Tuple, Union


class Table:
    def __init__(self, index: int, name: str, meaning: str, authors: str, external: bool):
        self.index: int = index
        self.name: str = name
        self.meaning: str = meaning
        self.authors: str = authors
        self.external: bool = external

    def __str__(self):
        return f'{self.index}. {self.name}'

    def __repr__(self):
        return f'Table({str(self)})'


class Workflow:
    def __init__(self, index: int, name: str):
        self.index: int = index
        self.name: str = name

    def __str__(self):
        return self.name

    def __repr__(self):
        return str(self.name)


class TableDependencies:
    def __init__(self):
        self.forward_dependencies: Set[Set[int, int]] = {}
        self.backward_dependencies: Set[Set[int, int]] = {}


class Store:
    def __init__(self, db_name: str):
        self.connection: sqlite3.Connection = sqlite3.connect(db_name)
        self.create_db_tables()

    def create_db_tables(self, force: bool = False):
        cursor = self.connection.cursor()
        if force:
            cursor.execute('DROP TABLE IF EXISTS TABLES;')
            cursor.execute('DROP TABLE IF EXISTS WORKFLOWS;')
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS TABLES
            (
                ID INTEGER PRIMARY KEY AUTOINCREMENT,
                NAME TEXT NOT NULL UNIQUE,
                MEANING TEXT NOT NULL DEFAULT '',
                AUTHORS TEXT NOT NULL DEFAULT '',
                EXTERNAL BOOLEAN NOT NULL DEFAULT FALSE
            );
        """)
        cursor.execute("""
                    CREATE TABLE IF NOT EXISTS WORKFLOWS
                    (
                        ID INTEGER PRIMARY KEY AUTOINCREMENT,
                        NAME TEXT NOT NULL UNIQUE
                    );
                """)
        cursor.close()

    def get_tables(self, search_text: str = '', only_names: bool = False) -> List[Union[str, Table]]:
        cursor: sqlite3.Cursor = self.connection.cursor()
        tables: List[Tuple] = cursor.execute(
            'SELECT ID, NAME, MEANING, AUTHORS, EXTERNAL FROM TABLES WHERE instr(NAME, ?) > 0;', (search_text,)).fetchall()
        cursor.close()
        if only_names:
            return [d[1] for d in tables]
        return [Table(*d) for d in tables]

    def get_tables_by_names(self, table_names: List[str]) -> List[Table]:
        cursor: sqlite3.Cursor = self.connection.cursor()
        sql: str = 'SELECT ID, NAME FROM WORKFLOWS WHERE 1=0'
        for name in table_names:
            sql += f' OR NAME = "{name}"'
        tables: List[Tuple] = cursor.execute(sql).fetchall()
        cursor.close()
        return [Table(*d) for d in tables]

    def get_workflows_by_names(self, workflow_names: List[str]) -> List[Workflow]:
        cursor: sqlite3.Cursor = self.connection.cursor()
        sql: str = 'SELECT ID, NAME FROM WORKFLOWS WHERE 1=0'
        for name in workflow_names:
            sql += f' OR NAME = "{name}"'
        workflows: List[Tuple] = cursor.execute(sql).fetchall()
        cursor.close()
        return [Workflow(*d) for d in workflows]

    def get_workflows(self, search_text: str = '', only_names: bool = False) -> List[Union[str, Workflow]]:
        cursor: sqlite3.Cursor = self.connection.cursor()
        workflows: List[Tuple] = cursor.execute(
            'SELECT ID, NAME FROM WORKFLOWS WHERE instr(NAME, ?) > 0;', (search_text,)).fetchall()
        cursor.close()
        if only_names:
            return [d[1] for d in workflows]
        return [Workflow(*d) for d in workflows]

    def insert_tables(self, table_names: List[str]):
        cursor: sqlite3.Cursor = self.connection.cursor()
        cursor.executemany("""
                            INSERT OR IGNORE INTO TABLES(NAME, MEANING, AUTHORS, EXTERNAL) VALUES(?, ?, ?, ?)
                           """, [(n, '', '', False) for n in table_names])
        self.connection.commit()
        cursor.close()

    def insert_workflows(self, workflow_names: List[str]):
        cursor: sqlite3.Cursor = self.connection.cursor()
        cursor.executemany("""
                            INSERT OR IGNORE INTO WORKFLOWS(NAME) VALUES(?)
                           """, [(n,) for n in workflow_names])
        self.connection.commit()
        cursor.close()
