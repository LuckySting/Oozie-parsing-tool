import sqlite3
from typing import List, Tuple, Union, Dict


class Table:
    def __init__(self, index: int, name: str, meaning: str, authors: str, sqooped: bool,
                 created_in_workflows: List[str],
                 updated_in_workflows: List[str], based_on_tables: List[str]):
        self.index: int = index
        self.name: str = name
        self.meaning: str = meaning
        self.authors: str = authors
        self.sqooped: bool = sqooped
        self.created_in_workflows: List[str] = created_in_workflows
        self.updated_in_workflows: List[str] = updated_in_workflows
        self.based_on_tables: List[str] = based_on_tables

    @staticmethod
    def from_dict(data: Dict[str, any]):
        return Table(**data)

    def __str__(self):
        return f'{self.index}. {self.name}'

    def __repr__(self):
        return f'Table({str(self)})'


class Workflow:
    def __init__(self, index: int, name: str, source_tables: List[str], effected_tables: List[str]):
        self.index: int = index
        self.name: str = name
        self.source_tables: List[str] = source_tables
        self.effected_tables: List[str] = effected_tables

    @staticmethod
    def from_dict(data: Dict[str, any]):
        return Table(**data)

    def __str__(self):
        return self.name

    def __repr__(self):
        return str(self.name)


class Store:
    def __init__(self, db_name: str):
        self.connection: sqlite3.Connection = sqlite3.connect(db_name)
        self.create_db_tables()

    def create_db_tables(self, force: bool = False):
        cursor = self.connection.cursor()
        if force:
            cursor.execute('DROP TABLE IF EXISTS TABLES;')
            cursor.execute('DROP TABLE IF EXISTS WORKFLOWS;')
            cursor.execute('DROP TABLE IF EXISTS TABLE_CREATED_IN;')
            cursor.execute('DROP TABLE IF EXISTS TABLE_USED_IN;')
            cursor.execute('DROP TABLE IF EXISTS TABLE_BASED_ON;')
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS TABLES
            (
                ID INTEGER PRIMARY KEY AUTOINCREMENT,
                NAME TEXT NOT NULL UNIQUE,
                MEANING TEXT NOT NULL DEFAULT '',
                AUTHORS TEXT NOT NULL DEFAULT '',
                SQOOPED BOOLEAN NOT NULL DEFAULT FALSE
            );
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS WORKFLOWS
            (
                ID INTEGER PRIMARY KEY AUTOINCREMENT,
                NAME TEXT NOT NULL UNIQUE
            );
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS TABLE_CREATED_IN
            (
                CREATED_TABLE REFERENCES TABLES,
                WORKFLOW REFERENCES WORKFLOWS,
                CONSTRAINT TCI_PK PRIMARY KEY(CREATED_TABLE, WORKFLOW)
            );
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS TABLE_USED_IN
            (
                CREATED_TABLE REFERENCES TABLES,
                WORKFLOW REFERENCES WORKFLOWS,
                CONSTRAINT TUI_PK PRIMARY KEY(CREATED_TABLE, WORKFLOW)
            );
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS TABLE_BASED_ON
            (
                TARGET_TABLE REFERENCES TABLES,
                BASE_TABLE REFERENCES TABLES,
                CONSTRAINT TBO_PK PRIMARY KEY(TARGET_TABLE, BASE_TABLE)
            );
        """)
        cursor.close()

    def get_tables(self, search_text: str = '', only_names: bool = False) -> List[Union[str, Table]]:
        cursor: sqlite3.Cursor = self.connection.cursor()
        tables: List[Tuple] = cursor.execute(
            'SELECT ID, NAME, MEANING, AUTHORS, SQOOPED FROM TABLES WHERE instr(NAME, ?) > 0;',
            (search_text,)).fetchall()
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

    def insert_tables(self, tables: List[Table]):
        cursor: sqlite3.Cursor = self.connection.cursor()
        cursor.executemany("""
                            INSERT OR IGNORE INTO TABLES(ID, NAME, MEANING, AUTHORS, SQOOPED) VALUES(?, ?, ?, ?, ?)
                           """, [(t.index, t.name, t.meaning, t.authors, t.sqooped) for t in tables])
        self.connection.commit()
        cursor.close()

    def insert_workflows(self, workflows: List[Workflow]):
        cursor: sqlite3.Cursor = self.connection.cursor()
        cursor.executemany("""
                            INSERT OR IGNORE INTO WORKFLOWS(ID, NAME) VALUES(?, ?)
                           """, [(w.index, w.name,) for w in workflows])
        self.connection.commit()
        cursor.close()

    def insert_table_created_in(self, created_ins: List[Tuple[int, int]]):
        cursor: sqlite3.Cursor = self.connection.cursor()
        cursor.executemany("""
                                INSERT OR IGNORE INTO TABLE_CREATED_IN(TABLES, WORKFLOWS) VALUES(?, ?)
                            """, created_ins)
        self.connection.commit()
        cursor.close()

    def insert_table_used_in(self, used_ins: List[Tuple[int, int]]):
        cursor: sqlite3.Cursor = self.connection.cursor()
        cursor.executemany("""
                                INSERT OR IGNORE INTO TABLE_USED_IN(TABLES, WORKFLOWS) VALUES(?, ?)
                            """, used_ins)
        self.connection.commit()
        cursor.close()

    def insert_table_based_on(self, based_ons: List[Tuple[int, int]]):
        cursor: sqlite3.Cursor = self.connection.cursor()
        cursor.executemany("""
                                INSERT OR IGNORE INTO TABLE_BASED_ON(TARGET_TABLE, BASE_TABLE) VALUES(?, ?)
                            """, based_ons)
        self.connection.commit()
        cursor.close()
