import sqlite3
from typing import List, Tuple, Union, Dict, Set


class Table:
    def __init__(self, index: int, name: str, meaning: str, authors: str, sqooped: Union[bool, int],
                 created_in_workflows=None,
                 used_in_workflows=None,
                 updated_in_workflows=None, based_on_tables=None, partitions=None):
        if partitions is None:
            partitions = list()
        if updated_in_workflows is None:
            updated_in_workflows = list()
        if based_on_tables is None:
            based_on_tables = list()
        if created_in_workflows is None:
            created_in_workflows = list()
        if used_in_workflows is None:
            used_in_workflows = list()
        self.index: int = index
        self.name: str = name
        self.meaning: str = meaning
        self.authors: str = authors
        self.sqooped: bool = bool(sqooped)
        self.created_in_workflows: List[str] = created_in_workflows
        self.updated_in_workflows: List[str] = updated_in_workflows
        self.used_in_workflows: List[str] = used_in_workflows
        self.based_on_tables: List[str] = based_on_tables
        self.partitions: List[str] = partitions

    @staticmethod
    def from_dict(data: Dict[str, any]):
        return Table(data['index'], data['name'], '', '', data['sqooped'], data['created_in_workflows'],
                     data['updated_in_workflows'], data['based_on_tables'])

    def __str__(self):
        return f'{self.index}. {self.name}'

    def __repr__(self):
        return f'Table({str(self)})'


class Workflow:
    def __init__(self, index: int, name: str, source_tables=None, effected_tables=None, predecessors=None,
                 descendants=None):
        if source_tables is None:
            source_tables = list()
        if effected_tables is None:
            effected_tables = list()
        if predecessors is None:
            predecessors = list()
        if descendants is None:
            descendants = list()
        self.index: int = index
        self.name: str = name
        self.source_tables: List[str] = source_tables
        self.effected_tables: List[str] = effected_tables
        self.predecessors: List[str] = predecessors
        self.descendants: List[str] = descendants

    @staticmethod
    def from_dict(data: Dict[str, any]):
        return Workflow(**data)

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
            cursor.execute('DROP TABLE IF EXISTS TABLE_PARTITIONS;')
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
                USED_TABLE REFERENCES TABLES,
                WORKFLOW REFERENCES WORKFLOWS,
                CONSTRAINT TUI_PK PRIMARY KEY(USED_TABLE, WORKFLOW)
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
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS TABLE_UPDATED_IN
            (
                UPDATED_TABLE REFERENCES TABLES,
                WORKFLOW REFERENCES WORKFLOWS,
                CONSTRAINT TUI_PK PRIMARY KEY(UPDATED_TABLE, WORKFLOW)
            );
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS TABLE_PARTITIONS
            (
                TARGET_TABLE REFERENCES TABLES,
                PARTITION_NAME TEXT NOT NULL,
                CONSTRAINT TP_PK PRIMARY KEY(TARGET_TABLE, PARTITION_NAME)
            );
        """)
        cursor.close()

    def update_table(self, table: Table) -> None:
        cursor: sqlite3.Cursor = self.connection.cursor()
        cursor.execute('UPDATE TABLES SET MEANING=?, AUTHORS=? WHERE ID = ?',
                       (table.meaning, table.authors, table.index))
        self.connection.commit()
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
        sql: str = 'SELECT * FROM TABLES WHERE 1=0'
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

    def populate_workflows_data(self, workflows: List[Workflow]):
        cursor: sqlite3.Cursor = self.connection.cursor()
        relations: List[Tuple] = cursor.execute(
            'SELECT TBO.TARGET_TABLE, TBO.BASE_TABLE FROM TABLE_BASED_ON TBO JOIN TABLES T WHERE TBO.BASE_TABLE = T.ID;'
        ).fetchall()
        relations_dict: Dict[int, Set[int]] = {}
        for r in relations:
            if r[0] not in relations_dict:
                relations_dict[r[0]] = {r[1]}
            else:
                relations_dict[r[0]].add(r[1])

        for workflow in workflows:
            used_tables: List[Tuple] = cursor.execute(
                'SELECT TABLES.ID, TABLES.NAME FROM TABLE_USED_IN JOIN TABLES WHERE WORKFLOW = ? AND USED_TABLE = TABLES.ID;',
                (workflow.index,)
            ).fetchall()
            used_tables_dict: Dict[int, str] = {t[0]: t[1] for t in used_tables}
            used_tables: Set[int] = set(used_tables_dict.keys())
            src_tables: Set[int] = set()
            pot_tables: Set[int] = used_tables.copy()
            pas_tables: Set[int] = set()
            while len(pot_tables):
                t_i = pot_tables.pop()
                if t_i not in relations_dict:
                    if t_i in used_tables_dict:
                        pas_tables.add(t_i)
                        continue
                else:
                    src_tbs = relations_dict[t_i].difference(pas_tables)
                    pas_tables.add(t_i)
                    pot_tables.update(src_tbs)
                    for t_n in src_tbs:
                        if t_n in used_tables_dict:
                            src_tables.add(t_n)
            effected_tables = used_tables.difference(src_tables)
            workflow.effected_tables += [used_tables_dict[t_i] for t_i in effected_tables]
            workflow.source_tables += [used_tables_dict[t_i] for t_i in src_tables]
            predecessors_sql: str = f"""
                SELECT DISTINCT WORKFLOWS.NAME
                FROM WORKFLOWS JOIN
                (
                    SELECT CREATED_TABLE AS T_ID, WORKFLOW AS W_ID FROM TABLE_CREATED_IN
                    UNION
                    SELECT * FROM TABLE_UPDATED_IN
                )
                on WORKFLOWS.ID = W_ID
                JOIN TABLES WHERE T_ID = TABLES.ID
                AND TABLES.NAME IN ({''.join([f"'{s}', " for s in workflow.source_tables])[:-2]});
            """
            descendants_sql: str = f"""
                SELECT DISTINCT WORKFLOWS.NAME
                FROM WORKFLOWS JOIN TABLE_USED_IN
                on WORKFLOWS.ID = WORKFLOW
                JOIN TABLES WHERE USED_TABLE = TABLES.ID
                AND TABLES.NAME IN ({''.join([f"'{s}', " for s in workflow.effected_tables])[:-2]});
            """
            workflow.predecessors = [t[0] for t in cursor.execute(predecessors_sql).fetchall() if t[0] != workflow.name]
            workflow.descendants = [t[0] for t in cursor.execute(descendants_sql).fetchall() if t[0] != workflow.name and t[0] not in workflow.predecessors]
        cursor.close()

    def populate_table_data(self, table: Table):
        # updated in
        # based on sqooped
        cursor: sqlite3.Cursor = self.connection.cursor()
        used_in = cursor.execute("""
            SELECT WORKFLOWS.NAME FROM TABLE_USED_IN JOIN WORKFLOWS ON WORKFLOWS.ID = TABLE_USED_IN.WORKFLOW AND USED_TABLE = ?
        """, (table.index,)).fetchall()
        used_in = {u[0] for u in used_in}
        created_in = cursor.execute("""
                    SELECT WORKFLOWS.NAME FROM TABLE_CREATED_IN JOIN WORKFLOWS ON WORKFLOWS.ID = TABLE_CREATED_IN.WORKFLOW AND CREATED_TABLE = ?
                """, (table.index,)).fetchall()
        created_in = {c[0] for c in created_in}
        updated_in = cursor.execute("""
                            SELECT WORKFLOWS.NAME FROM TABLE_UPDATED_IN JOIN WORKFLOWS ON WORKFLOWS.ID = TABLE_UPDATED_IN.WORKFLOW AND UPDATED_TABLE = ?
                        """, (table.index,)).fetchall()
        updated_in = {u[0] for u in updated_in}
        based_on = cursor.execute("""
                                    SELECT TABLES.NAME FROM TABLE_BASED_ON JOIN TABLES ON TABLES.ID = BASE_TABLE AND TARGET_TABLE = ?
                                """, (table.index,)).fetchall()
        based_on = {b[0] for b in based_on}
        partitions = cursor.execute("""
                                    SELECT PARTITION_NAME FROM TABLE_PARTITIONS WHERE TARGET_TABLE = ?
                                """, (table.index,)).fetchall()
        partitions = {p[0] for p in partitions}
        cursor.close()
        used_in = used_in.difference(created_in)
        used_in = used_in.difference(updated_in)
        table.created_in_workflows += list(created_in)
        table.used_in_workflows += list(used_in)
        table.updated_in_workflows += list(updated_in)
        table.based_on_tables += list(based_on)
        table.partitions += list(partitions)

    def insert_new_table(self, table_name) -> Table:
        cursor: sqlite3.Cursor = self.connection.cursor()
        cursor.execute("""
            INSERT INTO 
            TABLES(ID, NAME, MEANING, AUTHORS)
            VALUES((SELECT MAX(ID) + 1 FROM TABLES), ?, '', '') 
        """, (table_name,))
        self.connection.commit()
        cursor.close()

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
                                INSERT OR IGNORE INTO TABLE_CREATED_IN(CREATED_TABLE, WORKFLOW) VALUES(?, ?)
                            """, created_ins)
        self.connection.commit()
        cursor.close()

    def insert_table_used_in(self, used_ins: List[Tuple[int, int]]):
        cursor: sqlite3.Cursor = self.connection.cursor()
        cursor.executemany("""
                                INSERT OR IGNORE INTO TABLE_USED_IN(USED_TABLE, WORKFLOW) VALUES(?, ?)
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

    def insert_table_updated_in(self, updated_ins: List[Tuple[int, int]]):
        cursor: sqlite3.Cursor = self.connection.cursor()
        cursor.executemany("""
                                INSERT OR IGNORE INTO TABLE_UPDATED_IN(UPDATED_TABLE, WORKFLOW) VALUES(?, ?)
                            """, updated_ins)
        self.connection.commit()
        cursor.close()

    def insert_table_partitions(self, table_partitions: List[Tuple[int, str]]):
        cursor: sqlite3.Cursor = self.connection.cursor()
        cursor.executemany("""
                                        INSERT OR IGNORE INTO TABLE_PARTITIONS(TARGET_TABLE, PARTITION_NAME) VALUES(?, ?)
                                    """, table_partitions)
        self.connection.commit()
        cursor.close()

    def delete_tables(self, except_table_names: Tuple = tuple()):
        cursor: sqlite3.Cursor = self.connection.cursor()
        sql: str = """DELETE FROM TABLES WHERE NAME NOT IN ("""
        for table_name in except_table_names:
            sql += f"""'{table_name}',"""
        if sql[-1] == ',':
            sql = sql[:-1]
        sql += """)"""
        print(sql)
        cursor.execute(sql)
        self.connection.commit()
        cursor.close()
