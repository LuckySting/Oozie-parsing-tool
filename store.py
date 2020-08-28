import sqlite3
from enum import Enum
from typing import List, Tuple, Union, Dict, Set

from PyQt5.QtGui import QColor


class Color(Enum):
    RED: str = 'red'
    BLUE: str = 'blue'
    MAGENTA: str = 'magenta'
    GREEN: str = 'green'
    NONE: str = None

    @classmethod
    def to_q_color(cls, color: 'Color') -> QColor:
        if color is None:
            return QColor('black')
        else:
            return QColor(color.value)

    @classmethod
    def from_q_color(cls, color: QColor) -> 'Color':
        for c in cls:
            if Color.to_q_color(c) == color:
                return c
        return cls.NONE

    def __lt__(self, other: 'Color'):
        if other is None:
            other: Color = Color.NONE
        colors: List[Color] = [Color.MAGENTA, Color.BLUE, Color.GREEN, Color.RED, Color.NONE]
        return colors.index(self) < colors.index(other)

    def __gt__(self, other: 'Color'):
        return not self < other


class Table:
    def __init__(self, index: int, name: str, meaning: str, authors: str, sqooped: Union[bool, int],
                 color: Union[Color, str] = Color.NONE,
                 unplugged=None,
                 columns=None,
                 created_in_workflows=None,
                 used_in_workflows=None,
                 updated_in_workflows=None, based_on_tables=None, first_based_on_tables=None, partitions=None):
        if columns is None:
            columns = list()
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
        if first_based_on_tables is None:
            first_based_on_tables = list()
        self.index: int = index
        self.name: str = name
        self.meaning: str = meaning
        self.authors: str = authors
        self.color: Color = color if not isinstance(color, str) else Color(color)
        self.columns: List[str] = columns
        self.sqooped: bool = bool(sqooped)
        self.created_in_workflows: List[str] = created_in_workflows
        self.updated_in_workflows: List[str] = updated_in_workflows
        self.used_in_workflows: List[str] = used_in_workflows
        self.based_on_tables: List[str] = based_on_tables
        self.first_based_on_tables: List[str] = first_based_on_tables
        self.partitions: List[str] = partitions
        self.unplugged: bool = unplugged

    @staticmethod
    def from_dict(data: Dict[str, any]):
        return Table(**data)

    def __str__(self):
        return f'{self.index}. {self.name}'

    def __repr__(self):
        return f'Table({str(self)})'

    def __lt__(self, other):
        if self.color != other.color:
            return self.color < other.color
        else:
            return self.name < other.name

    def __gt__(self, other):
        return not self < other


class Workflow:
    def __init__(self, index: int, name: str, color: Color = Color.NONE,
                 source_tables=None, effected_tables=None, predecessors=None, descendants=None):
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
        self.color: Color = Color(color)
        self.source_tables: List[str] = source_tables
        self.effected_tables: List[str] = effected_tables
        self.predecessors: List[str] = predecessors
        self.descendants: List[str] = descendants

    @staticmethod
    def from_dict(data: Dict[str, any]):
        return Workflow(**data)

    def __str__(self):
        return f'{self.index}. {self.name}'

    def __repr__(self):
        return f'Workflow({str(self)})'


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
            cursor.execute('DROP TABLE IF EXISTS TABLE_COLUMNS;')
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS TABLES
            (
                ID INTEGER PRIMARY KEY AUTOINCREMENT,
                NAME TEXT NOT NULL UNIQUE,
                MEANING TEXT NOT NULL DEFAULT '',
                AUTHORS TEXT NOT NULL DEFAULT '',
                SQOOPED BOOLEAN NOT NULL DEFAULT FALSE,
                COLOR TEXT DEFAULT NULL
            );
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS WORKFLOWS
            (
                ID INTEGER PRIMARY KEY AUTOINCREMENT,
                NAME TEXT NOT NULL UNIQUE,
                COLOR TEXT DEFAULT NULL
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
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS TABLE_COLUMNS
            (
                TABLE_ID REFERENCES TABLES,
                COLUMN_NAME TEXT NOT NULL,
                COLUMN_TYPE TEXT NOT NULL,
                CONSTRAINT TC_PK PRIMARY KEY(TABLE_ID, COLUMN_NAME)
            );
        """)
        cursor.close()

    def update_workflow(self, workflow: Workflow) -> None:
        cursor: sqlite3.Cursor = self.connection.cursor()
        cursor.execute('UPDATE WORKFLOWS SET COLOR=? WHERE ID = ?',
                       (workflow.color.value, workflow.index))
        self.connection.commit()
        cursor.close()

    def update_table(self, table: Table) -> None:
        cursor: sqlite3.Cursor = self.connection.cursor()
        cursor.execute('UPDATE TABLES SET MEANING=?, AUTHORS=?, COLOR=? WHERE ID = ?',
                       (table.meaning, table.authors, table.color.value, table.index))
        self.connection.commit()
        cursor.close()

    def get_tables(self, search_text: str = '', color_filter=None, only_names: bool = False,
                   id_name_pairs: bool = False, only_unplugged=False) -> List[Union[str, Table, Tuple[int, str]]]:
        if color_filter is None:
            color_filter = []
        sql: str = """
        SELECT 
            ID,
            NAME,
            MEANING,
            AUTHORS,
            SQOOPED,
            COLOR,
            (
               SELECT COUNT(*)
               FROM (
                    SELECT TARGET_TABLE AS COLS
                    FROM TABLE_BASED_ON
                    WHERE TARGET_TABLE = TABLES.ID
                    UNION
                    SELECT CREATED_TABLE
                    FROM TABLE_CREATED_IN
                    WHERE CREATED_TABLE = TABLES.ID
                    UNION
                    SELECT UPDATED_TABLE
                    FROM TABLE_UPDATED_IN
                    WHERE UPDATED_TABLE = TABLES.ID
                    UNION
                    SELECT USED_TABLE
                    FROM TABLE_USED_IN
                    WHERE USED_TABLE = TABLES.ID
                )
            ) AS UNPLUGGED
        FROM TABLES 
        WHERE instr(NAME, ?) > 0
        """
        if len(color_filter) > 0:
            if Color.NONE not in color_filter:
                where_color: str = ' AND COLOR IN (' + ', '.join([f'\'{c.value}\'' for c in color_filter]) + ')'
            else:
                where_color: str = ' AND (COLOR IN (' + ', '.join(
                    [f'\'{c.value}\'' for c in color_filter]) + ') OR COLOR IS NULL)'
            sql += where_color
        if only_unplugged:
            sql += 'AND UNPLUGGED = 0'
        cursor: sqlite3.Cursor = self.connection.cursor()
        tables: List[Tuple] = cursor.execute(sql, (search_text,)).fetchall()
        cursor.close()
        if only_names:
            return [d[1] for d in tables]
        if id_name_pairs:
            return [(d[0], d[1]) for d in tables]
        return [Table(*d) for d in tables]

    def get_workflows(self, search_text: str = '', only_names: bool = False, color_filter: List[Color] = None) -> List[
        Union[str, Workflow]]:
        if color_filter is None:
            color_filter = []
        sql: str = 'SELECT ID, NAME, COLOR FROM WORKFLOWS WHERE instr(NAME, ?) > 0'
        if len(color_filter) > 0:
            if Color.NONE not in color_filter:
                where_color: str = ' AND COLOR IN (' + ', '.join([f'\'{c.value}\'' for c in color_filter]) + ')'
            else:
                where_color: str = ' AND (COLOR IN (' + ', '.join(
                    [f'\'{c.value}\'' for c in color_filter]) + ') OR COLOR IS NULL)'
            sql += where_color
        cursor: sqlite3.Cursor = self.connection.cursor()
        workflows: List[Tuple] = cursor.execute(
            sql, (search_text,)).fetchall()
        cursor.close()
        if only_names:
            return [d[1] for d in workflows]
        return [Workflow(*d) for d in workflows]

    def get_tables_by_names(self, table_names: List[str]) -> List[Table]:
        cursor: sqlite3.Cursor = self.connection.cursor()
        sql: str = 'SELECT * FROM TABLES WHERE NAME IN ('
        sql += ', '.join([f"'{t_n}'" for t_n in table_names]) + ')'
        tables: List[Tuple] = cursor.execute(sql).fetchall()
        cursor.close()
        return [Table(*d) for d in tables]

    def get_workflows_by_names(self, workflow_names: List[str]) -> List[Workflow]:
        cursor: sqlite3.Cursor = self.connection.cursor()
        sql: str = 'SELECT ID, NAME, COLOR FROM WORKFLOWS WHERE 1=0'
        for name in workflow_names:
            sql += f' OR NAME = "{name}"'
        workflows: List[Tuple] = cursor.execute(sql).fetchall()
        cursor.close()
        return [Workflow(*d) for d in workflows]

    def get_db_status(self) -> str:
        cursor: sqlite3.Cursor = self.connection.cursor()
        workflows_exists: bool = int(cursor.execute('SELECT COUNT(*) FROM WORKFLOWS').fetchall()[0][0]) > 0
        tables_exists: bool = int(cursor.execute('SELECT COUNT(*) FROM TABLES').fetchall()[0][0]) > 0
        columns_exists: bool = int(cursor.execute('SELECT COUNT(*) FROM TABLE_COLUMNS').fetchall()[0][0]) > 0
        if tables_exists and not columns_exists and not workflows_exists:
            return 'hive_extracted'
        elif tables_exists and columns_exists and not workflows_exists:
            return 'impala_extracted'
        elif tables_exists and columns_exists and workflows_exists:
            return 'workflows_computed'
        else:
            return 'db_empty'

    def get_related_tables(self, target_wf_name: str, base_wf_name: str) -> List[str]:
        cursor: sqlite3.Cursor = self.connection.cursor()
        sql: str = """
            SELECT DISTINCT
                (SELECT NAME FROM TABLES WHERE ID = E.EFFECTED_TABLE) AS RELATED_TABLE
            FROM
            (
                SELECT CREATED_TABLE AS EFFECTED_TABLE, WORKFLOW AS WORKFLOW_ID, W2.NAME AS WORKFLOW_NAME FROM TABLE_CREATED_IN JOIN WORKFLOWS W2 on TABLE_CREATED_IN.WORKFLOW = W2.ID
                UNION
                SELECT UPDATED_TABLE, WORKFLOW, W3.NAME FROM TABLE_UPDATED_IN JOIN WORKFLOWS W3 on TABLE_UPDATED_IN.WORKFLOW = W3.ID
            ) AS E JOIN TABLE_USED_IN TUI ON E.EFFECTED_TABLE = TUI.USED_TABLE JOIN WORKFLOWS W on TUI.WORKFLOW = W.ID
            WHERE E.WORKFLOW_NAME = ? AND W.NAME = ?;
        """
        related_tables: List[str] = [t[0] for t in cursor.execute(sql, (base_wf_name, target_wf_name))]
        return related_tables

    def populate_workflow_data(self, workflow: Workflow):
        cursor: sqlite3.Cursor = self.connection.cursor()
        relations: List[Tuple] = cursor.execute(
            'SELECT TARGET_TABLE, BASE_TABLE FROM TABLE_BASED_ON;'
        ).fetchall()
        relations_dict: Dict[int, Set[int]] = {}
        for r in relations:
            if r[0] not in relations_dict:
                relations_dict[r[0]] = {r[1]}
            else:
                relations_dict[r[0]].add(r[1])

        used_tables: List[Tuple] = cursor.execute(
            'SELECT TABLES.ID, TABLES.NAME, TABLES.MEANING, TABLES.AUTHORS, TABLES.SQOOPED, TABLES.COLOR FROM TABLE_USED_IN JOIN TABLES WHERE WORKFLOW = ? AND USED_TABLE = TABLES.ID;',
            (workflow.index,)
        ).fetchall()
        used_tables: List[Table] = [self.populate_table_data(Table(*t)) for t in used_tables]
        src_tables: Set[str] = set()
        for u_t in used_tables:
            src_tables.add(u_t.name)
            src_tables.update(u_t.based_on_tables)
        workflow.source_tables += list(src_tables)

        effected_tables: List[Tuple] = cursor.execute(
            """
            SELECT T.NAME FROM TABLE_CREATED_IN TCI JOIN TABLES T ON TCI.CREATED_TABLE = T.ID AND TCI.WORKFLOW = ?
            UNION
            SELECT T.NAME FROM TABLE_UPDATED_IN TUI JOIN TABLES T ON TUI.UPDATED_TABLE = T.ID AND TUI.WORKFLOW = ?
            """, (workflow.index, workflow.index)
        )
        effected_tables: Set[str] = {t[0] for t in effected_tables}
        workflow.effected_tables = effected_tables

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
        workflow.descendants = [t[0] for t in cursor.execute(descendants_sql).fetchall() if
                                t[0] != workflow.name and t[0] not in workflow.predecessors]
        cursor.close()

    def populate_table_data(self, table: Table) -> Table:
        cursor: sqlite3.Cursor = self.connection.cursor()
        relations: List[Tuple] = cursor.execute(
            'SELECT TARGET_TABLE, BASE_TABLE FROM TABLE_BASED_ON;'
        ).fetchall()
        relations_dict: Dict[int, Set[int]] = {}
        for r in relations:
            if r[0] not in relations_dict:
                relations_dict[r[0]] = {r[1]}
            else:
                relations_dict[r[0]].add(r[1])
        table_id_name: List[Tuple] = cursor.execute(
            'SELECT ID, NAME FROM TABLES'
        ).fetchall()
        table_id_name_dict: Dict[int, str] = {t[0]: t[1] for t in table_id_name}
        used_in = cursor.execute("""
            SELECT DISTINCT WORKFLOWS.NAME FROM TABLE_USED_IN JOIN WORKFLOWS ON WORKFLOWS.ID = TABLE_USED_IN.WORKFLOW AND USED_TABLE = ?
        """, (table.index,)).fetchall()
        used_in = {u[0] for u in used_in}
        created_in = cursor.execute("""
                    SELECT DISTINCT WORKFLOWS.NAME FROM TABLE_CREATED_IN JOIN WORKFLOWS ON WORKFLOWS.ID = TABLE_CREATED_IN.WORKFLOW AND CREATED_TABLE = ?
                """, (table.index,)).fetchall()
        created_in = {c[0] for c in created_in}
        updated_in = cursor.execute("""
                            SELECT DISTINCT WORKFLOWS.NAME FROM TABLE_UPDATED_IN JOIN WORKFLOWS ON WORKFLOWS.ID = TABLE_UPDATED_IN.WORKFLOW AND UPDATED_TABLE = ?
                        """, (table.index,)).fetchall()
        updated_in = {u[0] for u in updated_in}
        based_on = cursor.execute("""
                                    SELECT DISTINCT BASE_TABLE FROM TABLE_BASED_ON WHERE TARGET_TABLE = ?
                                """, (table.index,)).fetchall()
        based_on_tables: Set[int] = {t[0] for t in based_on}
        table.first_based_on_tables = list({table_id_name_dict[t[0]] for t in based_on})
        base_tables: Set[int] = set()
        while len(based_on_tables):
            table_id: int = based_on_tables.pop()
            base_tables.add(table_id)
            if table_id in relations_dict:
                based_on_tables.update(relations_dict[table_id].difference(base_tables))
        based_on = [table_id_name_dict[i] for i in base_tables if i not in based_on_tables]
        partitions = cursor.execute("""
                                    SELECT DISTINCT PARTITION_NAME FROM TABLE_PARTITIONS WHERE TARGET_TABLE = ?
                                """, (table.index,)).fetchall()
        partitions = {p[0] for p in partitions}
        columns = cursor.execute("""
                                SELECT DISTINCT COLUMN_NAME||'['||COLUMN_TYPE||']' FROM TABLE_COLUMNS WHERE TABLE_ID = ?
                                """, (table.index,)).fetchall()
        columns = {c[0] for c in columns}
        cursor.close()
        table.created_in_workflows = list(created_in)
        table.used_in_workflows = list(used_in)
        table.updated_in_workflows = list(updated_in)
        table.based_on_tables = list(based_on)
        table.partitions = list(partitions)
        table.columns = list(columns)
        return table

    def insert_new_table(self, table_name) -> Table:
        cursor: sqlite3.Cursor = self.connection.cursor()
        cursor.execute("""
            INSERT INTO 
            TABLES(ID, NAME, MEANING, AUTHORS)
            VALUES((SELECT MAX(ID) + 1 FROM TABLES), ?, '', '') 
        """, (table_name,))
        self.connection.commit()
        cursor.close()
        return self.get_tables_by_names([table_name])[0]

    def insert_tables(self, tables: List[Table]):
        cursor: sqlite3.Cursor = self.connection.cursor()
        cursor.executemany("""
                            INSERT OR IGNORE INTO TABLES(ID, NAME, MEANING, AUTHORS, SQOOPED) VALUES(?, ?, ?, ?, ?)
                           """, [(t.index, t.name, t.meaning, t.authors, t.sqooped) for t in tables])
        self.connection.commit()
        cursor.close()

    def insert_sqooped_tables(self, tables: Set[Tuple[int, str, bool]]):
        insert_tables: List[Tuple[int, str]] = [(t[0], t[1]) for t in tables if t[2]]
        update_tables: List[Tuple[int, str]] = [(t[0], t[1]) for t in tables if not t[2]]
        cursor: sqlite3.Cursor = self.connection.cursor()
        cursor.executemany("""
                            INSERT OR IGNORE INTO TABLES(ID, NAME, MEANING, AUTHORS, SQOOPED) VALUES(?, ?, ?, ?, ?)
                           """, [(t[0], t[1], '', '', True) for t in insert_tables])
        cursor.executemany("""
                                    UPDATE TABLES SET SQOOPED = 1 WHERE ID = ?
                                   """, [(t[0],) for t in update_tables])
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

    def insert_table_columns(self, table_columns: List[Tuple[int, str, str]]):
        cursor: sqlite3.Cursor = self.connection.cursor()
        cursor.executemany("""
                                        INSERT OR IGNORE INTO TABLE_COLUMNS(TABLE_ID, COLUMN_NAME, COLUMN_TYPE) 
                                        VALUES(?, ?, ?);
                                        """, table_columns)
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
        cursor.execute(sql)
        self.connection.commit()
        cursor.close()
