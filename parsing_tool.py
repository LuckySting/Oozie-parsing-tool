import json
import os
import re
import glob
from time import time
import sqlparse
from typing import List, Dict, Generator, Any, Set, Union, Tuple
from xml.etree.ElementTree import ElementTree, Element

from store import Store


def index_generator(start: int) -> int:
    """
    Generates unique integers
    :param start: start from
    :return: int value
    """
    i = start
    while True:
        yield i
        i += 1


def resolve_global(path_to_workflow: str):
    """
    Coroutine, opens job.properties file and generates dict to resolve global variables
    :param path_to_workflow: path to workflow
    :return:
    """
    path_to_job_properties: str = os.path.join(path_to_workflow, 'job.properties')
    dictionary: Dict[str, str] = {}
    try:
        with open(path_to_job_properties, 'r') as file:
            for line in file:
                if '=' in line:
                    temp = line.split('=')
                    key = temp[0]
                    value = '='.join(temp[1:])
                    dictionary[key] = value
    except FileNotFoundError:
        pass
    while True:
        key = yield
        if key in dictionary:
            yield dictionary[key].strip('\n')
        else:
            yield key


def replace_global(string: str, resolver: Generator) -> str:
    """
    Replaces all global variables in string, using resolver
    :param string: string to replace
    :param resolver: aimed resolve_global generator
    :return: string without globals
    """
    for gl in re.findall(r'\${[^${}]+}', string):
        repl = resolver.send(gl[2:-1])
        next(resolver)
        string = string.replace(gl, repl)
    return string


def parse_sqoop(el: Element) -> str:
    """
    Parse sqoop action xml element, extract table from it
    :param el: sqoop action xml element
    :return: sqoop table name
    """
    db_arg_i: int = None
    table_arg_i: int = None
    for arg_i in range(len(list(el))):
        if el[arg_i].text == '--hive-database':
            db_arg_i = arg_i + 1
        if el[arg_i].text == '--hive-table':
            table_arg_i = arg_i + 1
        if db_arg_i and table_arg_i:
            break
    if db_arg_i:
        return f'{el[db_arg_i].text}.{el[table_arg_i].text}'
    else:
        return el[table_arg_i].text


def get_hive_script(path_to_workflow: str, el: Element) -> str:
    """
    Find path to script file in workflow.xml action and read it's content
    :param path_to_workflow: path to workflow
    :param el: workflow action
    :return: script text
    """
    script_path: str = path_to_workflow
    for el_ in el:
        if 'script' in el_.tag:
            script_path = os.path.join(script_path, el_.text)
    script_text: str = ''
    try:
        with open(script_path, 'r') as file:
            script_text += ''.join(file.readlines())
    except FileNotFoundError:
        return ''
    return script_text


def extract_tables(statement: str, table_names: Set[str]) -> List[str]:
    """
    Extracts table names used in hql statement
    :param statement: hql statement
    :param table_names: set of known table names
    :return: set of used table names
    """
    used_table_names: List[str] = []
    for table_name in table_names:
        valid_name = re.match(r'^\S+\.\S+$', table_name)
        if not valid_name:
            continue
        without_schema: str = table_name.split('.')[1]
        if statement.find(f' {table_name} ') != -1 or statement.find(f' {without_schema} ') != -1:
            used_table_names.append(table_name)
    return used_table_names


def extract_partitions(statement) -> Set[str]:
    """
    Extract table partitions from hql create statement
    :param statement: hql create statement
    :return: set of column names partitioned by
    """
    partitions = re.search(r'partitioned by \([\s\w]+\)', statement.normalized, re.IGNORECASE)
    if partitions:
        word_list = partitions.group(0).lower().replace('partitioned by (', '').replace(')', '').split(' ')
        return {word_list[i] for i in range(len(word_list)) if i % 2 == 0}
    return set()


def parse_hql(script_text: str, workflow_id: int, tables_name_id_dict: Dict[str, int]):
    """
    Parses hql query and extracts relations between tables and workflows
    :param script_text: text of hql query
    :param workflow_id: workflow id
    :param tables_name_id_dict: {table_name: table_id} dict
    :return: table_based_on, table_created_in, table_partitions, table_updated_in, table_used_in
    """
    if script_text == '':
        return set(), set(), set(), set(), set()
    all_table_names = set(tables_name_id_dict.keys())
    table_based_on: Set[Tuple[int, int]] = set()
    table_created_in: Set[Tuple[int, int]] = set()
    table_partitions: Set[Tuple[int, str]] = set()
    table_updated_in: Set[Tuple[int, int]] = set()
    table_used_in: Set[Tuple[int, int]] = set()
    for statement in sqlparse.parse(script_text):
        command = statement.token_first(True, True)
        if not command:
            continue
        if command.normalized == 'CREATE':
            partitions: Set[str] = extract_partitions(statement)
            table_names: List[str] = list(reversed(extract_tables(statement.normalized, all_table_names)))
            if len(table_names) == 0:
                continue
            created_table_id = tables_name_id_dict[table_names.pop()]
            base_tables_ids = [tables_name_id_dict[t_n] for t_n in table_names]
            table_based_on.update(((created_table_id, b_t_i) for b_t_i in base_tables_ids))
            table_created_in.update(((created_table_id, workflow_id),))
            table_partitions.update(((created_table_id, p_n) for p_n in partitions))
            table_used_in.update(((b_t_i, workflow_id) for b_t_i in base_tables_ids))
        elif command.normalized == 'INSERT':
            table_names: List[str] = list(reversed(extract_tables(statement.normalized, all_table_names)))
            if len(table_names) == 0:
                continue
            inserted_table_id = tables_name_id_dict[table_names.pop()]
            base_tables_ids = [tables_name_id_dict[t_n] for t_n in table_names]
            table_based_on.update(((inserted_table_id, b_t_i) for b_t_i in base_tables_ids))
            table_updated_in.update(((inserted_table_id, workflow_id),))
            table_used_in.update(((b_t_i, workflow_id) for b_t_i in base_tables_ids))
        else:
            pass
    return table_based_on, table_created_in, table_partitions, table_updated_in, table_used_in


def parse_workflow(path_to_workflow_xml: str, workflow_id: int, table_id_name_pairs: List[Tuple[int, str]]):
    """
    Parse workflow and extracts tables and relations between them in workflow
    :param path_to_workflow_xml: path to workflow.xml
    :param workflow_id: id of that workflow
    :param table_id_name_pairs: list of (table_id, table_name) pairs
    :return: sqooped_tables, workflows, table_based_on, table_created_in, table_partitions, table_updated_in, table_used_in
    """
    with open(path_to_workflow_xml, 'r') as workflow_xml:
        root = ElementTree(file=workflow_xml).getroot()
    path_to_workflow = os.path.sep.join(path_to_workflow_xml.split(os.path.sep)[:-1])
    workflow_name = path_to_workflow.split(os.path.sep)[-1]
    r_g = resolve_global(path_to_workflow)
    next(r_g)
    tables_id_name_dict: Dict[int, str] = {t[0]: t[1] for t in table_id_name_pairs}
    tables_name_id_dict: Dict[str, int] = {t[1]: t[0] for t in table_id_name_pairs}
    index_g = index_generator(max(tables_id_name_dict.keys()) + 1)
    sqooped_tables: Set[Tuple[int, str, bool]] = set()
    workflows: Set[Tuple[int, str]] = {(workflow_id, workflow_name)}
    table_based_on: Set[Tuple[int, int]] = set()
    table_created_in: Set[Tuple[int, int]] = set()
    table_partitions: Set[Tuple[int, str]] = set()
    table_updated_in: Set[Tuple[int, int]] = set()
    table_used_in: Set[Tuple[int, int]] = set()
    for el in root:
        if 'action' in el.tag:
            for el_ in el:
                if 'sqoop' in el_.tag:
                    table_name: str = parse_sqoop(el_)
                    table_name: str = replace_global(table_name, r_g)
                    table_id: int = tables_name_id_dict.get(table_name, None)
                    new: bool = False
                    if table_id is None:
                        table_id = next(index_g)
                        tables_id_name_dict.update({table_id: table_name})
                        tables_name_id_dict.update({table_name: table_id})
                        new = True
                    sqooped_tables.update(((table_id, table_name, new),))
                    table_used_in.update(((table_id, workflow_id),))
    for el in root:
        if 'action' in el.tag:
            for el_ in el:
                if 'hive' in el_.tag:
                    script_text = replace_global(get_hive_script(path_to_workflow, el_), r_g)
                    _table_based_on, _table_created_in, _table_partitions, _table_updated_in, _table_used_in = parse_hql(
                        script_text, workflow_id, tables_name_id_dict)
                    table_based_on.update(_table_based_on)
                    table_created_in.update(_table_created_in)
                    table_partitions.update(_table_partitions)
                    table_updated_in.update(_table_updated_in)
                    table_used_in.update(_table_used_in)

    return sqooped_tables, workflows, table_based_on, table_created_in, table_partitions, table_updated_in, table_used_in


def parse_workflows_coroutine(working_dir: str, table_id_name_pairs: List[Tuple[int, str]]) -> Tuple[List[Tuple]]:
    """
    Coroutine, witch parses workflows one by one in working_dir, looking for tables in it
    :param working_dir: dir with workflows directories
    :param table_id_name_pairs: list of pairs (table_id, table_name) from hive/impala schema
    :return: sqooped_tables, workflows, table_based_on, table_created_in, table_partitions, table_updated_in, table_used_in
    (yields progress value after each parsed workflow)
    """
    paths_to_workflows: List[str] = glob.glob(f'{working_dir}/**/workflow.xml')
    progress: int = 0
    length: int = len(paths_to_workflows)
    sqooped_tables: Set[Tuple[int, str, bool]] = set()
    workflows: Set[Tuple[int, str]] = set()
    table_based_on: Set[Tuple[int, int]] = set()
    table_created_in: Set[Tuple[int, int]] = set()
    table_partitions: Set[Tuple[int, str]] = set()
    table_updated_in: Set[Tuple[int, int]] = set()
    table_used_in: Set[Tuple[int, int]] = set()
    index_g = index_generator(1)
    for path in paths_to_workflows:
        _sqooped_tables, _workflows, _table_based_on, _table_created_in, _table_partitions, _table_updated_in, _table_used_in = parse_workflow(
            path, next(index_g), table_id_name_pairs)
        sqooped_tables.update(_sqooped_tables)
        workflows.update(_workflows)
        table_based_on.update(_table_based_on)
        table_created_in.update(_table_created_in)
        table_partitions.update(_table_partitions)
        table_updated_in.update(_table_updated_in)
        table_used_in.update(_table_used_in)
        yield round(progress / length * 100)
        progress += 1
    return sqooped_tables, workflows, table_based_on, table_created_in, table_partitions, table_updated_in, table_used_in


if __name__ == '__main__':
    store = Store('db.sqlite3')
    for a in parse_workflows_coroutine('./workflow', store.get_tables(id_name_pairs=True)):
        print(a)
