import os
import re
import glob
from time import time
import sqlparse
from typing import List, Dict, Generator
from xml.etree.ElementTree import ElementTree, Element


def index_generator(start: int) -> int:
    i = start
    while True:
        yield i
        i += 1


def resolve_global(path_to_workflow: str):
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
    for gl in re.findall(r'\${[^${}]+}', string):
        repl = resolver.send(gl[2:-1])
        next(resolver)
        string = string.replace(gl, repl)
    return string


def parse_sqoop(el: Element) -> (str, str):
    next_is_target: bool = False
    target = ''
    for arg in el:
        if 'arg' not in arg.tag:
            continue
        if next_is_target and not target:
            target = arg.text
            continue
        if arg.text == '--hive-table':
            next_is_target = True
            continue
        if target:
            break
    return target


def parse_hive_for_creating(path_to_workflow: str, el: Element) -> List[str]:
    script_path: str = path_to_workflow
    for el_ in el:
        if 'script' in el_.tag:
            script_path = os.path.join(script_path, el_.text)
    script_text: str = ''
    try:
        with open(script_path, 'r') as file:
            script_text += ''.join(file.readlines())
    except FileNotFoundError:
        return []

    created_tables: List[str] = []

    for statement in sqlparse.parse(script_text):
        i = 0
        while i < len(statement.tokens):
            if statement.tokens[i].normalized == 'CREATE':
                while not (isinstance(statement.tokens[i], sqlparse.sql.Identifier) or isinstance(statement.tokens[i],
                                                                                                  sqlparse.sql.Comparison)):
                    i += 1
                table_name: str = statement.tokens[i].value.split(' ')[0]
                created_tables.append(table_name)
                break
            i += 1
    return created_tables


def parse_workflow_for_creating(path_to_workflow_xml: str) -> Dict[str, Dict]:
    with open(path_to_workflow_xml, 'r') as workflow_xml:
        root = ElementTree(file=workflow_xml).getroot()
    path_to_workflow = os.path.sep.join(path_to_workflow_xml.split(os.path.sep)[:-1])
    workflow_name = path_to_workflow.split(os.path.sep)[-1]
    r_g = resolve_global(path_to_workflow)
    next(r_g)
    tables: Dict[str, Dict] = {}
    for el in root:
        if 'action' in el.tag:
            for el_ in el:
                table: Dict = {
                    'index': '',
                    'name': '',
                    'sqooped': False,
                    'based_on_tables': set(),
                    'created_at_workflows': {workflow_name},
                    'updated_at_workflows': set(),
                    'used_in_workflows': set(),
                    'partitions': set()
                }
                if 'hive' in el_.tag:
                    table_names: List[str] = parse_hive_for_creating(path_to_workflow, el_)
                    for table_name in table_names:
                        table['name'] = table_name
                        tables[table_name] = table
                elif 'sqoop' in el_.tag:
                    table_name: str = parse_sqoop(el_)
                    table_name: str = replace_global(table_name, r_g)
                    table['name'] = table_name
                    table['sqooped'] = True
                    tables[table_name] = table
    return tables


def find_tables_generator(paths_to_workflows: List[str]) -> (Dict[str, Dict], int):
    index_g = index_generator(1)
    progress: int = 0
    total: int = len(paths_to_workflows)
    for path in paths_to_workflows:
        tables = parse_workflow_for_creating(path)
        for table_name in tables:
            tables[table_name]['index'] = next(index_g)
        yield tables, round(progress / total * 100)
        progress += 1


def parse_workflows(working_dir: str) -> Dict:
    paths: List[str] = glob.glob(f'{working_dir}/**/workflow.xml')
    all_tables: Dict[str, Dict] = {}
    for tables, progress in find_tables_generator(paths):
        all_tables.update(tables)
        print(f'Progress {progress}/100%')
    return all_tables


def parse_workflows_coroutine(working_dir: str) -> Dict:
    paths: List[str] = glob.glob(f'{working_dir}/**/workflow.xml')
    all_tables: Dict[str, Dict] = {}
    f_t_g = find_tables_generator(paths)
    for tables, progress in f_t_g:
        all_tables.update(tables)
        yield progress
    return all_tables


if __name__ == '__main__':
    t = time()
    # temp = parse_workflows('/home/shared/PycharmProjects/parsing_tool/workflow')
    temp: Dict = {}
    try:
        gen = parse_workflows_coroutine('/home/shared/PycharmProjects/parsing_tool/workflow')
        while True:
            progress: int = next(gen)
            print(f'Progress {progress}/100%')
    except StopIteration as ret:
        temp = ret.value
    print(len(temp.keys()))
    print(time() - t)
