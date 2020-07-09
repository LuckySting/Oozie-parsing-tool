import json
import os
import re
import glob
from time import time
import sqlparse
from typing import List, Dict, Generator, Any, Set, Union
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


def get_hive_script(path_to_workflow: str, el: Element) -> str:
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


def is_sub_select(parsed: sqlparse.sql.Statement):
    if not parsed.is_group:
        return False
    return 'SELECT' in parsed.value.upper()


def extract_from_select(parsed: sqlparse.sql.Statement):
    from_seen = False
    for item in parsed.tokens:
        if item.is_group:
            for x in extract_from_select(item):
                yield x
        if from_seen:
            if is_sub_select(item):
                for x in extract_from_select(item):
                    yield x
            elif item.ttype is sqlparse.tokens.Keyword and item.value.upper() in {'ORDER', 'GROUP', 'BY', 'HAVING',
                                                                                  'GROUP BY'}:
                from_seen = False
            else:
                if (item.ttype is sqlparse.tokens.Name or isinstance(item, sqlparse.sql.Identifier)) \
                        and not is_reserved(item):
                    yield item
        if item.ttype is sqlparse.tokens.Keyword and item.value.upper() == 'FROM':
            from_seen = True


def is_reserved(token: Any):
    keywords: Set[str] = {'ADD', 'ADMIN', 'AFTER', 'ANALYZE', 'ARCHIVE', 'ASC', 'BEFORE', 'BUCKET', 'BUCKETS',
                          'CASCADE', 'CHANGE', 'CLUSTER', 'CLUSTERED', 'CLUSTERSTATUS', 'COLLECTION', 'COLUMNS',
                          'COMMENT', 'COMPACT', 'COMPACTIONS', 'COMPUTE', 'CONCATENATE', 'CONTINUE', 'DATA',
                          'DATABASES', 'DATETIME', 'DAY', 'DBPROPERTIES', 'DEFERRED', 'DEFINED', 'DELIMITED',
                          'DEPENDENCY', 'DESC', 'DIRECTORIES', 'DIRECTORY', 'DISABLE', 'DISTRIBUTE', 'ELEM_TYPE',
                          'ENABLE', 'ESCAPED', 'EXCLUSIVE', 'EXPLAIN', 'EXPORT', 'FIELDS', 'FILE', 'FILEFORMAT',
                          'FIRST', 'FORMAT', 'FORMATTED', 'FUNCTIONS', 'HOLD_DDLTIME', 'HOUR', 'IDXPROPERTIES',
                          'IGNORE', 'INDEX', 'INDEXES', 'INPATH', 'INPUTDRIVER', 'INPUTFORMAT', 'ITEMS', 'JAR', 'KEYS',
                          'KEY_TYPE', 'LIMIT', 'LINES', 'LOAD', 'LOCATION', 'LOCK', 'LOCKS', 'LOGICAL', 'LONG',
                          'MAPJOIN', 'MATERIALIZED', 'METADATA', 'MINUS', 'MINUTE', 'MONTH', 'MSCK', 'NOSCAN',
                          'NO_DROP', 'OFFLINE', 'OPTION', 'OUTPUTDRIVER', 'OUTPUTFORMAT', 'OVERWRITE', 'OWNER',
                          'PARTITIONED', 'PARTITIONS', 'PLUS', 'PRETTY', 'PRINCIPALS', 'PROTECTION', 'PURGE', 'READ',
                          'READONLY', 'REBUILD', 'RECORDREADER', 'RECORDWRITER', 'REGEXP', 'RELOAD', 'RENAME',
                          'REPAIR', 'REPLACE', 'REPLICATION', 'RESTRICT', 'REWRITE', 'RLIKE', 'ROLE', 'ROLES',
                          'SCHEMA', 'SCHEMAS', 'SECOND', 'SEMI', 'SERDE', 'SERDEPROPERTIES', 'SERVER', 'SETS',
                          'SHARED', 'SHOW', 'SHOW_DATABASE', 'SKEWED', 'SORT', 'SORTED', 'SSL', 'STATISTICS', 'STORED',
                          'STREAMTABLE', 'STRING', 'STRUCT', 'TABLES', 'TBLPROPERTIES', 'TEMPORARY', 'TERMINATED',
                          'TINYINT', 'TOUCH', 'TRANSACTIONS', 'UNARCHIVE', 'UNDO', 'UNIONTYPE', 'UNLOCK', 'UNSET',
                          'UNSIGNED', 'URI', 'USE', 'UTC', 'UTCTIMESTAMP', 'VALUE_TYPE', 'VIEW', 'WHILE', 'YEAR'}
    if isinstance(token, str):
        return token.upper() in keywords
    return token.value.upper() in keywords


def extract_from_create(parsed: sqlparse.sql.Statement):
    i = 0
    for item in parsed.tokens:
        if isinstance(item, sqlparse.sql.Identifier):
            if not is_reserved(item):
                yield item
            i += 1
            continue
        if item.is_group:
            if isinstance(item, sqlparse.sql.Parenthesis):
                if 'SELECT' not in item.normalized.upper():
                    continue
            for x in extract_from_create(item):
                yield x
        if item.normalized.upper() == 'SELECT':
            select_tokens = parsed.tokens[i:]
            select_statement = sqlparse.sql.Statement(tokens=select_tokens)
            for x in extract_from_select(select_statement):
                yield x
            break
        i += 1


def extract_from_external_create(parsed: sqlparse.sql.Statement) -> List[str]:
    return [parsed.value.replace('  ', ' ').split(' (')[0].split(' ')[-1]]


def magic_filter(identifies: List[str]) -> List[str]:
    filtered: List[str] = []
    for idf in identifies:
        bad = False
        for word in idf.lower().replace('stored as parquet', ' ').split(' '):
            if is_reserved(word):
                bad = True
                break
        if not bad:
            filtered.append(idf)
    return filtered


def replace_with(identifies: List[sqlparse.sql.Identifier]) -> List[str]:
    tables: List[str] = []
    for idf in identifies:
        match = re.match(r'^.+\s\(', idf.value)
        if match:
            tables.append(match.group(0).replace(' (', ''))
            continue
        match = re.match(r'^\(.+\)', idf.value)
        if match:
            tables.append(match.group(0).replace('(', '').replace(')', ''))
            continue
        if ' AS ' in idf.value.upper() and '(' in idf.value:

            alias: str = idf.value.lower().split('as')[0].strip()
            sub_tables: List[sqlparse.sql.Token] = list(extract_from_select(idf))
            for sub_table in sub_tables:
                table_name: str = sub_table.value.split(' ')[0]
                tables.append(f'{table_name} {alias}')
        else:
            tables.append(idf.value)
    return list(dict.fromkeys(tables))


def replace_alias(identifies: List[str]) -> List[str]:
    identifies = [i.lower().replace('stored as parquet', ' ').replace(' as ', ' ') for i in identifies]
    alias_table: Dict[str, str] = {i.split(' ')[-1]: i.split(' ')[0] for i in identifies}
    tables: List[str] = []
    for alias in alias_table:
        current_alias: str = alias
        while current_alias in alias_table:
            if current_alias == alias_table[current_alias]:
                break
            current_alias = alias_table[current_alias]
        if current_alias not in tables:
            tables.append(current_alias)
    return tables


def parse_hql_create(statement: sqlparse.sql.Statement, workflow_name: str) -> List[Dict[str, Any]]:
    output: List[Dict[str, Any]] = []
    if 'EXTERNAL' in statement.value.upper():
        table_names = extract_from_external_create(statement)
    else:
        identifies = list(dict.fromkeys(extract_from_create(statement)))
        table_names = replace_alias(magic_filter(replace_with(identifies)))
    table = {
        'index': None,
        'name': table_names[0],
        'sqooped': False,
        'created_in_workflows': {workflow_name},
        'used_in_workflows': set(),
        'based_on_tables': set(),
        'used_by_tables': set(),
        'updated_in_workflows': set()
    }
    for table_name in table_names[1:]:
        table['based_on_tables'].add(table_name)
        output.append({
            'index': None,
            'name': table_name,
            'sqooped': False,
            'created_in_workflows': set(),
            'used_in_workflows': {workflow_name},
            'based_on_tables': set(),
            'used_by_tables': {table['name']},
            'updated_in_workflows': set()
        })
    output.append(table)
    return output


def extract_from_insert(parsed: sqlparse.sql.Statement):
    i = 0
    for item in parsed.tokens:
        if isinstance(item, sqlparse.sql.Identifier):
            yield item
            i += 1
            continue
        if item.is_group and not isinstance(item, sqlparse.sql.Parenthesis):
            for x in extract_from_insert(item):
                yield x
        if item.value.upper() == 'SELECT':
            select_tokens = parsed.tokens[i:]
            select_statement = sqlparse.sql.Statement(tokens=select_tokens)
            for x in extract_from_select(select_statement):
                yield x
            break
        i += 1


def parse_hql_insert(statement: sqlparse.sql.Statement, workflow_name: str) -> List[Dict[str, Any]]:
    identifies: List[sqlparse.tokens.Token] = list(extract_from_insert(statement))
    table_names: List[str] = replace_alias(magic_filter(replace_with(identifies)))
    output: List[Dict[str, Any]] = []
    table = {
        'index': None,
        'name': table_names[0],
        'sqooped': False,
        'created_in_workflows': set(),
        'used_in_workflows': {workflow_name},
        'based_on_tables': set(),
        'used_by_tables': set(),
        'updated_in_workflows': {workflow_name}
    }
    for table_name in table_names[1:]:
        table['based_on_tables'].add(table_name)
        output.append({
            'index': None,
            'name': table_name,
            'sqooped': False,
            'created_in_workflows': set(),
            'used_in_workflows': {workflow_name},
            'based_on_tables': set(),
            'used_by_tables': {table['name']},
            'updated_in_workflows': set()
        })
    output.append(table)
    return output


def parse_hql(script_text: str, workflow_name: str) -> List[Dict[str, Any]]:
    if script_text == '':
        return {}
    formatted_script: List[str] = []
    for line in script_text.split('\n'):
        if '--' not in line:
            formatted_script.append(line)
    script_text = ' '.join(formatted_script)
    tables: List[Dict[str, Any]] = []
    for statement in sqlparse.parse(script_text):
        command = statement.token_first(True, True)
        if not command:
            continue
        if command.normalized == 'CREATE':
            tables += parse_hql_create(statement, workflow_name)
        elif command.normalized == 'INSERT':
            tables += parse_hql_insert(statement, workflow_name)
    return tables


def parse_workflow(path_to_workflow_xml: str) -> Dict[str, Dict]:
    with open(path_to_workflow_xml, 'r') as workflow_xml:
        root = ElementTree(file=workflow_xml).getroot()
    path_to_workflow = os.path.sep.join(path_to_workflow_xml.split(os.path.sep)[:-1])
    workflow_name = path_to_workflow.split(os.path.sep)[-1]
    r_g = resolve_global(path_to_workflow)
    next(r_g)
    tables: Dict[str, Dict[str, Any]] = {}
    for el in root:
        if 'action' in el.tag:
            for el_ in el:
                if 'hive' in el_.tag:
                    script_text = replace_global(get_hive_script(path_to_workflow, el_), r_g)
                    tables_: List[Dict[str, Any]] = parse_hql(script_text, workflow_name)
                    for table in tables_:
                        if table['name'] not in tables:
                            tables[table['name']] = table
                        else:
                            tables[table['name']]['created_in_workflows'].update(table['created_in_workflows'])
                            tables[table['name']]['used_in_workflows'].update(table['used_in_workflows'])
                            tables[table['name']]['updated_in_workflows'].update(table['updated_in_workflows'])
                            tables[table['name']]['based_on_tables'].update(table['based_on_tables'])
                            tables[table['name']]['used_by_tables'].update(table['used_by_tables'])
                elif 'sqoop' in el_.tag:
                    table_name: str = parse_sqoop(el_)
                    table_name: str = replace_global(table_name, r_g)
                    if table_name not in tables:
                        tables[table_name] = {
                            'index': None,
                            'name': table_name,
                            'sqooped': True,
                            'created_in_workflows': {workflow_name},
                            'used_in_workflows': set(),
                            'updated_in_workflows': set(),
                            'based_on_tables': set(),
                            'used_by_tables': set()
                        }
                    else:
                        tables[table_name]['sqooped'] = True
    return tables


def find_tables_generator(paths_to_workflows: List[str]) -> (Dict[str, Dict], int):
    index_g = index_generator(1)
    progress: int = 0
    total: int = len(paths_to_workflows)
    for path in paths_to_workflows:
        tables = parse_workflow(path)
        for table_name in tables:
            tables[table_name]['index'] = next(index_g)
        yield tables, round(progress / total * 100)
        progress += 1


def parse_workflows(working_dir: str) -> Dict:
    paths: List[str] = glob.glob(f'{working_dir}/**/workflow.xml')
    all_tables: Dict[str, Dict] = {}
    for tables, progress in find_tables_generator(paths):
        for table_name in tables:
            if table_name not in all_tables:
                all_tables[table_name] = tables[table_name]
            else:
                for key in tables[table_name]:
                    if isinstance(tables[table_name][key], set):
                        all_tables[table_name][key].update(tables[table_name][key])
        print(f'Progress {progress}/100%')
    return all_tables


def link_tables(tables: Dict[str, Dict]) -> Dict[str, Dict]:
    for table_name in tables:
        for base_table_name in tables[table_name]['based_on_tables']:
            if base_table_name in table_name:
                tables[base_table_name]['used_by_tables'].add(table_name)
        for used_table_name in tables[table_name]['used_by_tables']:
            if used_table_name in table_name:
                tables[used_table_name]['based_on_tables'].add(table_name)


def extract_workflows(tables: Dict[str, Dict]) -> Dict[str, Dict]:
    index_g = index_generator(1)
    workflows: Dict[str, Dict] = {}
    for table_name in tables:
        for workflow_name in tables[table_name]['created_in_workflows']:
            if workflow_name not in workflows:
                workflows[workflow_name] = {
                    'index': next(index_g),
                    'name': workflow_name,
                    'source_tables': set(),
                    'effected_tables': {table_name}
                }
            else:
                workflows[workflow_name]['effected_tables'].add(table_name)
        for workflow_name in tables[table_name]['used_in_workflows']:
            if workflow_name not in workflows:
                workflows[workflow_name] = {
                    'index': next(index_g),
                    'name': workflow_name,
                    'source_tables': {table_name},
                    'effected_tables': set()
                }
            else:
                workflows[workflow_name]['source_tables'].add(table_name)
    return workflows


def parse_workflows_coroutine(working_dir: str) -> Dict:
    paths: List[str] = glob.glob(f'{working_dir}/**/workflow.xml')
    all_tables: Dict[str, Dict] = {}
    all_workflows: Dict[str, Dict] = {}
    f_t_g = find_tables_generator(paths)
    for tables, progress in f_t_g:
        all_tables.update(tables)
        yield progress
    link_tables(all_tables)
    for table_name in all_tables:
        for key in all_tables[table_name]:
            if isinstance(all_tables[table_name][key], set):
                all_tables[table_name][key] = list(all_tables[table_name][key])
    for workflow_name in all_workflows:
        for key in all_workflows[workflow_name]:
            if isinstance(all_workflows[workflow_name][key], set):
                all_workflows[workflow_name][key] = list(all_workflows[workflow_name][key])
    return all_tables, all_workflows


if __name__ == '__main__':
    t = time()
    tables: Dict = {}
    workflows: Dict = {}
    try:
        gen = parse_workflows_coroutine('/home/shared/PycharmProjects/parsing_tool/workflow')
        while True:
            progress: int = next(gen)
            print(f'Progress {progress}/100%')
    except StopIteration as ret:
        tables, workflows = ret.value
    print(time() - t)

    with open('result.json', 'w') as file:
        json.dump({
            'workflows': workflows,
            'tables': tables
        }, file)
