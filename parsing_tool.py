import os
from typing import List, Dict
from xml.etree.ElementTree import ElementTree


def find_hql_scripts(path_to_workflow_xml: str) -> List:
    with open(path_to_workflow_xml, 'r') as workflow_xml:
        root = ElementTree(file=workflow_xml).getroot()
    for el in root:
        if 'action' in el.tag:
            for el_ in el:
                if 'hive' in el_.tag or 'sqoop' in el_.tag:
                    for el__ in el_:
                        if 'script' in el__.tag:
                            yield el__.text
                            break


def find_tables(path_to_script: str) -> (List, List):
    select_commands: List = []
    ddml_commands: List = []
    with open(path_to_script, 'r') as file:
        while file.readable():
            line: str = file.readline()
            if 'SELECT' in line:
                select_commands.append(line)
            elif 'INSERT' in line or 'UPDATE' in line or 'DELETE' in line or 'ALTER' in line or 'CREATE' in line:
                ddml_commands.append(line)

    return [], []


def parse_workflow(working_dir: str, workflow_name: dir) -> Dict:
    abs_path: str = os.path.join(working_dir, workflow_name, 'workflow.xml')
    created_tables = []
    updated_tables = []
    for script_name in find_hql_scripts(abs_path):
        created, updated = find_tables(os.path.join(working_dir, workflow_name, script_name))


if __name__ == '__main__':
    parse_workflow('/home/shared/PycharmProjects/parsing_tool/workflow', 'prof-impr-dashboard')
