from typing import List, Tuple, Dict
import shutil
import os

import gviz_api

CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))


# Adapted from: https://developers.google.com/chart/interactive/docs/gallery/treemap
def get_treemap(description: List[Tuple[str, str]], data: Dict[str, List[Tuple]], dir_path: str):
    shutil.copy(os.path.join(CURRENT_DIR, 'template.html'), os.path.join(dir_path, 'index.html'))

    for name, data in data.items():
        data_table = gviz_api.DataTable(description, data)
        json_data = data_table.ToJSon()

        with open(os.path.join(dir_path, name + '.json'), 'w') as f:
            f.write(json_data)
