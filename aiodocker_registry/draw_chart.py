from typing import List, Tuple

import gviz_api


# Adapted from: https://developers.google.com/chart/interactive/docs/gallery/treemap
def _generate_html(json_data, redirect: str):
    redirect = "true" if redirect else "false"

    return """
<html>
  <head>
  <title>TreeMap</title>
    <script type="text/javascript" src="https://www.gstatic.com/charts/loader.js"></script>
    <script>
      google.charts.load('current', {'packages':['treemap']});
      google.charts.setOnLoadCallback(drawChart);
      
      var treemap = null;
      var data = null;
      
      function drawChart() {        
        data = new google.visualization.DataTable(""" + json_data + """);        
        treemap = new google.visualization.TreeMap(document.getElementById('chart_div'));
        
        if(""" + redirect + """) {
            google.visualization.events.addListener(treemap, 'select', selectHandler);
        }
        
        treemap.draw(data);
      }
      
      function selectHandler(e) {
          var selectedItem = treemap.getSelection()[0];
          var value = data.getValue(selectedItem.row, 0);
          window.location.href = value + ".html";
      }
    </script>
  </head>
  <body>
    <div id="chart_div" style="width: 100%; height: 100%;"></div>
  </body>
</html>
"""


def get_treemap(description: List[Tuple[str, str]], data: List[Tuple], file_path: str, redirect: bool):
    # # Loading it into gviz_api.DataTable
    data_table = gviz_api.DataTable(description, data)
    #
    # # Creating a JSON string
    json_data = data_table.ToJSon()

    # Putting the JS code and JSon string into the template
    with open(file_path, 'w') as f:
        contents = _generate_html(json_data, redirect)
        f.write(contents)
