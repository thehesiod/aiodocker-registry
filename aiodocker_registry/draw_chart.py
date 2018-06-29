from typing import List, Tuple

import gviz_api


# Adapted from: https://developers.google.com/chart/interactive/docs/gallery/treemap
def _generate_html(json_data):
    return """
<html>
  <head>
  <title>TreeMap</title>
    <script type="text/javascript" src="https://www.gstatic.com/charts/loader.js"></script>
    <script>
      google.charts.load('current', {'packages':['treemap']});
      google.charts.setOnLoadCallback(drawChart);
      function drawChart() {
        var json_table = new google.visualization.TreeMap(document.getElementById('chart_div'));

        var data = new google.visualization.DataTable(""" + json_data + """);        
        json_table.draw(data);
      }
    </script>
  </head>
  <body>
    <div id="chart_div" style="width: 100%; height: 100%;"></div>
  </body>
</html>
"""


def get_treemap(description: List[Tuple[str, str]], data: List[Tuple]):
    # # Loading it into gviz_api.DataTable
    data_table = gviz_api.DataTable(description, data)
    #
    # # Creating a JSON string
    json_data = data_table.ToJSon()

    # Putting the JS code and JSon string into the template
    return _generate_html(json_data)


def print_example():
    description = [
        ('Location', 'string'),
        ('Parent', 'string'),
        ('Market trade volume (size)', 'number'),
        ('Market increase/decrease (color)', 'number')
    ]

    data = [
        ['Global', None, 0, 0],
        ['America', 'Global', 0, 0],
        ['Europe', 'Global', 0, 0],
        ['Asia', 'Global', 0, 0],
        ['Australia', 'Global', 0, 0],
        ['Africa', 'Global', 0, 0],
        ['Brazil', 'America', 11, 10],
        ['USA', 'America', 52, 31],
        ['Mexico', 'America', 24, 12],
        ['Canada', 'America', 16, -23],
        ['France', 'Europe', 42, -11],
        ['Germany', 'Europe', 31, -2],
        ['Sweden', 'Europe', 22, -13],
        ['Italy', 'Europe', 17, 4],
        ['UK', 'Europe', 21, -5],
        ['China', 'Asia', 36, 4],
        ['Japan', 'Asia', 20, -12],
        ['India', 'Asia', 40, 63],
        ['Laos', 'Asia', 4, 34],
        ['Mongolia', 'Asia', 1, -5],
        ['Israel', 'Asia', 12, 24],
        ['Iran', 'Asia', 18, 13],
        ['Pakistan', 'Asia', 11, -52],
        ['Egypt', 'Africa', 21, 0],
        ['S. Africa', 'Africa', 30, 43],
        ['Sudan', 'Africa', 12, 2],
        ['Congo', 'Africa', 10, 12],
        ['Zaire', 'Africa', 8, 10]
    ]

    print(get_treemap(description, data))


if __name__ == "__main__":
    print_example()
