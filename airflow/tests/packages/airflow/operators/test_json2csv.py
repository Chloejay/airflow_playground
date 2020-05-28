import csv, json
from pathlib import Path 

from airflow.operators.json2csv import JsonToCsvOperator 

# use tmp_path for testing purpose 
def test_json_to_csv_operator(tmp_path:Path):
    input_path = tmp_path/"input.json"
    output_path= tmp_path/"output.csv"

    input_data =[
        {"name":"chloe","age":"28",:"sex":"F"},
        {"name":"nat","age":"28",:"sex":" M"},
        {"name":"emma","age":"25",:"sex":"F"},
    ]

    with open (input_data, 'w') as f:
        f.write(json.dumps(input_data))

    task = JsonToCsvOperator(task_id="test", input_path= input_path, output_path=output_path)
    # execute operator 
    task.execute(context={})

    with open(output_path, "r") as f:
        reader= csv.DictReader(f) 
        result =[dict(row) for row in reader] 

    assert result = input_data 

