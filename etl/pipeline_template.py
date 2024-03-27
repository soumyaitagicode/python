import json

def execute_etl_pipeline(pipeline_config):
    # Implement ETL logic here
    print("Executing ETL Pipeline...")
    print(json.dumps(pipeline_config, indent=2))

if __name__ == "__main__":
    with open("pipelines/sample_pipeline.json") as file:
        pipeline_config = json.load(file)
        execute_etl_pipeline(pipeline_config)
