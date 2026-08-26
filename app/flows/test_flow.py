from prefect import flow, task


@task
def extract_data():
    print("Extracting data...")
    return [1, 2, 3, 4, 5]


@task
def transform_data(data):
    print("Transforming data...")
    return [item * 2 for item in data]


@task
def load_data(data):
    print(f"Loading data: {data}")


@flow
def test_pipeline():
    data = extract_data()
    transformed_data = transform_data(data)
    load_data(transformed_data)