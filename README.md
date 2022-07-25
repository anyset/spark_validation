### Spark data validation tool

### Usage

- Build expectations from OpenAPI schema files
    ```shell
    schemas/scripts/launch.py
    ```

- validate pyspark dataframes from expectations

    ```pycon
    expectations = json.load(file)
    validator = Validator(expectations)
    validator.validate_all(dataframe)
    ```
