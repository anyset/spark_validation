from dynaconf import Dynaconf
from schemas.utils.profiler import SchemaProfiler
from schemas.utils.openapi import resolve_schema
from schemas.generators import records


def questions():
    choice = int(
        input(
            """
Please select an action
1 - Generate data classes from an open api schema
2 - Generate expectations from an open api schema
"""
        )
    )

    if choice == 1:
        write_classes_for_schema()

    if choice == 2:
        file_path = (
            input(
                """which schema file ? (default : 'label/resources/schemas/dicom_modular.yml')
"""
            )
            or "label/resources/schemas/dicom_modular.yml"
        )
        schema_name = (
            input(
                """which schema object ? (default : 'Dicom')
"""
            )
            or "Dicom"
        )
        suite_name = (
            input(
                f"""which expectations suite ? (default : '{schema_name}')
"""
            )
            or f"{schema_name}"
        )
        write_expectations(suite_name, schema_name, file_path)


def write_expectations(suite_name, schema_name, file_path):
    mapping = resolve_schema(Dynaconf(settings_files=file_path))
    schema = mapping.components.schemas[schema_name]
    profiler = SchemaProfiler(schema, suite_name)
    print(f"Exporting suite {suite_name}...")
    profiler.export("label/resources/expectations")
    print("Done.")


def write_classes_for_schema():
    dicom_related_schemas = Dynaconf(
        settings_files="resources/schemas/dicom_modular.yml"
    )
    resolved_schemas = resolve_schema(dicom_related_schemas)

    records.write_file(
        resolved_schemas.components.schemas, "label/schemas/generated/records.py"
    )
    print("Done.")
    pass


questions()
