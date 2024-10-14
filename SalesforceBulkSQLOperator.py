from airflow.providers.salesforce.operators.bulk import SalesforceBulkOperator
from airflow.providers.common.sql.hooks.sql_hook import SqlHook


class SalesforceBulkSQLOperator(SalesforceBulkOperator):
    """
    Executes a SQL query and inserts the results into Salesforce using bulk operations.
    Columns in the SQL query are expected to match 1:1 to their corresponding API fields in Salesforce.
    
    Args:
        sql_conn_id: The ID of the SQL connection to use.
        sql: The SQL query to execute.
        object_name: The name of the Salesforce object to insert into.
        external_id_field: The name of the external ID field in the Salesforce object.
        batch_size: The number of records to include in each batch.
        use_serial: Whether to use serial mode for the bulk operation.
        skip_if_empty: If True, skip execution and log if no records are found. If False, raise an error when no records are found.
        **kwargs: Additional arguments passed to the parent BaseOperator.
    """

    def __init__(
        self,
        sql_conn_id: str,
        sql: str,
        object_name: str,
        external_id_field: str,
        batch_size: int = 10000,
        use_serial: bool = False,
        skip_if_empty: bool = True,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.sql_conn_id = sql_conn_id
        self.sql = sql
        self.object_name = object_name
        self.external_id_field = external_id_field
        self.batch_size = batch_size
        self.use_serial = use_serial
        self.skip_if_empty = skip_if_empty

    def execute(self, context):
        # Get SQL hook and execute query
        sql_hook = SqlHook(self.sql_conn_id)
        records = sql_hook.get_records(self.sql)

        # Create payload from query results, mapping fields by name
        payload = []
        for row in records:
            record = {}
            for column, value in zip(self.sql.split(','), row):
                record[column.strip()] = value
            payload.append(record)

        # Check if records exist
        if not payload:
            if self.skip_if_empty:
                self.log.info("No records found in the SQL query. Salesforce bulk insert will not be executed.")
                return  # Exit gracefully if no records are found
            else:
                error_message = "No records found in the SQL query, but skip_if_empty is set to False. Raising an error."
                self.log.error(error_message)
                raise ValueError(error_message)  # Raise an error if skip_if_empty is False

        self.log.info(f"Preparing to insert {len(payload)} records into Salesforce {self.object_name}.")

        # Execute Salesforce bulk insert
        bulk_operator = SalesforceBulkOperator(
            task_id=self.task_id,  # Can be omitted
            operation="insert",
            object_name=self.object_name,
            payload=payload,
            external_id_field=self.external_id_field,
            batch_size=self.batch_size,
            use_serial=self.use_serial,
        )

        # Error handling during bulk operation
        try:
            bulk_operator.execute(context)
            self.log.info(f"Successfully inserted {len(payload)} records into Salesforce {self.object_name}.")
        except Exception as e:
            self.log.error(f"Salesforce bulk insert failed: {e}")
            raise

