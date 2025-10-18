import pandas as pd
from typing import Optional, Dict, Callable, List, Tuple, Union
from ..constants import DATE_FORMAT, DATETIME_FORMAT
from .base import BaseDatabaseAdapter, Engine
from ..models import TableReference
from ..exceptions import QueryExecutionError
import time
from ..logger import app_logger

class ClickHouseAdapter(BaseDatabaseAdapter):
    """ClickHouse adapter with parameterized queries"""
    def _execute_query(self, query: Union[str, Tuple[str, Dict]], engine: Engine, timezone: str) -> pd.DataFrame:
        df = None
        tz_set = None
        start_time = time.time()
        app_logger.info('start')

        if timezone:
            tz_set = f"SETTINGS session_timezone = '{timezone}'"
        try:
            if isinstance(query, tuple):
                query, params = query
                if tz_set:
                    query = f'{query} {tz_set}'
                app_logger.info(f'query\n {query}')
                app_logger.info(f'{params=}')
                df = pd.read_sql(query, engine, params=params)
            else:
                if tz_set:
                    query = f'{query} {tz_set}'
                app_logger.info(f'query\n {query}')
                df = pd.read_sql(query, engine)

            execution_time = time.time() - start_time
            app_logger.info(f"Query executed in {execution_time:.2f}s")
            return df

        except Exception as e:
            execution_time = time.time() - start_time
            app_logger.error(f"Query execution failed after {execution_time:.2f}s: {str(e)}")

            raise QueryExecutionError(f"Query failed: {str(e)}")

    def build_metadata_columns_query(self, table_ref: TableReference) -> Tuple[str, Dict]:
        query = """
            SELECT
                name as column_name,
                type as data_type,
                position as column_id
            FROM system.columns
            WHERE database = %(schema)s
            AND table = %(table)s
            ORDER BY position
        """
        params = {'schema': table_ref.schema, 'table': table_ref.name}
        return query, params

    def build_primary_key_query(self, table_ref: TableReference) -> Tuple[str, Dict]:
        query = """
            SELECT name as pk_column_name
            FROM system.columns
            WHERE database = %(schema)s
            AND table = %(table)s
            AND is_in_primary_key = 1
            ORDER BY position
        """
        params = {'schema': table_ref.schema, 'table': table_ref.name}
        return query, params

    def build_count_query(self, table_ref: TableReference, date_column: str,
                         start_date: Optional[str], end_date: Optional[str]) -> Tuple[str, Dict]:
        query = f"""
            SELECT
                toDate({date_column}) as dt,
                count(*) as cnt
            FROM {table_ref.full_name}
            WHERE 1=1
        """
        params = {}


        if start_date:
            query += f" AND {date_column} >= toDate(%(start_date)s)"
            params['start_date'] = start_date
        if end_date:
            query += f" AND {date_column} < toDate(%(end_date)s) + INTERVAL 1 day"
            params['end_date'] = end_date

        query += " GROUP BY dt ORDER BY dt DESC"
        return query, params

    def build_data_query(self, table_ref: TableReference, columns: List[str],
                        date_column: Optional[str], update_column: str,
                        start_date: Optional[str], end_date: Optional[str],
                        exclude_recent_hours: Optional[int] = None) -> Tuple[str, Dict]:
        params = {}
        # Add recent data exclusion flag
        exclusion_condition,  exclusion_params = self._build_exclusion_condition(
            update_column, exclude_recent_hours
        )

        if exclusion_condition:
            columns.append(exclusion_condition)
            params.update(exclusion_params)

        query = f"""
        SELECT {', '.join(columns)}
        FROM {table_ref.full_name}
        WHERE 1=1\n"""

        if start_date and date_column:
            query += f"            AND {date_column} >= toDate(%(start_date)s)\n"
            params['start_date'] = start_date
        if end_date and date_column:
            query += f"            AND {date_column} < toDate(%(end_date)s) + INTERVAL 1 day\n"
            params['end_date'] = end_date

        return query, params

    def _build_exclusion_condition(self, update_column: str,
                                    exclude_recent_hours: int) -> Tuple[str, Dict]:
        """ClickHouse-specific implementation for recent data exclusion"""
        if  update_column and exclude_recent_hours:


            exclude_recent_hours = exclude_recent_hours

            condition = f"""case when {update_column} > (now() - INTERVAL %(exclude_recent_hours)s HOUR) then 'y' end as xrecently_changed"""
            params = {'exclude_recent_hours':  exclude_recent_hours}
            return condition, params

        return None, None

    def _get_type_conversion_rules(self, timezone:str ) -> Dict[str, Callable]:
        return {
            r'datetime\(': lambda x: pd.to_datetime(x, errors='coerce').dt.tz_convert(timezone).dt.tz_localize(None).strftime(DATETIME_FORMAT).str.replace(r'\s00:00:00$', '', regex=True),
            r'datetime64': lambda x: pd.to_datetime(x, errors='coerce').dt.tz_convert(timezone).dt.tz_localize(None).strftime(DATETIME_FORMAT).str.replace(r'\s00:00:00$', '', regex=True),
            r'date': lambda x: pd.to_datetime(x, errors='coerce').dt.strftime(DATE_FORMAT).str.replace(r'\s00:00:00$', '', regex=True),
            r'uint64|uint8|float|decimal': lambda x: x.astype(str).str.replace(r'\.0+$', '', regex=True),
        }