
import sys, os
import unittest
import pandas as pd
import numpy as np
import time
from utils import (
    compare_dataframes,
    prepare_dataframe,
    cross_fill_missing_dates,
    ComparisonStats,
    ComparisonDiffDetails,
    validate_dataframe_size,
    get_dataframe_size_gb
)


sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

class TestUtils(unittest.TestCase):
    
    def test_prepare_dataframe_basic(self):
        """Test basic dataframe preparation with null handling"""
        df = pd.DataFrame({
            'col1': [1, 2, np.nan, 4],
            'col2': ['a', ' ', None, 'd'],
            'col3': [1.0, 2.5, 3.0, 4.0]
        })
        
        result = prepare_dataframe(df)
        
        self.assertEqual(result.shape, df.shape)
        self.assertEqual(result['col1'].iloc[2], 'N/A')
        self.assertEqual(result['col2'].iloc[1], 'N/A')
        self.assertEqual(result['col2'].iloc[2], 'N/A')
        self.assertTrue(all(result.dtypes == 'object'))

    def test_compare_dataframes_identical(self):
        """Test comparison of identical dataframes"""
        df1 = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie'],
            'age': [25, 30, 35]
        })
        
        df2 = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie'],
            'age': [25, 30, 35]
        })
        
        stats, details = compare_dataframes(df1, df2, ['id'], 3)
        
        self.assertEqual(stats.total_source_rows, 3)
        self.assertEqual(stats.total_target_rows, 3)
        self.assertEqual(stats.common_pk_rows, 3)
        self.assertEqual(stats.total_matched_rows, 3)
        self.assertEqual(stats.final_diff_score, 0.0)

    def test_compare_dataframes_different_values(self):
        """Test comparison with different values"""
        df1 = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie'],
            'age': [25, 30, 35]
        })
        
        df2 = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['Alice', 'Robert', 'Charlie'],
            'age': [25, 31, 35]
        })
        
        stats, details = compare_dataframes(df1, df2, ['id'], 3)
        
        self.assertEqual(stats.common_pk_rows, 3)
        self.assertGreater(stats.final_diff_score, 0.0)
        self.assertEqual(len(details.discrepancies_per_col_examples), 2)

    def test_compare_dataframes_different_keys(self):
        """Test comparison with different primary keys"""
        df1 = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie']
        })
        
        df2 = pd.DataFrame({
            'id': [1, 2, 4],
            'name': ['Alice', 'Bob', 'David']
        })
        
        stats, details = compare_dataframes(df1, df2, ['id'], 3)
        
        self.assertEqual(stats.only_source_rows, 1)
        self.assertEqual(stats.only_target_rows, 1)
        self.assertEqual(stats.common_pk_rows, 2)

    def test_compare_dataframes_empty(self):
        """Test comparison with empty dataframes"""
        df1 = pd.DataFrame({'id': [], 'name': []})
        df2 = pd.DataFrame({'id': [], 'name': []})
        
        stats, details = compare_dataframes(df1, df2, ['id'], 3)
        self.assertIsNone(stats)
        self.assertIsNone(details)

    def test_compare_dataframes_missing_columns(self):
        """Test comparison with missing key columns"""
        df1 = pd.DataFrame({'id': [1], 'name': ['Alice']})
        df2 = pd.DataFrame({'name': ['Alice']})  # Missing id column
        
        with self.assertRaises(ValueError):
            compare_dataframes(df1, df2, ['id'], 3)

    def test_cross_fill_missing_dates(self):
        """Test cross-filling missing dates"""
        df1 = pd.DataFrame({
            'dt': pd.to_datetime(['2023-01-01', '2023-01-02']),
            'cnt': [10, 20]
        })
        
        df2 = pd.DataFrame({
            'dt': pd.to_datetime(['2023-01-02', '2023-01-03']),
            'cnt': [15, 25]
        })
        
        result1, result2 = cross_fill_missing_dates(df1, df2)
        
        self.assertEqual(len(result1), 3)
        self.assertEqual(len(result2), 3)
        self.assertEqual(result1['cnt'].sum(), 30)
        self.assertEqual(result2['cnt'].sum(), 40)


    def test_get_dataframe_size_gb(self):
        """Test dataframe size calculation"""
        df = pd.DataFrame({'col': range(1000)})
        size_gb = get_dataframe_size_gb(df)
        
        self.assertGreater(size_gb, 0.0)
        self.assertLess(size_gb, 0.1)

    def test_performance_small_dataframe(self):
        """Performance test for small dataframes"""
        n_records = 100000
        
        df1 = pd.DataFrame({
            'id': range(n_records),
            'value': [f'text_{i}' for i in range(n_records)],
            'value2': [f'text_{i}' for i in range(n_records)],
            'value3': [f'text_{i}' for i in range(n_records)],
            'value4': [f'text_{i}' for i in range(n_records)],
            'value5': [f'text_{i}' for i in range(n_records)],
            'value6': [f'text_{i}' for i in range(n_records)],
            'value7': [f'text_{i}' for i in range(n_records)],
            'value8': [f'text_{i}' for i in range(n_records)],
            'value9': [f'text_{i}' for i in range(n_records)],
            'value10': [f'text_{i}' for i in range(n_records)],
        })
        
        df2 = df1.copy()
        # Modify a few records
        df2.loc[10:15, 'value'] = 'modified'
        
        start_time = time.time()
        stats, details = compare_dataframes(df1, df2, ['id'])
        execution_time = time.time() - start_time
        
        self.assertLess(execution_time, 1.0)  # Should complete in <1 second
        self.assertGreater(stats.final_diff_score, 0.0)

    def test_performance_medium_dataframe(self):
        n_records = 1000*1000
        

        df1 = pd.DataFrame({
            'id': range(1, n_records +1),
            'int_col': 1,# np.random.randint(1, 100, n_records),
            'float_col': np.random.rand(n_records),
            'str_col': [f'text_{i}' for i in range(n_records)],
            'str_col2': [f'text_a_{i}' for i in range(n_records)],
            'str_col3': [f'text_b_{i}' for i in range(n_records)],
            'str_col4': [f'text_c_{i}' for i in range(n_records)],
            'str_col5': [f'text_d_{i}' for i in range(n_records)],
            'str_col6': [f'text_d_{i}' for i in range(n_records)],
            'bool_col': np.random.choice([True, False], n_records)
        })
        print(df1['id'].min())
        print(df1['id'].max())
        df2 = df1.copy()

        
        # Change few records
        k = 100
        #k unique indexes
        change_indices = np.random.choice(np.arange(1, n_records), size=k, replace=False)
        for idx in change_indices:
            df2.loc[idx, 'float_col'] += 0.1
        df2.loc[change_indices[0],'str_col']  = 'pink_floyd'

        #add one record df2
        new_record = {
            'id': n_records + 1,
            'int_col': 100500,
            'float_col': -0.42,
            'str_col': 'limp_bizkit',
            'str_col2': 'alice_cooper',
            'str_col3': 'rammstein',
            'str_col4': 'him',
            'str_col5': 'nine inch nails',
            'str_col6': 'prodigy',
            'bool_col': True,
        }
        df2 = pd.concat([df2, pd.DataFrame([new_record])], ignore_index=True)    

        print(f"Memory df1: {df1.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB")
        print(f"Memory df2: {df2.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB")    

        start_time = time.time()
        stats, details = compare_dataframes(df1, df2, ['id'])
        execution_time = time.time() - start_time
        print(f'{execution_time=}')
        self.assertLess(execution_time, 5.0)  # Should complete in <1 second
        self.assertGreater(stats.final_diff_score, 0.0)

    def test_edge_case_all_different(self):
        """Test edge case where all records are different"""
        df1 = pd.DataFrame({
            'id': [1, 2, 3],
            'value': ['a', 'b', 'c']
        })
        
        df2 = pd.DataFrame({
            'id': [4, 5, 6],
            'value': ['x', 'y', 'z']
        })
        
        stats, details = compare_dataframes(df1, df2, ['id'], 3)
        
        self.assertEqual(stats.only_source_rows, 3)
        self.assertEqual(stats.only_target_rows, 3)
        self.assertEqual(stats.common_pk_rows, 0)
        self.assertEqual(stats.final_diff_score, 100.0)

    def test_edge_case_complete_match(self):
        """Test edge case where everything matches perfectly"""
        df1 = pd.DataFrame({
            'id': [1, 2, 3],
            'value': ['a', 'b', 'c']
        })
        
        df2 = df1.copy()
        
        stats, details = compare_dataframes(df1, df2, ['id'], 3)
        
        self.assertEqual(stats.final_diff_score, 0.0)
        self.assertEqual(stats.total_matched_rows, 3)

    def test_compound_primary_key(self):
        """Test comparison with compound primary key"""
        df1 = pd.DataFrame({
            'id1': [1, 1, 2],
            'id2': ['a', 'b', 'a'],
            'value': [10, 20, 30]
        })
        
        df2 = pd.DataFrame({
            'id1': [1, 2, 2],
            'id2': ['a', 'a', 'b'],
            'value': [10, 30, 40]
        })
        
        stats, details = compare_dataframes(df1, df2, ['id1', 'id2'], 3)
        
        self.assertEqual(stats.common_pk_rows, 2)  # (1,a) and (2,a)
        self.assertEqual(stats.only_source_rows, 1)  # (1,b)
        self.assertEqual(stats.only_target_rows, 1)  # (2,b)

if __name__ == '__main__':
    unittest.main(verbosity=2)


