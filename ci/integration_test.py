#!/usr/bin/env python3
"""
Integration tests for xoverrr with real databases
"""

import sys, os
import time
import logging
from sqlalchemy import create_engine

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from core import DataQualityComparator, DataReference
from models import COMPARISON_SUCCESS, COMPARISON_FAILED

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class IntegrationTest:
    def __init__(self):
        self.source_engine = None
        self.target_engine = None
        self.comparator = None
    
    def setup_databases(self):
        """Подключение к тестовым базам"""
        logger.info("Setting up database connections...")
        
        self.source_engine = create_engine(
            "postgresql://myuser@localhost:5433/postgres"
        )
        
        self.target_engine = create_engine(
            "postgresql://myuser@localhost:5433/postgres"
        )
        
        self.comparator = DataQualityComparator(
            source_engine=self.source_engine,
            target_engine=self.target_engine,
            timezone='UTC'
        )
        
        logger.info("Database pool established")
    
    def wait_for_databases(self, timeout=60):
        """Ожидание готовности баз данных"""
        logger.info("Waiting for databases to be ready...")
        
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                with self.source_engine.connect() as conn:
                    conn.execute("SELECT 1")
                with self.target_engine.connect() as conn:
                    conn.execute("SELECT 1")
                logger.info("All databases are ready!")
                return True
            except Exception as e:
                logger.info(f"Databases not ready yet: {e}")
                time.sleep(5)
        
        raise Exception("Databases not ready within timeout")
    
    def test_compare_sample_users(self):
        logger.info("Testing compare_sample for tables...")
        
        source_table = DataReference("public", "t1")
        target_table = DataReference("public", "t2")
        
        status, report, stats, details = self.comparator.compare_sample(
            source_table=source_table,
            target_table=target_table,
            date_column="created_at",
            date_range=("2024-01-01", "2028-01-31"),
            tolerance_percentage=0.0,  # Нулевая толерантность для теста
            exclude_recent_hours=0.0001,
            max_examples=3
        )
        
        logger.info(f"Users comparison status: {status}")
        if report:
            logger.info(f"Report:\n{report}")
        
        assert status == COMPARISON_SUCCESS, f"Users comparison failed: {status}"
        logger.info("✓ Users table comparison passed")
    
    def test_compare_sample_products(self):
        """Тест сравнения таблицы products"""
        logger.info("Testing compare_sample for products table...")
        
        source_table = DataReference("products", "test_schema")
        target_table = DataReference("products", "test_db")
        
        status, report, stats, details = self.comparator.compare_sample(
            source_table=source_table,
            target_table=target_table,
            date_column="created_at",
            custom_primary_key=["product_id"],
            tolerance_percentage=0.0,
            max_examples=2
        )
        
        logger.info(f"Products comparison status: {status}")
        if report:
            logger.info(f"Report:\n{report}")
        
        assert status == COMPARISON_SUCCESS, f"Products comparison failed: {status}"
        logger.info("✓ Products table comparison passed")
    
    def test_count_comparison(self):
        """Тест сравнения счетчиков"""
        logger.info("Testing count comparison...")
        
        source_table = DataReference("users", "test_schema")
        target_table = DataReference("users", "test_db")
        
        status, report, stats, details = self.comparator.compare_counts(
            source_table=source_table,
            target_table=target_table,
            date_column="created_at",
            date_range=("2024-01-01", "2024-01-31"),
            tolerance_percentage=0.0,
            max_examples=5
        )
        
        logger.info(f"Count comparison status: {status}")
        if report:
            logger.info(f"Report:\n{report}")
        
        assert status == COMPARISON_SUCCESS, f"Count comparison failed: {status}"
        logger.info("✓ Count comparison passed")
    
    def run_all_tests(self):
        """Запуск всех тестов"""
        try:
            self.setup_databases()
            self.wait_for_databases()
            
            self.test_compare_sample_users()
            
            logger.info("All integration tests passed!")
            return True
            
        except Exception as e:
            logger.error("Integration test failed: {e}")
            return False

if __name__ == "__main__":
    test_runner = IntegrationTest()
    success = test_runner.run_all_tests()
    exit(0 if success else 1)