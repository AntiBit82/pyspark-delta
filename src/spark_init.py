"""
Spark session initialization with Delta Lake support.
"""

import os
import sys
import logging
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

logger = logging.getLogger(__name__)


def init_spark(app_name: str = "PySparkDelta", clear_screen: bool = True) -> SparkSession:
    """
    Initialize a Spark session with Delta Lake support.
    
    Tries to use delta-spark package configuration first, falls back to local JARs if that fails.
    
    Args:
        app_name: Name of the Spark application
        clear_screen: Whether to clear terminal after initialization (hides Maven errors)
        
    Returns:
        Configured SparkSession
    """
    # Get project root for consistent warehouse and metastore location
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    warehouse_dir = os.path.join(project_root, "spark-warehouse")
    metastore_dir = os.path.join(project_root, "metastore_db")
    
    # Try standard method first (using delta package)
    try:
        logger.info("Attempting to initialize Spark with delta-spark package...")
        builder = (
            SparkSession.builder
            .appName(app_name)
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.warehouse.dir", warehouse_dir)
            .config("javax.jdo.option.ConnectionURL", f"jdbc:derby:;databaseName={metastore_dir};create=true")
            .enableHiveSupport()
        )
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
        logger.info("Successfully initialized Spark with delta-spark package")
        if clear_screen:
            os.system('clear' if os.name != 'nt' else 'cls')

        spark.sparkContext.setLogLevel("ERROR")
        return spark
    except Exception as e:
        logger.warning("Failed to initialize with delta-spark package, falling back to local JARs...")
        
        # Fallback: Use local JARs
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        jars_dir = os.path.join(project_root, "jars")
        
        if not os.path.exists(jars_dir):
            raise RuntimeError(
                f"Delta initialization failed and no local JARs found at {jars_dir}. "
                "Please download JARs manually or check your network connection."
            )
        
        jar_files = [os.path.join(jars_dir, f) for f in os.listdir(jars_dir) if f.endswith(".jar")]
        
        if not jar_files:
            raise RuntimeError(f"No JAR files found in {jars_dir}")
        
        jars_path = ",".join(jar_files)
        logger.info(f"Using local JARs: {len(jar_files)} files found")
        
        spark = (
            SparkSession.builder
            .appName(app_name)
            .config("spark.jars", jars_path)
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.warehouse.dir", warehouse_dir)
            .config("javax.jdo.option.ConnectionURL", f"jdbc:derby:;databaseName={metastore_dir};create=true")
            .enableHiveSupport()
            .getOrCreate()
        )
        logger.info("Successfully initialized Spark with local JARs")
        if clear_screen:
            os.system('clear' if os.name != 'nt' else 'cls')

        spark.sparkContext.setLogLevel("ERROR")
        return spark
