"""
Spark session initialization with Delta Lake support.
"""

import os
import sys
import logging
import subprocess

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
    # Configure Python for PySpark workers (important for Windows where python3 may not exist)
    if os.name == 'nt':  # Windows only
        python_executable = subprocess.run(
            [sys.executable, "-c", "import sys; print(sys.executable)"],
            capture_output=True,
            text=True
        ).stdout.strip()
        os.environ["PYSPARK_PYTHON"] = python_executable
        os.environ["PYSPARK_DRIVER_PYTHON"] = python_executable
    
    # Get project root for consistent warehouse and metastore location
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    warehouse_dir = os.path.join(project_root, "spark-warehouse")
    metastore_dir = os.path.join(project_root, "metastore_db")
    
    # Windows-specific: Configure Hadoop environment and native library path
    hadoop_java_options = ""
    if os.name == 'nt':  # Windows only
        hadoop_home = os.path.join(project_root, "hadoop-3.3.6")
        native_dir = os.path.join(hadoop_home, "lib", "native")
        hadoop_home_normalized = hadoop_home.replace("\\", "/")
        native_dir_normalized = native_dir.replace("\\", "/")

        # Export HADOOP_HOME and add bin to PATH so native helpers are discoverable
        if os.path.exists(hadoop_home):
            os.environ["HADOOP_HOME"] = hadoop_home
            os.environ["PATH"] = f"{os.path.join(hadoop_home, 'bin')};{os.environ.get('PATH', '')}"

        # Build JVM options for Hadoop and native libs (java.library.path)
        opts = [f"-Dhadoop.home.dir={hadoop_home_normalized}"]
        if os.path.exists(native_dir):
            opts.append(f"-Djava.library.path={native_dir_normalized}")
        hadoop_java_options = " ".join(opts)

        # Warn if winutils or native libs seem missing—this is often the cause
        winutils = os.path.join(hadoop_home, "bin", "winutils.exe")
        if not os.path.exists(winutils):
            logger.warning("winutils.exe not found at %s; Windows native Hadoop IO may fail", winutils)
    
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
            # Performance tuning for local multi-threaded execution
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.driver.memory", "2g")
            .config("spark.sql.autoBroadcastJoinThreshold", "20m")
        )
        # Add Hadoop options only on Windows
        if hadoop_java_options:
            builder = (
                builder
                .config("spark.driver.extraJavaOptions", hadoop_java_options)
                .config("spark.executor.extraJavaOptions", hadoop_java_options)
            )
        builder = builder.enableHiveSupport()
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
            # Performance tuning for local multi-threaded execution
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.driver.memory", "2g")
            .config("spark.sql.autoBroadcastJoinThreshold", "20m")
        )
        # Add Hadoop options only on Windows
        if hadoop_java_options:
            spark = (
                spark
                .config("spark.driver.extraJavaOptions", hadoop_java_options)
                .config("spark.executor.extraJavaOptions", hadoop_java_options)
            )
        spark = spark.enableHiveSupport().getOrCreate()
        logger.info("Successfully initialized Spark with local JARs")
        if clear_screen:
            os.system('clear' if os.name != 'nt' else 'cls')

        spark.sparkContext.setLogLevel("ERROR")
        return spark
