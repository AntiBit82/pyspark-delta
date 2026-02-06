"""
Simple example demonstrating PySpark with Delta Lake.

This example shows:
1. Creating a Spark session with Delta Lake support
2. Creating multiple DataFrames (employees, departments, projects, assignments)
3. Writing data to Delta tables as unmanaged tables
4. Reading data back and performing various joins
"""

import logging
from pyspark.sql.functions import col
from spark_init import init_spark

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def main():
    # Create Spark session with Delta Lake support
    logger.info("Creating Spark session...")
    spark = init_spark("SimpleDeltaExample")
    
    # Create departments data
    logger.info("Creating departments data...")
    departments_data = [
        (1, "Engineering", "Berlin"),
        (2, "Sales", "Amsterdam"),
        (3, "Marketing", "Barcelona"),
        (4, "HR", "Paris"),
    ]
    departments_df = spark.createDataFrame(departments_data, ["id", "dept_name", "location"])
    
    logger.info("Departments:")
    departments_df.show()
    
    # Create employees data
    logger.info("Creating employees data...")
    employees_data = [
        (1, "Lars Müller", 1, 95000),
        (2, "Sophie Dubois", 1, 87000),
        (3, "Antonio Rossi", 2, 78000),
        (4, "Maria García", 2, 82000),
        (5, "Jan Kowalski", 3, 71000),
        (6, "Emma Andersson", 1, 102000),
        (7, "Dimitris Papadopoulos", 4, 68000),
    ]
    employees_df = spark.createDataFrame(employees_data, ["id", "name", "dept_id", "salary"])
    
    logger.info("Employees:")
    employees_df.show()
    
    # Create projects data
    logger.info("Creating projects data...")
    projects_data = [
        (101, "Cloud Migration", 1),
        (102, "CRM Upgrade", 2),
        (103, "Brand Refresh", 3),
        (104, "AI Platform", 1),
        (105, "Market Expansion", 2),
    ]
    projects_df = spark.createDataFrame(projects_data, ["id", "project_name", "dept_id"])
    
    logger.info("Projects:")
    projects_df.show()
    
    # Create employee-project assignments
    logger.info("Creating employee-project assignments...")
    assignments_data = [
        (1, 101),
        (2, 101),
        (2, 102),
        (6, 104),
        (3, 102),
        (4, 105),
        (5, 103),
    ]
    assignments_df = spark.createDataFrame(assignments_data, ["emp_id", "project_id"])
    
    logger.info("Assignments:")
    assignments_df.show()
    
    # Write all tables
    logger.info("Writing Delta tables...")
    departments_df.write.format("delta").mode("overwrite").option("path", "./data/departments").saveAsTable("departments")
    employees_df.write.format("delta").mode("overwrite").option("path", "./data/employees").saveAsTable("employees")
    projects_df.write.format("delta").mode("overwrite").option("path", "./data/projects").saveAsTable("projects")
    assignments_df.write.format("delta").mode("overwrite").option("path", "./data/assignments").saveAsTable("assignments")
    
    # Read back tables
    logger.info("Reading tables...")
    departments_read = spark.read.table("departments")
    employees_read = spark.read.table("employees")
    projects_read = spark.read.table("projects")
    assignments_read = spark.read.table("assignments")
    
    # Example 1: Simple join (employees with departments)
    logger.info("Example 1: Employees with departments:")
    emp_dept = employees_read.join(departments_read, employees_read.dept_id == departments_read.id)
    emp_dept.select(employees_read.id.alias("emp_id"), "name", "dept_name", "location", "salary").show()
    
    # Example 2: Three-way join (employees -> assignments -> projects)
    logger.info("Example 2: Employee project assignments:")
    emp_projects = (employees_read
        .join(assignments_read, employees_read.id == assignments_read.emp_id)
        .join(projects_read, projects_read.id == assignments_read.project_id)
    )
    emp_projects.select(employees_read.id.alias("emp_id"), "name", "project_name").show()
    
    # Example 3: Full join chain (employees -> departments and projects)
    logger.info("Example 3: Full report with all details (left joins to keep all employees):")
    full_report = (employees_read.alias("e")
        .join(departments_read.alias("d"), col("e.dept_id") == col("d.id"))
        .join(assignments_read.alias("a"), col("e.id") == col("a.emp_id"), "left")
        .join(projects_read.alias("p"), col("p.id") == col("a.project_id"), "left")
    )
    full_report.select(
        col("e.id").alias("emp_id"), 
        "e.name", 
        "d.dept_name",
        "p.project_name", 
        "d.location", 
        "e.salary"
    ).show()
    
    # Example 4: Same query using SQL
    logger.info("Example 4: Same full report using SQL:")
    sql_result = spark.sql("""
        SELECT 
            e.id as emp_id,
            e.name,
            d.dept_name,
            p.project_name,
            d.location,
            e.salary
        FROM employees e
        JOIN departments d ON e.dept_id = d.id
        LEFT JOIN assignments a ON e.id = a.emp_id
        LEFT JOIN projects p ON p.id = a.project_id
    """)
    sql_result.show()
    
    logger.info(f"Total employees: {employees_read.count()}")
    logger.info(f"Total departments: {departments_read.count()}")
    logger.info(f"Total projects: {projects_read.count()}")
    logger.info(f"Total assignments: {assignments_read.count()}")
    
    # Stop Spark session
    spark.stop()
    logger.info("Done!")


if __name__ == "__main__":
    main()
 