import logging
from pyspark.sql.functions import col
from pyspark.errors.exceptions.captured import AnalysisException
from spark_init import init_spark

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def main():
    spark = init_spark("SimpleJoinsExample")

    logger.info("Reading tables...")
    try:
        departments_read = spark.read.table("departments")
    except AnalysisException as e:
        logger.error("Error reading departments table. Did you run create_data.py first?")
        return
    
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
    
    spark.stop()

if __name__ == "__main__":
    main()
 