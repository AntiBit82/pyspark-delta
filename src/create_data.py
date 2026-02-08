import logging
from pyspark.sql import SparkSession
from spark_init import init_spark

def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)

    spark = init_spark("CreateData")

    # DEPARTMENTS
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

    #  EMPLOYEES
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

    # PROJECTS
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

    # EMPLOYEE-PROJECT ASSIGNMENTS
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
    
    spark.stop()

if __name__ == "__main__":
    main()