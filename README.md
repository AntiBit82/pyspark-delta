# PySpark Delta

A simple Python project for working with PySpark and Delta Lake on local file system


## Requirements

- Python 3.11.6 tested
- Java 8 or higher (required by PySpark)


## Installation

Create and activate a virtual environment (recommended):

**macOS/Linux:**
```bash
python3 -m venv .venv
source .venv/bin/activate
```

**Windows (Command Prompt):**
```cmd
python -m venv .venv
.venv\Scripts\activate.bat
```

**Windows (PowerShell):**
```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
```

Install dependencies:
```bash
pip install -r requirements.txt
```

## Quick Start

Activate the virtual environment, then run:
```bash
python src/simple_example.py
```

## What It Does

The example script demonstrates:
- Creating a Spark session with Delta Lake support
- Creating a DataFrame with sample data
- Writing data to a Delta table at `./data/my-table`
- Reading data back from the Delta table

The output will look like:
```
+---+-------+---+
| id|   name|age|
+---+-------+---+
|  1|  Alice| 30|
|  2|    Bob| 35|
|  3|Charlie| 28|
|  4|  Diana| 32|
|  5|    Eve| 29|
+---+-------+---+
```


## Common Issues and Solutions

### Java Not Found

**Error:** `JAVA_HOME is not set`

**Solution:** Install Java 8 or higher and set JAVA_HOME:

**macOS (using Homebrew):**
```bash
brew install openjdk@11
export JAVA_HOME=$(/usr/libexec/java_home -v 11)
```

**Linux:**
```bash
sudo apt-get install openjdk-11-jdk
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
```

**Windows:**
1. Download and install Java from [Oracle](https://www.oracle.com/java/technologies/downloads/) or [AdoptOpenJDK](https://adoptopenjdk.net/)
2. Set JAVA_HOME environment variable to the installation path (e.g., `C:\Program Files\Java\jdk-11`)
3. Add `%JAVA_HOME%\bin` to your PATH
```

### PySpark Memory Issues

**Error:** OutOfMemoryError

**Solution:** Increase executor memory in the script:

```python
.config("spark.executor.memory", "4g")
.config("spark.driver.memory", "4g")
```
