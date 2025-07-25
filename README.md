# ETL DBF Oracle Pipeline

A comprehensive, modular ETL pipeline for Oracle databases with multi-format support and configuration-driven operations. Handles CSV, DBF, XLSX, and Oracle table sources with automatic schema management, type mapping, and constraint creation.

## ✨ Key Features

- **🔄 Multi-Format Support**: CSV, DBF, XLSX ↔ Oracle Database, DBF, XLSX
- **⚙️ Configuration-Driven**: YAML-based table definitions with validation
- **🚀 High Performance**: Built on Polars for fast data processing
- **🛡️ Enterprise Ready**: Transaction support, constraint management, comprehensive error handling
- **🔧 Modular Architecture**: Clean separation of concerns for maintainability
- **📊 Advanced Processing**: Automatic type conversion, column sanitization, batch processing

## 🏗️ Architecture

Refactored modular design with clear separation of concerns:

```
etl_dbf_oracle/
├── config/              # Configuration management
│   └── table_config.py  # TableConfig class with YAML support
├── database/            # Database layer
│   ├── connection.py    # Oracle connection management
│   ├── schema.py        # DDL generation, type mapping
│   └── operations.py    # CRUD operations, constraints
├── etl/                 # ETL pipeline components
│   ├── extractors.py    # Multi-format data extraction
│   ├── transformers.py  # Data cleaning and sanitization
│   ├── loaders.py       # Data loading with batch processing
│   └── pipeline.py      # OracleETL orchestration
└── utils/               # Utilities
    └── helpers.py       # Validation, column sanitization
```

## 🚀 Quick Start

### 1. Installation

```bash
# Install with uv (recommended)
uv sync

# Or with pip
pip install -e .
```

### 2. Environment Setup

Create `.env` file:
```env
ORACLE_USER=your_username
ORACLE_PASSWORD=your_password
ORACLE_HOST=localhost
ORACLE_PORT=1521
ORACLE_SERVICE_NAME=XEPDB1
```

### 3. Configuration

Create `config/table_config.yaml`:
```yaml
tables:
  # DBF to Oracle example
  legacy_data:
    source_type: dbf
    source_file_path: data/legacy.dbf
    target_type: oracle
    target_table: LEGACY_DATA
    primary_key: [ID]
    batch_size: 1000
    drop_if_exists: true

  # CSV to Oracle with constraints
  products:
    source_type: csv
    source_file_path: data/products.csv
    target_type: oracle
    target_table: PRODUCTS
    primary_key: [PRODUCT_ID]
    foreign_keys:
      - columns: [CATEGORY_ID]
        references_table: CATEGORIES
        references_columns: [ID]
    indexes:
      - name: IDX_PRODUCT_NAME
        columns: [NAME]
        unique: false
```

### 4. Run Pipeline

```bash
# Simple execution
uv run python main.py

# With debug logging
LOG_LEVEL=DEBUG uv run python main.py
```

Or programmatically:
```python
from etl_dbf_oracle import OracleETL

# Initialize and run
etl = OracleETL.from_env_file('.env')
results = etl.run_multiple_tables('config/table_config.yaml', 'data/')

# Print summary
print(etl.get_pipeline_summary(results))
```

## 📋 Configuration Reference

### Complete Table Configuration

```yaml
tables:
  table_name:
    # Source Configuration
    source_type: csv|dbf|xlsx|table|custom_query
    source_file_path: path/to/file.csv      # For file sources
    source_table: SOURCE_TABLE              # For table sources
    source_schema: SCHEMA_NAME               # Optional for table sources
    
    # Target Configuration
    target_type: oracle|dbf|xlsx
    target_table: TARGET_TABLE               # Always required
    target_file_path: path/to/output.dbf     # For file targets
    
    # Processing Options
    batch_size: 1000                         # Default: 1000
    drop_if_exists: true                     # Default: true
    
    # Database Constraints (Oracle only)
    primary_key: [COLUMN1, COLUMN2]
    foreign_keys:
      - columns: [FK_COLUMN]
        references_table: REF_TABLE
        references_columns: [REF_COLUMN]
    indexes:
      - name: IDX_NAME
        columns: [COLUMN1, COLUMN2]
        unique: false
    
    # Format-Specific Options
    csv_options:
      infer_schema_length: 10000
      null_values: ['', 'NULL', 'N/A']
      try_parse_dates: true
      
    dbf_options: {}
    
    xlsx_options:
      sheet_name: Sheet1
      header: 0
      skip_rows: 0
```

## 🔧 Supported Data Sources & Targets

### Sources
- **CSV Files**: High-performance reading with Polars
- **DBF Files**: Legacy dBase file support
- **XLSX Files**: Excel spreadsheet support with sheet selection
- **Oracle Tables**: Direct table-to-table transfers
- **Custom SQL Queries**: Complex transformations with DuckDB

### Targets
- **Oracle Database**: Full schema management with constraints
- **DBF Files**: Legacy format export
- **XLSX Files**: Modern Excel output

## 🎯 Advanced Features

### Custom SQL Queries

```yaml
sales_analysis:
  source_type: custom_query
  custom_query: sql/sales_report.sql
  target_type: oracle
  target_table: SALES_ANALYSIS
  data_source:
    type: csv
    file_path: data/sales.csv
    table_alias: sales_data
    options:
      infer_schema_length: 10000
```

SQL file (`sql/sales_report.sql`):
```sql
SELECT 
    region,
    product_category,
    SUM(amount) as total_sales,
    COUNT(*) as transaction_count
FROM sales_data
WHERE sale_date >= '2025-01-01'
GROUP BY region, product_category
```

### Programmatic Usage

```python
from etl_dbf_oracle import OracleETL, TableConfig

# Single table processing
config = TableConfig({
    'source_type': 'dbf',
    'source_file_path': 'legacy/data.dbf',
    'target_type': 'oracle',
    'target_table': 'MIGRATED_DATA',
    'primary_key': ['ID'],
    'batch_size': 500
})

etl = OracleETL.from_env_file('.env')
result = etl.run_etl_for_table(config)

print(f"Migrated {result['total_rows']} rows")
print(f"Column mapping: {result['column_mapping']}")

# Context manager for automatic connection management
with OracleETL.from_env_file('.env') as etl:
    results = etl.run_multiple_tables('config/tables.yaml')
    for table, result in results.items():
        if result.get('error'):
            print(f"❌ {table}: {result['error']}")
        else:
            print(f"✅ {table}: {result['total_rows']:,} rows")
```

## 🔄 Data Type Mapping

Automatic type conversion between formats:

| Source Type | Oracle Type  | DBF Type | XLSX Type |
|-------------|--------------|----------|-----------|
| Integer     | NUMBER(19,0) | N(10,0)  | Integer   |
| Float       | NUMBER       | N(15,2)  | Float     |
| String      | VARCHAR2     | C(width) | Text      |
| Boolean     | NUMBER(1,0)  | L        | Boolean   |
| Date        | DATE         | D        | Date      |
| DateTime    | TIMESTAMP    | D        | DateTime  |

## 🛠️ Development Commands

```bash
# Check package structure
uv tree

# Test imports
uv run python -c "from etl_dbf_oracle import TableConfig, OracleETL; print('✓ Package loaded')"

# Run with specific configuration
uv run python main.py

# Debug mode
LOG_LEVEL=DEBUG uv run python main.py
```

## 🐛 Troubleshooting

### Common Issues

**Oracle Connection Errors**
```bash
# Verify environment variables
cat .env

# Test connection
uv run python -c "from etl_dbf_oracle import OracleETL; etl = OracleETL.from_env_file('.env'); print('✓ Connected' if etl.test_connection() else '❌ Failed')"
```

**File Not Found Errors**
- Check file paths in configuration are correct
- Ensure data directory structure matches configuration
- Use absolute paths if relative paths cause issues

**Type Conversion Issues**
- Enable debug logging: `LOG_LEVEL=DEBUG`
- Check null value handling in format-specific options
- Review column sanitization in transformation logs

**Memory Issues with Large Files**
- Reduce `batch_size` in configuration
- Process files in smaller chunks
- Monitor system memory usage

### Debug Logging

```python
import logging
logging.basicConfig(level=logging.DEBUG)

# Your ETL code with detailed logging
etl = OracleETL.from_env_file('.env')
results = etl.run_multiple_tables('config/tables.yaml')
```

## 📊 Performance Considerations

- **Batch Size**: Balance between memory usage and performance (default: 1000)
- **Memory Management**: Polars uses lazy evaluation for large datasets
- **Database Connections**: Connection pooling for multiple table processing
- **Indexing**: Create appropriate indexes for foreign key columns

## 🤝 Contributing

The modular architecture makes contributions straightforward:

1. **Extractors** (`etl/extractors.py`): Add new data source types
2. **Transformers** (`etl/transformers.py`): Add data cleaning logic
3. **Loaders** (`etl/loaders.py`): Add new target formats
4. **Schema** (`database/schema.py`): Extend type mapping

## 📄 License

MIT License - see LICENSE file for details.

---

**🚀 Built with modern Python tooling: Polars, Oracle DB, Pydantic validation, and comprehensive error handling.**