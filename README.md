# ETL DBF Oracle Pipeline

A comprehensive, configuration-driven ETL pipeline supporting multiple data sources (CSV, DBF, XLSX, Oracle tables) and targets (Oracle database, DBF files, XLSX files) with automatic schema management and type mapping.

## Features

### 🚀 **Multi-Format Support**
- **Sources**: CSV, DBF, XLSX, Oracle tables, Custom SQL queries
- **Targets**: Oracle database, DBF files, XLSX files
- **Automatic type mapping** between different formats
- **Schema inference** and automatic table creation

### 🔧 **Configuration-Driven**
- **YAML-based configuration** for all ETL operations
- **Explicit file paths** - no implicit naming conventions
- **Flexible data source mapping** for custom queries
- **Batch processing** with configurable batch sizes

### 📊 **Advanced Data Processing**
- **Polars-based** high-performance data processing
- **Automatic datetime parsing** with multiple format support
- **Column name sanitization** for Oracle compatibility
- **Case-insensitive column validation**
- **Custom transformations** support

### 🛡️ **Enterprise Ready** 
- **Comprehensive error handling** and validation
- **Detailed logging** with progress tracking
- **Transaction support** for database operations
- **Constraint management** (primary keys, foreign keys, indexes)
- **Data validation** after loading

## Installation

### Prerequisites
- Python 3.13.5+
- Oracle Database (for Oracle targets)
- uv package manager (recommended)

### Install Dependencies

```bash
# Using uv (recommended)
uv sync

# Or using pip
pip install -r requirements.txt
```

### Required Dependencies
- `polars>=1.31.0` - High-performance DataFrame library
- `pandas>=2.0.0` - For XLSX support and data conversion
- `openpyxl>=3.1.5` - Excel file support for pandas
- `oracledb>=3.2.0` - Oracle database connectivity
- `duckdb>=1.3.2` - For custom query execution
- `dbf>=0.99.10` - DBF file support
- `pyyaml>=6.0.2` - YAML configuration parsing
- `python-dotenv>=1.1.1` - Environment variable management

## Quick Start

### 1. Environment Setup

Create a `.env` file with your Oracle connection details:

```env
ORACLE_HOST=localhost
ORACLE_PORT=1521
ORACLE_SERVICE_NAME=XEPDB1
ORACLE_USERNAME=your_username
ORACLE_PASSWORD=your_password
```

### 2. Basic Configuration

Create a `config/tables.yaml` file:

```yaml
tables:
  # CSV to Oracle
  products:
    source_type: csv
    source_file_path: data/products.csv
    target_type: oracle
    target_table: PRODUCTS
    primary_key: [ID]
    batch_size: 1000
    drop_if_exists: true

  # DBF to Oracle
  legacy_customers:
    source_type: dbf
    source_file_path: data/customers.dbf
    target_type: oracle
    target_table: CUSTOMERS
    primary_key: [CUSTOMER_ID]
    
  # XLSX to DBF
  sales_export:
    source_type: xlsx
    source_file_path: data/sales.xlsx
    target_type: dbf
    target_table: SALES_EXPORT
    target_file_path: output/sales_export.dbf
    xlsx_options:
      sheet_name: SalesData
      header: 0
```

### 3. Run ETL Pipeline

```python
from etl_dbf_oracle import OracleETL

# Initialize ETL pipeline
etl = OracleETL.from_env_file('.env')

# Process all tables
results = etl.run_multiple_tables('config/tables.yaml', 'data/')

# Print summary
print(etl.get_pipeline_summary(results))
```

## Configuration Reference

### Table Configuration Structure

```yaml
tables:
  table_name:
    # Source configuration
    source_type: csv|dbf|xlsx|table|custom_query
    source_file_path: path/to/source/file  # Required for file sources
    source_table: SOURCE_TABLE             # Required for table sources
    source_schema: SCHEMA_NAME              # Optional for table sources
    
    # Target configuration  
    target_type: oracle|dbf|xlsx
    target_table: TARGET_TABLE              # Always required
    target_file_path: path/to/target/file   # Required for file targets
    
    # Processing options
    batch_size: 1000                        # Default: 1000
    drop_if_exists: true                    # Default: true
    
    # Database constraints (Oracle only)
    primary_key: [COL1, COL2]
    foreign_keys:
      - columns: [FK_COL]
        references_table: REF_TABLE
        references_columns: [REF_COL]
    indexes:
      - name: IDX_NAME
        columns: [COL1, COL2]
        unique: false
    
    # Format-specific options
    csv_options:
      infer_schema_length: 10000
      null_values: ['', 'NULL', 'N/A']
      
    dbf_options: {}
    
    xlsx_options:
      sheet_name: Sheet1                    # Sheet to read/write
      header: 0                            # Header row (0-indexed)
      skip_rows: 0                         # Rows to skip
```

## Supported Data Sources

### 1. CSV Files
```yaml
source_type: csv
source_file_path: data/products.csv
csv_options:
  infer_schema_length: 10000
  try_parse_dates: false
  null_values: ['', 'NULL', 'N/A']
  ignore_errors: true
```

### 2. DBF Files
```yaml
source_type: dbf
source_file_path: data/legacy.dbf
dbf_options: {}
```

### 3. XLSX Files
```yaml
source_type: xlsx  
source_file_path: data/sales.xlsx
xlsx_options:
  sheet_name: SalesData      # Or use sheet_id: 0
  header: 0                  # Header row
  skip_rows: 0              # Rows to skip
```

### 4. Oracle Tables
```yaml
source_type: table
source_table: EMPLOYEES
source_schema: HR          # Optional
```

### 5. Custom SQL Queries
```yaml
source_type: custom_query
custom_query: sql/complex_query.sql
data_source:
  type: csv                # Data source for the query
  file_path: data/transactions.csv
  table_alias: transactions
  options:
    infer_schema_length: 10000
```

## Supported Targets

### 1. Oracle Database
```yaml
target_type: oracle
target_table: PRODUCTS
primary_key: [ID]
foreign_keys:
  - columns: [CATEGORY_ID]
    references_table: CATEGORIES  
    references_columns: [ID]
indexes:
  - name: IDX_PRODUCT_NAME
    columns: [NAME]
```

### 2. DBF Files
```yaml
target_type: dbf
target_table: EXPORT_DATA
target_file_path: output/export.dbf
```

### 3. XLSX Files  
```yaml
target_type: xlsx
target_table: REPORT_DATA
target_file_path: reports/monthly_report.xlsx
xlsx_options:
  worksheet_name: MonthlyData
  header: true
  index: false
```

## Advanced Usage

### Custom SQL Queries with Multiple Data Sources

```yaml
tables:
  sales_analysis:
    source_type: custom_query
    custom_query: sql/sales_analysis.sql
    target_type: oracle
    target_table: SALES_ANALYSIS
    data_source:
      type: csv
      file_path: data/sales.csv
      table_alias: sales_data
      options:
        infer_schema_length: 10000
```

SQL file (`sql/sales_analysis.sql`):
```sql
SELECT 
    product_name,
    SUM(quantity) as total_quantity,
    AVG(price) as avg_price,
    COUNT(*) as transaction_count
FROM sales_data
WHERE date >= '2025-01-01'
GROUP BY product_name
ORDER BY total_quantity DESC
```

### Programmatic Usage

```python
from etl_dbf_oracle import OracleETL
from etl_dbf_oracle.config import TableConfig

# Initialize with explicit connection
etl = OracleETL.from_env_file('.env')

# Single table processing
config = TableConfig({
    'source_type': 'csv',
    'source_file_path': 'data/products.csv',
    'target_type': 'oracle', 
    'target_table': 'PRODUCTS',
    'primary_key': ['ID'],
    'batch_size': 1000
})

result = etl.run_etl_for_table(config, 'data/products.csv')
print(f"Loaded {result['total_rows']} rows")

# Context manager usage
with OracleETL.from_env_file('.env') as etl:
    results = etl.run_multiple_tables('config/tables.yaml')
    print(etl.get_pipeline_summary(results))
```

## Data Type Mapping

The pipeline automatically handles type conversion between formats:

| Polars Type | Oracle Type | DBF Type | XLSX Type |
|-------------|-------------|----------|-----------|
| Int64       | NUMBER(19,0)| N(10,0)  | Integer   |
| Float64     | NUMBER      | N(15,2)  | Float     |
| Utf8        | VARCHAR2    | C(width) | Text      |
| Boolean     | NUMBER(1,0) | L        | Boolean   |
| Date        | DATE        | D        | Date      |
| Datetime    | TIMESTAMP   | D        | DateTime  |

## Error Handling

The pipeline provides comprehensive error handling:

- **Configuration validation** before processing
- **File existence checks** for all file-based sources
- **Schema validation** and automatic corrections
- **Data type conversion** with fallbacks
- **Constraint validation** for database targets
- **Detailed error messages** with context

## Performance Considerations

### Batch Processing
- Configure appropriate `batch_size` for your data volume
- Default: 1000 rows per batch
- Larger batches = better performance, more memory usage

### Memory Management
- Polars uses lazy evaluation when possible
- Large files are processed in chunks
- Monitor memory usage for very large datasets

### Database Optimization
- Use appropriate indexes for foreign key columns
- Consider partitioning for very large tables
- Monitor Oracle connection pool settings

## Troubleshooting

### Common Issues

1. **"XLSX reading/writing requires openpyxl"**
   This should not occur with normal installation since openpyxl is included as a dependency. If it does occur:
   ```bash
   pip install openpyxl
   ```

2. **"Oracle connection failed"**
   - Check `.env` file configuration
   - Verify Oracle service is running
   - Test connection with Oracle SQL Developer

3. **"Primary key columns not found"**
   - Check column name case sensitivity
   - Verify column names in source data
   - Use case-insensitive matching

4. **"Invalid table name"**
   - For Oracle targets, table names must follow Oracle naming rules
   - For file targets, ensure valid file paths

### Debug Mode

Enable detailed logging:

```python
import logging
logging.basicConfig(level=logging.DEBUG)

# Your ETL code here
```

## Example Configurations

### Complete Multi-Source Example

```yaml
tables:
  # CSV to Oracle with full constraints
  products:
    source_type: csv
    source_file_path: data/products.csv
    target_type: oracle
    target_table: PRODUCTS
    primary_key: [ID]
    foreign_keys:
      - columns: [CATEGORY_ID]
        references_table: CATEGORIES
        references_columns: [ID]
    indexes:
      - name: IDX_PRODUCT_NAME
        columns: [NAME]
        unique: false
    csv_options:
      infer_schema_length: 10000
      null_values: ['', 'NULL', 'N/A']

  # Legacy DBF to modern XLSX
  legacy_to_modern:
    source_type: dbf
    source_file_path: legacy/olddata.dbf
    target_type: xlsx
    target_table: MODERNIZED_DATA
    target_file_path: output/modernized.xlsx
    xlsx_options:
      worksheet_name: ModernData
      header: true

  # Complex analysis with custom query
  quarterly_report:
    source_type: custom_query
    custom_query: sql/quarterly_analysis.sql
    target_type: xlsx
    target_table: Q1_ANALYSIS
    target_file_path: reports/q1_2025.xlsx
    data_source:
      type: xlsx
      file_path: data/raw_sales.xlsx
      table_alias: sales
      options:
        sheet_name: RawData
        header: 0
    xlsx_options:
      worksheet_name: Analysis
      header: true
```

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

For support and questions:
- Create an issue on GitHub
- Check the troubleshooting section above
- Review the example configurations

---

**Built with ❤️ using Polars, pandas, and modern Python tooling.**