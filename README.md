# Google Sheets Spark Connector

The Google Sheet Spark Connector is a Python package that makes it easy to do 2 things:
  1. Ingest data from a google spreadsheet into your Spark environment
  2. Programmatically update a google spreadsheet using a Spark DataFrame

It utilizes the [gspread](https://docs.gspread.org/en/v5.7.0/) Python library for basic functionality ([outlined below](#examples)), but also includes [functions](#advanced-use-cases) that use the [Google Sheets API](https://developers.google.com/sheets/api/guides/concepts) to do more advanced operations not covered by gspread.


# Table of Contents
- [Shameless Plug](#shameless-plug)
- [Prerequisites](#prerequisites)
- [Credentials](#credentials)
- [Requirements](#requirements)
- [Setup](#setup)
  - [Clone the repository](#clone-the-repository)
  - [Install the library using pip](#install-the-library-using-pip)
  - [Imports](#imports)
  - [Credentials Dict](#credentials-dict)
- [Authentication](#authentication)
- [Basic Use Cases](#basic-use-cases)
  - [Examples](#examples)
- [Advanced Use Cases](#advanced-use-cases)
  - [Scenario 1](#scenario-1)
  - [Scenario 2](#scenario-2)
  - [Scenario 2](#scenario-3)

# Shameless Plug
- Related Medium Post
  - Here's a [link](https://medium.com/@liam_clifford/google-sheets-spark-connector-7fa7a8c2f60e) to a (hopefully) less technical version of this! 
  
# Prerequisites
- In order to use the code, you will need to have a google service account user. 
  - This is mandoratory because it will allow programmatic access to the Google Sheets API.


# Credentials
- Once you have created your google service account user, you will need to generate json credentials. 
  - Here is [documentation](https://developers.google.com/workspace/guides/create-credentials#create_credentials_for_a_service_account) on how to create credentials for your service account user.
    - From there, you will be using the following (ie. for when we go to define the [`credentials_dict`](#credentials-dict)):
      - `project_id`
      - `private_key_id`
      - `private_key`
      - `client_email`
      - `client_id`


# Requirements
- authlib==1.2.0
- google-auth==2.16.2
- gspread==5.7.2
- oauth2client==4.1.3
- pyspark==3.3.2
- google-api-python-client==2.80.0


# Setup
## Clone the repository:
```python
%sh rm -rf Google-Sheets-Spark-Connector
```
```python
%sh git clone https://github.com/liam-clifford/Google-Sheets-Spark-Connector.git
```
## Install the library using pip:
```python
%pip install ./Google-Sheets-Spark-Connector
```


## Imports:
```python
import google_sheets_spark_connector
from google_sheets_spark_connector import *
```
## Credentials Dict:
  - You have two options to define the `credentials_dict` variable:
    1. Provide your own credentials in the `credentials.py` file and use the `get_credentials_file()` function:
      ```python
      credentials_dict = get_credentials_file()
      ```
    2. Set your own credentials via:
     ```python
     credentials = {
      'project_id' : project_id,
      'private_key_id' : private_key_id,
      'private_key' : private_key,
      'client_email' : client_email,
      'client_id' : client_id
      }
      credentials_dict = generate_credentials_dict(credentials)
     ```


# Authentication
  ```python
  # Define the `spreadsheet_id` variable (Found in spreadsheet URL - it is typically a 44 character alphanumerical string)
  spreadsheet_id = 'insert_spreadsheet_id_here'
  
  # Get the Google Sheets client
  gc = auth_google_sheet(spreadsheet_id, credentials_dict)
  ```


# Basic Use Cases
- Most of the functions contained in this section are native functions of the [`gspread`](https://docs.gspread.org/en/v5.7.0/) library or use the [Google Sheets API](https://developers.google.com/sheets/api/guides/concepts) directly.


## Examples
  ```python
  # Open the target sheet
  sheet = gc.get_worksheet_by_id(0)  # Alternatively, you could also use `gc.worksheet('Sheet1')`

  # Retrieve all the rows from the target sheet
  data = sheet.get_all_records()

  # Auto-fit all columns in the target sheet (default: auto-fits all columns)
  # To specify the range of column(s) to auto-fit, you can use the `startIndex` and `endIndex` kwargs.
  auto_fit_cols(client=gc, sheet=sheet)

  # Update the target sheet with new data
  update_sheet(client=gc, data=[['Name', 'Age'], ['Alice', 28], ['Bob', 35]], sheet=sheet, cell_range='A1')

  # Apply a filter to the target sheet (default behavior)
  # This will filter out rows where column A equals a blank string ('')
  apply_filter(client=gc, sheet=sheet)

  # Applies a filter to the 2nd column (startColumnIndex=1) of the target sheet, beginning in the 2nd row (startRowIndex=1)
  # This will filter out rows where column C (filterColumnIndex=2) equals 'EXCLUDE_ME' (string_exclusion_filter='EXCLUDE_ME')
  apply_filter(client=gc, sheet=sheet, string_exclusion_filter='EXCLUDE_ME', startRowIndex=1, startColumnIndex=0, filterColumnIndex=2)

  # Remove the filter from the target sheet
  remove_filter(client=gc, sheet=sheet)
  ```
  
  
# Advanced Use Cases:
## Scenario 1:
### Import data from a google spreadsheet into your Spark environment. 
```python
my_temp_view = 'insert_your_temporary_view_name_here'

df = read_google_sheet(
    workbook_id='INSERT_WORKBOOK_ID',
    sheet_id='INSERT_SHEET_ID',
    credentials=credentials_dict,
    make_columns_unique=True,
    clean_column_names=True,
    drop_empty_rows=True,
    temp_view_name=my_temp_view
)

display(df)

display(spark.sql(f'select * from {my_temp_view}'))
```
#### Interpretation:
- `read_google_sheet` creates a Spark DataFrame based on the data it reads in from a Google Spreadsheet (specified by the `workbook_id` and `sheet_id` variables provided). 
  - It takes in the following parameters:
    - `workbook_id`: ID of the Google Sheets workbook.
    - `sheet_id`: ID of the sheet within the workbook.
    - `credentials`: authentication credentials for accessing the Google Sheets API.
    - `make_columns_unique`: flag indicating whether to make column names unique (default: True).
    - `clean_column_names`: flag indicating basically whether to remove non-alphanumerical characters or not (default: True).
    - `drop_empty_rows`: flag indicating whether to drop empty rows (default: True).
    - `cell_range`: optional string specifying a range of cells to retrieve from the sheet.
    - `temp_view_name`: optional string specifying a temporary view name for the data in the Spark DataFrame.


## Scenario 2:
### Push data from a Spark DataFrame into a google spreadsheet.
```python
# Create a Spark DataFrame
spark_df = spark.createDataFrame([
    ("John Doe", 25),
    ("Jane Doe", 28),
    ("Bob Smith", 30)
], ["name", "age"])

# Push the data to a Google Sheet
refresh_data_in_sheet(
    sheet_name_or_id='INSERT_SHEET_NAME_OR_ID',
    spark_df=spark_df,
    workbook_id='INSERT_WORKBOOK_ID',
    row_number_to_push_into=2,
    column_letter_to_push_into='A',
    starting_row_filter_index=1,
    starting_column_filter_index=0,
    apply_filter_to_column_index=1,
    credentials_dict=credentials_dict
)
```
#### Interpretation:
- This will push the contents of the `spark_df` variable into the target spreadsheet - starting cell A2.
  - The **column** is determined by the `column_letter_to_push_into` variable (A = Column A)
  - The **row** is determined by the `row_number_to_push_into` variable (2 = row 2)

- There will also be a filter applied to `column B`, beginning in row 1:
  - The **row** that the filter is applied to is determined by the `starting_row_filter_index` variable (1 = row 2).
  - The **column** that the filter starts at is determined by the `starting_column_filter_index` variable (0 = Column A).
  - The **column** that the filter is applied to is determined by the `apply_filter_to_column_index` variable (1 = Column B).
    - To not have a filter applied, be sure to not pass `apply_filter_to_column_index` into the kwargs


## Scenario 3:
### Push data from a Spark DataFrame into a google spreadsheet `as html`.
```python
spark_df = spark.sql("""
    SELECT 
   '=HYPERLINK("test.com","1st test")' AS `Cell With 1 Link`
  ,'<a href="test.com">2nd test</a> and <a href="test.com">3rd test</a>.' AS `Cell With Multiple Links`
  ,'<div style="background:#00A972;color:white">=HYPERLINK("test.com","4th test")</div>' AS `Cell With link and HTML`
""")

refresh_data_in_sheet(
    sheet_name_or_id='INSERT_SHEET_NAME_OR_ID',
    spark_df=spark_df,
    workbook_id='INSERT_WORKBOOK_ID',
    row_number_to_push_into=2,
    column_letter_to_push_into='B',
    startRowIndexFilter=0,
    columnIndexFilter=1,
    credentials_dict=credentials_dict,
    starting_row_filter_index=1,
    starting_column_filter_index=1,
    apply_filter_to_column_index=2,
    push_df_as_html=True,
    horizontalAlignment='CENTER',
    verticalAlignment='MIDDLE',
    fontFamily='Barlow',
    fontSize=10,
    wrapStrategy='WRAP'
)
```
#### Interpretation:
##### The code above will perform 4 total API requests:
  1. Push the spark dataframe into the target workbook sheet (the dataframe will be pushed in as HTML)
  2. Re-process the sheet again (to collect the sheet metadata and add in the additional sheet formatting we would like to include)
      - In most cases, this is `not` necessary; however, this is required to support the multiple hyperlink use case
  3. Re-update the sheet once more to ensure that we are properly formatting the html, as well as any hyperlinks provided in the `spark_df` variable.
  4. Apply a filter to:
      - Remove any blanks contained in column C (`apply_filter_to_column_index`=2), starting at row 2 (`starting_row_filter_index`=1).
##### All rows that are beneath the row containing the dataframe column headers will have the following `HTML` format:
  - horizontally alligned: CENTER (per `horizontalAlignment`)
  - vertical alligned: MIDDLE (per `verticalAlignment`)
  - Font Family: Barlow (per `fontFamily`)
  - Font Size: 10 (per `fontSize`)
  - Text Wrap: WRAP (per `wrapStrategy`)
##### Individual Columns:
  - **`Cell With 1 Link`**:
    - This column uses the `HYPERLINK` function
  - **`Cell With Multiple Links`**:
    - This column demonstrates how you would include 2 hyperlinks in 1 single cell
  - **`Cell With link and HTML`**:
    - This column demonstrates how you could push a <div> container (with styling) into a cell
##### Kwarg Details:
  - `pause_between_sheet_updates`: the pause time (in seconds) between sheet updates.
  - `starting_row_filter_index`: The 0-based index of the starting row for applying the filter. Default is 0.
  - `starting_column_filter_index`: The 0-based index of the starting column for applying the filter. Default is 0.
  - `apply_filter_to_column_index`: The 0-based index of the column to apply the filter to. Default is 0.
  - `string_exclusion_filter`: String that is used to filter `out` rows from the column specified in `apply_filter_to_column_index`.
  
  - `push_df_as_html`: whether to push the DataFrame into the google spreadsheet as HTML
  - when `push_df_as_html` = True, you can leverage the following:
    - `horizontalAlignment`: the horizontal alignment of the pushed data
    - `verticalAlignment`: the vertical alignment of the pushed data
    - `fontFamily`: the font family of the pushed data
    - `fontSize`: the font size of the pushed data
    - `wrapStrategy`: the wrap strategy of the pushed data
