import gspread
from gspread import Client
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from authlib.integrations.requests_client import AssertionSession
from oauth2client.service_account import ServiceAccountCredentials
from typing import Dict, Union, Optional, Tuple, Any, List

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *

import urllib.parse
import time
import re
import json
import pandas as pd

from google_sheets_spark_connector.credentials import get_credentials_file

spark = SparkSession \
    .builder \
    .appName("Spark") \
    .getOrCreate()
# OPTIONAL IF USING Databricks

def generate_credentials_dict(credentials: Dict):
  
    """
    Purpose:
        Generate a dictionary containing the Google service account credentials.
    Args:
        project_id (str): The ID of the project containing the Google service account.
        private_key_id (str): The ID of the private key.
        private_key (str): The private key.
        client_email (str): The email address associated with the Google service account.
        client_id (str): The client ID associated with the Google service account.
    Returns:
        A dictionary containing the Google service account credentials.
    """
    
    # Ensure that all required keys are present
    assert "project_id" in credentials.keys(), "Error: 'project_id' key not found in `credentials`"
    assert "private_key_id" in credentials.keys(), "Error: 'private_key_id' key not found in `credentials`"
    assert "private_key" in credentials.keys(), "Error: 'private_key' key not found in `credentials`"
    assert "client_email" in credentials.keys(), "Error: 'client_email' key not found in `credentials`"
    assert "client_id" in credentials.keys(), "Error: 'client_id' key not found in `credentials`"

    # Extract the necessary values
    project_id = credentials["project_id"]
    private_key_id = credentials["private_key_id"]
    private_key = credentials["private_key"]
    client_email = credentials["client_email"]
    client_id = credentials["client_id"]
    
    type = 'service_account'
    auth_uri = 'https://accounts.google.com/o/oauth2/auth'
    token_uri = 'https://oauth2.googleapis.com/token'
    auth_provider_x509_cert_url = 'https://www.googleapis.com/oauth2/v1/certs'
    private_key = private_key.replace('\\n', '\n')
    
    credentials_dict = {
        'type': type,
        'project_id': project_id,
        'private_key_id': private_key_id,
        'private_key': f'-----BEGIN PRIVATE KEY-----\n{private_key}\n-----END PRIVATE KEY-----\n',
        'client_email': client_email,
        'client_id': client_id,
        'auth_uri': auth_uri,
        'token_uri': token_uri,
        'auth_provider_x509_cert_url': auth_provider_x509_cert_url,
        'client_x509_cert_url': f'https://www.googleapis.com/robot/v1/metadata/x509/{urllib.parse.quote(client_email)}'
    }
    return credentials_dict
  
def create_assertion_session(credentials_dict: Dict, subject: Optional[str] = None) -> AssertionSession:
    """
    Creates an assertion session using the provided credentials dictionary and subject.
    Args:
        credentials_dict: A dictionary containing the credentials to be used for the assertion session.
        subject: The subject of the assertion session.
    Returns:
        An AssertionSession object.
    """
    scopes = ['https://spreadsheets.google.com/feeds',
              'https://www.googleapis.com/auth/drive',
              'https://www.googleapis.com/auth/analytics.readonly',
              'https://www.googleapis.com/auth/presentations']

    token_url = credentials_dict['token_uri']
    issuer = credentials_dict['client_email']
    key = credentials_dict['private_key']
    key_id = credentials_dict['private_key_id']
    header = {'alg': 'RS256'}

    if key_id:
        header['kid'] = key_id
    claims = {'scope': ' '.join(scopes)}

    return AssertionSession(
        grant_type=AssertionSession.JWT_BEARER_GRANT_TYPE,
        token_endpoint=token_url,
        issuer=issuer,
        audience=token_url,
        claims=claims,
        subject=subject,
        key=key,
        header=header,
    )
    
def get_credentials(credentials_dict: Dict) -> Tuple[ServiceAccountCredentials, AssertionSession]:
    """
    Returns a tuple containing credentials and an assertion session.
    Args:
        credentials_dict (dict): A dictionary containing credentials.
    Returns:
        Tuple[ServiceAccountCredentials, AssertionSession]: A tuple containing credentials and an assertion session.
    """

    scopes = ['https://spreadsheets.google.com/feeds',\
              'https://www.googleapis.com/auth/drive',\
              'https://www.googleapis.com/auth/analytics.readonly',\
              'https://www.googleapis.com/auth/presentations']
    
    credentials = ServiceAccountCredentials.from_json_keyfile_dict(credentials_dict)
    session = create_assertion_session(credentials_dict)
    return credentials, session

def retry_api_call(api_call):
    retries = 5
    for attempt in range(retries):
        try:
            return api_call
        except (gspread.exceptions.APIError, HTTPError) as error:
            if isinstance(error, gspread.exceptions.APIError) and error.response.status_code == 429:
                print("Rate limit exceeded. Retrying in 60 seconds...")
                time.sleep(60)
            else:
                print("An error occurred:", error)
                raise
        except Exception as e:
            print("An unexpected error occurred:", e)
            raise
    print("Maximum retries reached. Exiting...")
    raise

def client(credentials_dict: Dict) -> Client:
    """
    Returns an authorized client object for using the Google Sheets API.
    Parameters:
    credentials_dict (dict): Dictionary containing the service account credentials.
    Returns:
    gspread.Client: An authorized client object for using the Google Sheets API.
    """
    
    credentials, session = get_credentials(credentials_dict)
    return Client(credentials, session)


def open_spreadsheet_by_id(retries,credentials_dict,spreadsheet_id):
    while retries < 5:
        try:
            gc = client(credentials_dict).open_by_key(spreadsheet_id)
            break

        except Exception as e:
            print(f"Error: {e}")
            retries += 1
            if retries < 5:
                print(f"Retrying... (Attempt {retries}/5)")
                time.sleep(20)
            else:
                print("Max retries reached. Unable to open spreadsheet.")
                raise
    return gc


def service(credentials_dict: Dict):
    """
    Returns an instance of the Google Sheets API client.
    Parameters:
    credentials_dict (dict): A dictionary containing service account credentials.
    Returns:
    Resource: An instance of the Google Sheets API client.
    """
    
    credentials, session = get_credentials(credentials_dict)
    return build('sheets', 'v4', credentials=credentials)


def auth_google_sheet(spreadsheet_id: str, credentials_dict: Dict):
    """
    Authenticate with Google Sheets API using service account credentials.
    Parameters:
    - spreadsheet_id: str - the ID of the spreadsheet to authenticate for
    - credentials_dict: Dict - a dictionary containing the service account credentials
    
    Returns:
    - gsheet: gspread.models.Spreadsheet - an authenticated instance of the Google Sheets API
    """
    
    scopes = ['https://spreadsheets.google.com/feeds',\
              'https://www.googleapis.com/auth/drive',\
              'https://www.googleapis.com/auth/analytics.readonly',\
              'https://www.googleapis.com/auth/presentations']
    
    credentials = ServiceAccountCredentials.from_json_keyfile_dict(credentials_dict)
    session = create_assertion_session(credentials_dict)
    client = Client(credentials, session)
    if len(spreadsheet_id) == 44:
        try:
            gsheet = open_spreadsheet_by_id(retries=0,credentials_dict=credentials_dict,spreadsheet_id=workbook_id)                
        except:
            gsheet = client.open(spreadsheet_id)
    else:
        gsheet = client.open(spreadsheet_id)
    return gsheet

def update_sheet(client, data, sheet, cell_range):
    """
    Updates the data of a sheet in a Google Sheets workbook with a given set of data.
    Args:
    - client (gspread.Client): an authorized Google Sheets API client.
    - data (list[list[str]]): the data to write to the sheet.
    - sheet (gspread.Worksheet): the worksheet object of the target sheet the data is going into. 
    - cell_range (str): the cell or range to update with the provided data (use R1C1 notation).
    Returns: None
    """
    
    sheet_title = sheet.title
    
    sheet_address = f'{sheet_title}!{cell_range}'
    
    if sheet_address.split('!')[0].count(':') >= 1:
        print(f'Sheet Title ({sheet_address}) contains colon; removing colon and update will take place starting in cell A1.')
        sheet_address = sheet_address.split('!')[0]

    print(sheet_address)
    
    try:
        client.values_update(sheet_address, params={'valueInputOption': 'USER_ENTERED'}, body={'values': data})
    except Exception as e:
        if 'try again' in str(e).lower():
            t = 45
            print(f'Initial sheet update failed due to error: {e}\nTrying again in {t} seconds')
            time.sleep(t)
            client.values_update(sheet_address, params={'valueInputOption': 'USER_ENTERED'}, body={'values': data})
        else:
            print(f'Error (`update_sheets`): {e}')

def remove_filter(client, sheet):
    """
    Removes any existing basic filter from the specified sheet.
    
    Args:
    - client (googleapiclient.discovery.Resource): authenticated Google Sheets API client object
    - sheet (gspread.Worksheet): the worksheet object to remove the filter from
    
    Returns: None
    """
    
    sheetId = sheet._properties['sheetId']
    body = {
        'requests': [{
            'clearBasicFilter': {
                'sheetId': sheetId
            }
        }]
    }
    client.batch_update(body)

def hide_or_unhide_sheet(spreadsheet_id, sheet_id, hide_or_unhide_boolean, credentials_dict=credentials_dict):
    ss = auth_google_sheet(
        spreadsheet_id=spreadsheet_id, credentials_dict=credentials_dict
    )
    body = {
        "requests": [
            {
                "updateSheetProperties": {
                    "properties": {
                        "sheetId": sheet_id,
                        "hidden": hide_or_unhide_boolean,
                    },
                    "fields": "hidden",
                }
            }
        ]
    }
    ss.batch_update(body)

def apply_filter(client, sheet, **kwargs: Any) -> None:
    """
    Reapply filters to a Google Sheet.
    Args:
        client (Resource): The Google Sheets API client.
        sheet (Resource): The Google Sheet to update.
        **kwargs: Additional arguments to configure filter settings.
            string_exclusion_filter (str): String value used to filter out rows (for a column) that you would like to be filtered out post-update.
            startRowIndex (int): The starting row index to apply the filter to. Default is 0.
            startColumnIndex (int): The index of the column you would like to start the filter at. Default is 0 (ie. column A).
            filterColumnIndex (int): The index of the column to apply the actual filter to. Default is 0 (ie. column A).
    """
    string_exclusion_filter = kwargs.get('string_exclusion_filter', '')
    start_row_index  = kwargs.get('startRowIndex', 0)
    start_col_index  = kwargs.get('startColumnIndex', 0)
    filter_col_index = kwargs.get('filterColumnIndex', 0)
    
    sheet_props = sheet._properties
    grid_props = sheet_props.get('gridProperties', {})
    row_count = grid_props.get('rowCount', 0)
    col_count = grid_props.get('columnCount', 0)
    
    filter_range = {
        'sheetId': sheet_props.get('sheetId', 0),
        'startRowIndex': start_row_index,
        'startColumnIndex': start_col_index,
        'endRowIndex': row_count,
        'endColumnIndex': col_count
    }
    
    criteria = {filter_col_index: {'hiddenValues': [f'{string_exclusion_filter}']}}
    
    filter_settings = {'range': filter_range, 'criteria': criteria}
    basic_filter = {'setBasicFilter': {'filter': filter_settings}}
    requests = [{'updateSheetProperties': {'properties': sheet_props, 'fields': 'gridProperties'}}]
    requests.append(basic_filter)
    
    body = {'requests': requests}
    client.batch_update(body)

def auto_fit_cols(client, sheet, **kwargs: Any) -> None:
    """
    Auto-fits columns in a worksheet.
    Args:
    - client: gspread.client.Client - The authenticated client object.
    - sheet: gspread.Worksheet - The worksheet to be processed.
    - kwargs:
        - startIndex: int - The index of the starting column (inclusive) to be processed. (default: 0)
        - endIndex: int - The index of the ending column (exclusive) to be processed. (default: the total number of columns in the worksheet)
    Returns: None
    """

    start_index = kwargs.get('startIndex', 0)
    end_index = kwargs.get('endIndex', sheet.col_count)

    body = {
        "requests": [
            {
                "autoResizeDimensions": {
                    "dimensions": {
                        "sheetId": sheet.id,
                        "dimension": "COLUMNS",
                        "startIndex": start_index,
                        "endIndex": end_index
                    }
                }
            }
        ]
    }
    
    client.batch_update(body)

def set_all_spark_df_columns_to_strings(spark_df):
    # CONVERTS SPARK DF TO PANDAS > CHANGES ALL COLUMNS TO STRINGS > BACK TO SPARK DF
    p_df               = spark_df.toPandas()
    p_df[p_df.columns] = p_df[p_df.columns].astype(str)
    spark_df           = spark.createDataFrame(p_df)
    return spark_df
    
def gsheet_pandas_conversion(spark_df, **kwargs) -> Union[str, List[List[Union[str, int, float]]]]:
    """
    Converts a PySpark DataFrame to a Pandas DataFrame.
    Args:
        spark_df (DataFrame): A PySpark DataFrame.
        push_df_as_html (bool, optional): A flag indicating whether to return an HTML table.
        html_table_style (str, optional): The style string to apply to the HTML table.
    Returns:
        If `push_df_as_html` is True, an HTML table string. Otherwise, a list of lists representing the Pandas DataFrame.
    """
  
    # Get the original column names
    original_columns = spark_df.columns
    
    # Rename duplicate columns to make them unique
    new_columns = []
    for x in range(len(original_columns)):
        if original_columns[:x].count(original_columns[x]) >= 1:
            new_columns.append(str(original_columns[x]) + str(original_columns[:x].count(original_columns[x])))
        else:
            new_columns.append(str(original_columns[x]))

    spark_df = spark_df.toDF(*new_columns)
        
    # ADDED BELOW 03.28.24
    for dataType in spark_df.dtypes:
        col_name = dataType[0]
        col_type = dataType[1]

        if col_type=='boolean':
            spark_df = spark_df.withColumn(col_name,when(col(col_name).isNull(),False))

        elif col_type.find('array') != -1:
            spark_df = spark_df.withColumn(col_name,when(col(col_name).isNull(),array()))

    # converts all columns to strings
    spark_df = set_all_spark_df_columns_to_strings(spark_df)
    
    # BLOCKED OFF BELOW 03.28.24
    # # Convert boolean and array columns to string
    # for col_name, col_type in spark_df.dtypes:
    #     if col_type == 'boolean':
    #         spark_df = spark_df.withColumn(col_name, when(col(col_name).isNull(), False).otherwise(col(col_name)).cast('string'))
    #     elif col_type.find('array') != -1:
    #         spark_df = spark_df.withColumn(col_name, when(col(col_name).isNull(), array()).otherwise(col(col_name)).cast('string'))
    #     else:
    #         spark_df = spark_df.withColumn(col_name, when(col('`'+col_name+'`').isNull(), '').otherwise(col('`'+col_name+'`')))

    # Convert Spark DataFrame to Pandas DataFrame and remap original column names
    spark_df = spark_df.toDF(*original_columns).toPandas()

    if kwargs and kwargs.get('push_df_as_html',False):
        # Generate HTML table from Pandas DataFrame
        html_table = spark_df.to_html(escape=False, index=False)

        # Remove thead section of the HTML table
        html_table = html_table.split('<thead>')[0] + html_table.split('</thead>')[1]

        if kwargs.get('html_table_style'):
            # Add a style attribute to the HTML table
            html_table = html_table.replace('<table ', f'<table style="{kwargs["html_table_style"]}" ')

        return html_table
    else:
        # Convert Pandas DataFrame to list of lists
        return spark_df.values.tolist()

def get_all_sheet_row_metadata(spreadsheet_id: str, sheet_id: str, credentials_dict: Dict) -> List[Dict[str, Any]]:
    """
    Retrieves metadata for all rows in a Google Sheet.
    
    Args:
        spreadsheet_id (str): The ID of the Google Sheet to retrieve data from.
        sheet_id (str): The ID of the sheet within the Google Sheet to retrieve data from.
        
    Returns:
        List[Dict[str, Any]]: A list of dictionaries representing metadata for each row in the sheet.
    """
    
    gc = open_spreadsheet_by_id(retries=0,credentials_dict=credentials_dict,spreadsheet_id=spreadsheet_id)
                
    sheet = gc.get_worksheet_by_id(int(sheet_id))
    sheet_properties = sheet._properties
    ranges = [sheet_properties['title']]
    include_grid_data = 'false'
    request = service(credentials_dict).spreadsheets().get(
        spreadsheetId=spreadsheet_id, ranges=ranges, includeGridData=include_grid_data)
    response = request.execute()
    data = response['sheets'][0]['data'][0]['rowData']
    return data

def reprocess_sheet_data(workbook_id: str, sheet_id: str, data: List[Dict[str, Any]], credentials_dict: Dict) -> None:
    """
    Reprocesses sheet data by replacing the contents of the specified sheet with the given data.
    Args:
        workbook_id (str): The ID of the Google Sheets workbook.
        sheet_id (str): The ID of the sheet to reprocess.
        data (List[Dict[str, Any]]): A list of dictionaries representing the rows of the sheet. Each dictionary should
            have keys corresponding to the column names and values corresponding to the cell values.
    Returns:
        None
    """

    # Open the workbook by ID
    gc = open_spreadsheet_by_id(retries=0,credentials_dict=credentials_dict,spreadsheet_id=spreadsheet_id)

    # Define the batch update request body
    request_body = {
        "requests": [
            {
                "update_cells": {
                    "fields": "*",
                    "range": {
                        "sheet_id": sheet_id,
                        "start_column_index": 0,
                        "start_row_index": 0,
                    },
                    "rows": data,
                },
            },
        ]
    }

    # Execute the batch update request
    gc.batch_update(request_body)

def push_df_as_html_to_sheet(workbook_id: str,\
                             sheet_id: str,\
                             col_index: int,\
                             row_index: int,\
                             html_list: List[str],\
                             credentials_dict: Dict,\
                             **kwargs: Any) -> None:
    """
    Pushes a list of HTML strings to a Google Sheet.
    Args:
    - workbook_id (str): ID of the Google Sheet workbook
    - sheet_id (str): ID of the Google Sheet
    - col_index (int): 0-based index of the starting column where the HTML should be pasted
    - row_index (int): 0-based index of the starting row where the HTML should be pasted
    - html_list (List[str]): list of HTML strings
    - **kwargs: additional keyword arguments to specify formatting of the pasted data. Available options:
        - horizontalAlignment (str): horizontal alignment for text in pasted cells (left, center, right)
        - verticalAlignment (str): vertical alignment for text in pasted cells (top, middle, bottom)
        - wrapStrategy (str): wrap strategy for text in pasted cells (overflow, wrap, clip)
        - fontFamily (str): font family for text in pasted cells (e.g. Arial)
        - fontSize (int): font size for text in pasted cells
    Returns:
    - None
    """

    # Open workbook
    gc = open_spreadsheet_by_id(retries=0,credentials_dict=credentials_dict,spreadsheet_id=workbook_id)
    
    # Update sheet with HTML data
    body = {
        "requests": [
            {
                "pasteData": {
                    "html": True,
                    "data": html_list,
                    "coordinate": {
                        "sheetId": sheet_id,
                        "columnIndex": col_index,
                        "rowIndex": row_index,
                    },
                },
            },
        ]
    }
    gc.batch_update(body)
    
    # Wait for update to take effect
    time.sleep(5)
    
    # Update cell formatting
    data = get_all_sheet_row_metadata(workbook_id, sheet_id, credentials_dict)
    list_of_nested_dicts = []
    for i in data:
        try:
            nested_dict = dict(i)['values']
            if data.index(i) >= row_index: 
                for x in nested_dict:
                    try:
                        effective_format = x.get('effectiveFormat', {})
                        user_entered_format = x.get('userEnteredFormat', {})
                        
                        if kwargs.get('horizontalAlignment'):
                            effective_format['horizontalAlignment'] = kwargs['horizontalAlignment']
                        if kwargs.get('verticalAlignment'):
                            effective_format['verticalAlignment'] = kwargs['verticalAlignment']
                        if kwargs.get('wrapStrategy'):
                            effective_format['wrapStrategy'] = kwargs['wrapStrategy']
                            
                        x.update({'effectiveFormat': effective_format})
                        
                        if kwargs.get('fontFamily') or kwargs.get('fontSize'):
                            try:
                                text_format = effective_format['textFormat']
                                if kwargs.get('fontFamily'):
                                    text_format['fontFamily'] = kwargs['fontFamily']
                                if kwargs.get('fontSize'):
                                    text_format['fontSize'] = kwargs['fontSize']
                            except KeyError:
                                text_format = {'fontFamily': kwargs.get('fontFamily', None),
                                               'fontSize': kwargs.get('fontSize', None)}
                            user_entered_format.update({'textFormat': text_format})
                            
                            if kwargs.get('horizontalAlignment'):
                                user_entered_format['horizontalAlignment'] = kwargs['horizontalAlignment']
                            if kwargs.get('verticalAlignment'):
                                user_entered_format['verticalAlignment'] = kwargs['verticalAlignment']
                            if kwargs.get('wrapStrategy'):
                                user_entered_format['wrapStrategy'] = kwargs['wrapStrategy']
                            
                            x.update({'userEnteredFormat': user_entered_format})
                    except:
                        continue
        except:
            nested_dict = {}
        list_of_nested_dicts.append({'values':nested_dict})
    
    reprocess_sheet_data(workbook_id, sheet_id, list_of_nested_dicts, credentials_dict)

def remove_sheet_formatting_from_row_index_downwards(spreadsheet_id: str, sheet_id: str, start_row_index: int, credentials_dict: Dict):
    """
    Removes all formatting from a Google Sheet from the given row index downwards.
    
    Args:
    - spreadsheet_id (str): the ID of the Google Sheet to update
    - sheet_id (int): the ID of the sheet within the workbook to update
    - start_row_index (int): the row index from which to start removing formatting (inclusive)
    
    Returns: None
    """
    
    # Authenticate with Google Sheets API
    gc = client(credentials_dict)
    
    # Get the specified sheet by ID
    wb = open_spreadsheet_by_id(retries=0,credentials_dict=credentials_dict,spreadsheet_id=spreadsheet_id)
    sheet = wb.get_worksheet_by_id(sheet_id)
    
    # Construct the batch update request body
    body = {
        "requests": [
            {
                "updateCells": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": start_row_index - 1  # Subtract 1 because API uses 0-indexing
                    },
                    "fields": "userEnteredFormat"  # Remove all user-entered formatting
                }
            }
        ]
    }
    
    # Send the batch update request to the API to remove formatting
    sheet.spreadsheet.batch_update(body)
    
    return None

def column_string(n):
    """
    Convert a column number to its corresponding column string.
    e.g. 1 -> 'A', 2 -> 'B', ..., 26 -> 'Z', 27 -> 'AA', 28 -> 'AB', ..., 703 -> 'AAA', etc.
    """
    result = ""
    while n > 0:
        n, remainder = divmod(n - 1, 26)
        result = chr(65 + remainder) + result
    return result

def refresh_data_in_sheet(sheet_name_or_id,\
                          spark_df,\
                          workbook_id,\
                          row_number_to_push_into,\
                          column_letter_to_push_into,
                          credentials_dict,\
                          **kwargs):

    """
    Refreshes data in a Google Sheet with new data from a PySpark DataFrame.
    Args:
    - sheet_name_or_id (str or int): name or ID of the Google Sheet to update
    - spark_df (pyspark.sql.DataFrame): PySpark DataFrame containing the data to update the sheet with
    - workbook_id (str): ID of the Google Sheet workbook
    - row_number_to_push_into (int): 1-based index of the row to start pasting data into
    - column_letter_to_push_into (str): letter representing the column to start pasting data into
    - credentials_dict (dict): dictionary containing authentication credentials for Google Sheets API
    - **kwargs: additional keyword arguments to specify optional behavior:
        - pause_between_sheet_updates (int, optional): the number of seconds to wait between each update to the sheet. Default is 1 second.
        - preserve_last_n_columns (int, optional): the number of columns to preserve at the end of the sheet. Default is 0.
        - push_df_as_html (bool, optional): If True, push the data to the sheet as HTML. Default is False.
        - horizontalAlignment (str, optional): The horizontal alignment for text in pasted cells (left, center, right).
        - verticalAlignment (str, optional): The vertical alignment for text in pasted cells (top, middle, bottom).
        - wrapStrategy (str, optional): The wrap strategy for text in pasted cells (overflow, wrap, clip).
        - fontFamily (str, optional): The font family for text in pasted cells (e.g. Arial).
        - fontSize (int, optional): The font size for text in pasted cells.
        - starting_row_filter_index (int, optional): The 0-based index of the starting row for applying the filter. Default is 0.
        - starting_column_filter_index (int, optional): The 0-based index of the starting column for applying the filter. Default is 0.
        - apply_filter_to_column_index (int, optional): The 0-based index of the column to apply the filter to. Default is 0.
        - string_exclusion_filter (str, optional): A string to exclude from the filter. Default is an empty string.
        - do_not_clear_entire_target_sheet_range (bool, optional): If true, then it will only clear the range of the cells that your df will eventually replace
    Returns:
    - None
    """
    
    # Authenticate with Google Sheets API
    gc = auth_google_sheet(workbook_id, credentials_dict)

    # Get the specified sheet by name or ID
    if len(re.sub("[^0-9]", "", str(sheet_name_or_id)))==len(str(sheet_name_or_id)):
        sheet = gc.get_worksheet_by_id(int(sheet_name_or_id))
    else:
        sheet = gc.worksheet(sheet_name_or_id)

    sheet_name = sheet.title
    sheet_id = sheet._properties['sheetId']

    # Remove any existing filters from the sheet
    remove_filter(gc, sheet)

    # Wait for the specified amount of time between sheet updates
    time.sleep(kwargs.get('pause_between_sheet_updates', 1))
    
    # convert spark_df into proper payload format needed to handle various Google Sheet API requests 
    spark_body_list_of_list = gsheet_pandas_conversion(spark_df, push_df_as_html=kwargs.get('push_df_as_html',False))

    # get size of df
    size_of_df = len(spark_body_list_of_list)
                              
    # Write headers to the sheet
    headers = [[col for col in spark_df.columns]]
    update_sheet(client=gc,
                 data=headers,
                 sheet=sheet,
                 cell_range=f"{column_letter_to_push_into}{row_number_to_push_into}")

    # determine end row of sheet clearing step (slated to happen next)
    end_row = kwargs.get('do_not_clear_entire_target_sheet_range', False)
    if end_row:
      end_row = size_of_df+row_number_to_push_into+1
      end_col = column_string(len(spark_body_list_of_list[0]))
    if not end_row:
      end_row = ''
      end_col = column_string(sheet.col_count - kwargs.get('preserve_last_n_columns', 0))

    # Clear the sheet data from the starting cell to the end of the row
    range_to_clear = f"{sheet_name}!{column_letter_to_push_into}{row_number_to_push_into+1}:{end_col}{end_row}"
    gc.values_clear(range_to_clear)

    # Wait for the specified amount of time between sheet updates
    time.sleep(kwargs.get('pause_between_sheet_updates', 1))

    # Write data to the sheet as HTML, if specified
    if 'push_df_as_html' in kwargs and kwargs.get('push_df_as_html',False):
        remove_sheet_formatting_from_row_index_downwards(spreadsheet_id=workbook_id,
                                                         sheet_id=sheet_id,
                                                         start_row_index=row_number_to_push_into+1,
                                                         credentials_dict=credentials_dict)
        
        colIndex = ord(column_letter_to_push_into.lower()) - 97
        push_df_as_html_to_sheet(workbook_id=workbook_id,
                                 sheet_id=sheet_id,
                                 col_index=colIndex,
                                 row_index=row_number_to_push_into,
                                 html_list=spark_body_list_of_list,
                                 credentials_dict=credentials_dict,
                                 **kwargs)
        
    # Otherwise, write dataframe body data to the sheet
    else:
        update_sheet(client=gc,
                     data=spark_body_list_of_list,
                     sheet=sheet,
                     cell_range=f"{column_letter_to_push_into}{row_number_to_push_into+1}")

    # Wait for the specified amount of time between sheet updates
    time.sleep(kwargs.get('pause_between_sheet_updates', 1))

    # Apply a filter to the sheet, if specified
    if kwargs is not None and 'apply_filter_to_column_index' in kwargs:
      
        assert 'starting_row_filter_index' in kwargs, '`starting_row_filter_index` is missing'
        assert 'starting_column_filter_index' in kwargs, '`starting_column_filter_index` is missing'
      
        string_exclusion_filter = kwargs.get('string_exclusion_filter', '')
        startRowIndex = kwargs.get('starting_row_filter_index', 0)
        startColumnIndex = kwargs.get('starting_column_filter_index', 0)
        filterColumnIndex = kwargs.get('apply_filter_to_column_index', 0)
        
        apply_filter(client=gc,
                       sheet=sheet,
                       string_exclusion_filter=string_exclusion_filter,
                       startRowIndex=startRowIndex,
                       startColumnIndex=startColumnIndex,
                       filterColumnIndex=filterColumnIndex)

        
def read_google_sheet(
      workbook_id: str, # ID of the Google Sheets workbook
      sheet_id: str, # ID of the sheet within the workbook
      credentials: Credentials, # authentication credentials for accessing the Google Sheets API
      make_columns_unique: bool = True, # flag indicating whether to make column names unique
      clean_column_names: bool = True, # flag indicating whether to clean column names
      drop_empty_rows: bool = True, # flag indicating whether to drop empty rows
      cell_range: Optional[str] = None, # optional string specifying a range of cells to retrieve from the sheet
      temp_view_name: Optional[str] = None, # optional string specifying a temporary view name for the data in the Spark DataFrame
    ) -> DataFrame: # returns a Spark DataFrame containing the data from the Google Sheet

    gc = retry_api_call(client(credentials)) # creates a gspread client object using the provided credentials

    try:
        spreadsheet = retry_api_call(open_spreadsheet_by_id(retries=0,credentials_dict=credentials,spreadsheet_id=workbook_id)) # tries to open the workbook by ID
    except:
        spreadsheet = retry_api_call(gc.open(workbook_id)) # if that fails, tries to open the workbook by name

    worksheet = (retry_api_call(spreadsheet.get_worksheet_by_id(int(sheet_id))) # gets the worksheet by ID if sheet_id is an integer
                 if re.match(r"^\d+$", sheet_id) # else gets the worksheet by name
                 else retry_api_call(spreadsheet.worksheet(sheet_id)))

    range_data = (f"{worksheet.title}!{cell_range}" # specifies a cell range to retrieve if cell_range is provided
                  if cell_range else worksheet.title)

    values = retry_api_call(spreadsheet.values_get(range_data)) # retrieves the values within the specified cell range
    values = values['values'] # extracts the values from the response
    df = pd.DataFrame(values) # creates a pandas DataFrame from the values
    df.fillna("",inplace=True) # replaces any null values with empty strings
    
    if drop_empty_rows:
        df.dropna(how="all", inplace=True) # drops any rows where all values are null
        df = df[df.astype(bool).sum(axis=1) > 0] # drops any rows where all values are empty strings
        
    headers = df.iloc[0] # extracts the header row from the DataFrame
    headers = [str(col).lower() for col in headers] # converts the header names to lowercase strings

    if make_columns_unique:
        cols = []
        for i, header in enumerate(headers):
            if headers[:i].count(header):
                cols.append(f"{header}{headers[:i].count(header)}") # appends a count to duplicate header names
            else:
                cols.append(header)
    else:
        cols = headers

    df = pd.DataFrame(df.values[1:], columns=cols) # creates a new DataFrame with the headers and data

    try:
        spark_df = spark.createDataFrame(df) # try to create a Spark DataFrame from the pandas DataFrame
    except:
        # if creating a Spark DataFrame from pandas fails, it's likely because there are duplicate column names
        cleaned_cols = []
        for i, col in enumerate(cols):
            if cols[:i].count(col):
                cleaned_cols.append(f"{col}{cols[:i].count(col)}")
            else:
                cleaned_cols.append(col)
        df = df.rename(columns=dict(zip(cols, cleaned_cols))) # rename columns to make them unique
        spark_df = spark.createDataFrame(df) # try to create Spark DataFrame again with the cleaned columns

    if clean_column_names:
        # if specified, clean up the column names in the Spark DataFrame
        pandas_before = spark_df.toPandas()
        pandas_before.columns = [
            re.sub("[^a-zA-Z0-9]+", "_", col) # replace non-alphanumeric characters with underscores
            for col in pandas_before.columns
        ]
        headers = [
            re.sub("__", "_", re.sub("___", "_", col)) # replace multiple consecutive underscores with a single underscore
            for col in pandas_before.columns.values
        ]
        headers = [
            col[:-3] if col.endswith("___") else col # remove trailing underscores if there are three consecutive underscores
            for col in headers
        ]
        headers = [
            col[:-2] if col.endswith("__") else col # remove trailing underscores if there are two consecutive underscores
            for col in headers
        ]
        headers = [
            col[:-1] if col.endswith("_") else col # remove trailing underscore if there is only one underscore
            for col in headers
        ]
        pandas_before.columns = headers
        cols = [col.lower() for col in pandas_before.columns.values] # convert column names to lowercase
        df = pandas_before.rename(columns=dict(zip(headers, cols))) # rename columns in pandas DataFrame to lowercase
        spark_df = spark.createDataFrame(df) # create Spark DataFrame from the cleaned up pandas DataFrame

    if temp_view_name:
        spark_df.createOrReplaceTempView(temp_view_name) # create a temporary view of the Spark DataFrame with the specified name

    return spark_df # return the Spark DataFrame
