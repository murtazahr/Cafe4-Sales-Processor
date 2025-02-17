import os
import tkinter as tk
from datetime import datetime
from tkinter import filedialog

import pandas as pd

ACCOUNT_NAME = '70d82aaf-788e-4c20-8b41-708e719b2daf'


# Function to normalize table names (handle NBSP and regular spaces)
def normalize_table_name(table_name):
    if not isinstance(table_name, str):
        return str(table_name)
    # Replace NBSP (Unicode U+00A0) with regular space
    return table_name.replace('\u00A0', ' ').strip()


customer_mapping = {
    'Walk-ins': '4947c933-8e0e-4158-8cf0-336ccfa1a540',
    'Matsya Guest - Room 1': '1fee8999-29b4-4784-a35e-5215d2cecc94',
    'Matsya Guest - Room 2': '22e813da-66d4-4343-9229-40172b981bb8',
    'Matsya Guest - Room 3': '3c2efa29-cf72-4944-aa67-1a170c135c00',
    'Matsya Guest - Room 4': '4f2bf9a5-1665-4f9f-b66a-6b62bd3af1b7',
    'Matsya Guest - Room 5': '21548363-be12-4ab1-a52f-44855bc393fe',
    'Matsya Guest - Room 6': 'c4912c67-75b2-4b17-8cd9-0316a4fa3403',
    'Matsya Guest - Room 7': '1969193b-dc3d-4b61-affb-b634b31cc6a9',
    'Matsya Guest - Room 8': '1969193b-dc3d-4b61-affb-b634b31cc6a9',
    'Matsya Guest - Room 9': '199b6759-5d86-4a8e-b0e2-30a696b8fc89',
    'Captain Hooks': 'd328811f-3438-4525-8927-152cde42c92e',
    'Havelock Experience': 'b1c69dd2-d4e7-48b3-aa69-cf98fb882e1e',
    'Island Quest': '969788ce-f0f7-4928-a92e-d1cb3cefe140',
    'Abdul Haseeb': 'fc23947d-05d6-418e-ba5c-06cf1ed785c8'
}

# Normalize all keys in customer_mapping
normalized_customer_mapping = {normalize_table_name(k): v for k, v in customer_mapping.items()}


def select_input_file():
    """Open file dialog for user to select input file"""
    root = tk.Tk()
    root.withdraw()  # Hide the main window
    file_path = filedialog.askopenfilename(
        title="Select Sales Order Report Excel File",
        filetypes=[("Excel files", "*.xlsx"), ("All files", "*.*")]
    )
    return file_path


def validate_excel_file(filepath):
    """Validate if the Excel file exists and has required columns"""
    if not os.path.exists(filepath):
        raise FileNotFoundError(
            f"Error: Cannot find '{filepath}'. Please ensure the file is in the same directory as this program.")

    try:
        # Try to read first few rows to validate structure
        df = pd.read_excel(filepath, nrows=1, header=5, engine='openpyxl')
        required_columns = ['Order Id', 'Order Date', 'Bill No', 'Table Name', 'Total']
        missing_columns = [col for col in required_columns if col not in df.columns]

        if missing_columns:
            raise ValueError(f"Error: Excel file is missing required columns: {', '.join(missing_columns)}")

    except Exception as e:
        raise ValueError(
            f"Error: Unable to read Excel file. Please ensure it's not corrupted and has the correct format. Details: {str(e)}")


def sanitize_dataframe(df):
    """Sanitize the input dataframe by removing non-order rows"""
    print("Sanitizing input data...")

    # Keep track of original row count
    original_count = len(df)

    # Create a proper copy of the dataframe
    df = df.copy()

    # Convert Total to numeric using loc
    df.loc[:, 'Total'] = pd.to_numeric(df['Total'], errors='coerce')

    # Apply all filters at once
    mask = (
            df['Order Id'].notna() &
            (df['Order Id'] != '') &
            df['Total'].notna() &
            (df['Total'] != 0) &
            df['Bill No'].notna() &
            (df['Bill No'].astype(str).str.strip() != '') &
            df['Table Name'].notna() &
            (df['Table Name'].astype(str).str.strip() != '')
    )

    # Convert dates and add to mask
    df.loc[:, 'Order Date'] = pd.to_datetime(df['Order Date'], errors='coerce', dayfirst=True)
    mask = mask & df['Order Date'].notna()

    # Apply the mask
    df = df[mask].reset_index(drop=True)

    rows_removed = original_count - len(df)
    print(f"Removed {rows_removed} invalid/summary rows")
    print(f"Remaining valid order rows: {len(df)}")

    return df


def map_customer(table_name):
    """Map table name to customer ID with validation and NBSP handling"""
    if not isinstance(table_name, str):
        print(f"Warning: Invalid table name format: {table_name}. Using default 'Walk-ins'")
        return customer_mapping['Walk-ins']

    # Normalize the table name
    normalized_name = normalize_table_name(table_name)

    if normalized_name.startswith(('GL-', 'UL-', 'LL-')) or normalized_name == 'OD':
        return customer_mapping['Walk-ins']
    elif 'Matsya - Room' in normalized_name:
        try:
            room_num = normalized_name.split('Room ')[-1].strip()
            customer_key = normalize_table_name(f'Matsya Guest - Room {room_num}')
            if customer_key not in normalized_customer_mapping:
                print(f"Warning: Unknown room number in '{normalized_name}'. Using default 'Walk-ins'")
                return customer_mapping['Walk-ins']
            return normalized_customer_mapping[customer_key]
        except Exception:
            print(f"Warning: Unable to parse room number from '{normalized_name}'. Using default 'Walk-ins'")
            return customer_mapping['Walk-ins']
    elif normalized_name in ['Manta Ray']:
        return customer_mapping['Matsya Guest - Room 8']
    elif normalized_name in ['Sting Ray']:
        return customer_mapping['Matsya Guest - Room 9']
    elif normalized_name in ['Matsya Guest Breakfast', 'Matsya Staff Meals']:
        return customer_mapping['Island Quest']
    elif normalized_name == 'Ray Homes Breakfast':
        return customer_mapping['Havelock Experience']
    elif normalized_name == 'CH Staff Meals':
        return customer_mapping['Captain Hooks']

    # Check if the normalized name exists in our mapping
    if normalized_name in normalized_customer_mapping:
        return normalized_customer_mapping[normalized_name]

    print(f"Warning: Unknown table name '{normalized_name}'. Using default 'Walk-ins'")
    return customer_mapping['Walk-ins']


def process_sales_data(input_file):
    """Main function to process sales data"""
    try:
        print(f"Starting process at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Selected file: {input_file}")

        if not input_file:
            print("No file selected. Exiting...")
            return

        print("Validating input file...")
        validate_excel_file(input_file)

        print("Reading Excel file...")
        df = pd.read_excel(input_file, header=5, engine='openpyxl')

        # Sanitize input data
        df = sanitize_dataframe(df)

        if len(df) == 0:
            raise ValueError("Error: No valid order data found in the Excel file after filtering")

        print(f"Processing {len(df)} records...")

        # Create output dataframe with all columns
        output_df = pd.DataFrame(columns=[
            'IssueDate', 'DueDate', 'DueDateDays', 'DueDateDate', 'Reference', 'QuoteNumber',
            'OrderNumber', 'Customer', 'SalesQuote', 'SalesOrder', 'BillingAddress', 'ExchangeRate',
            'ExchangeRateIsInverse', 'Description', 'Lines.1.Item', 'Lines.1.Account', 'Lines.1.CapitalAccount',
            'Lines.1.SubAccount', 'Lines.1.SpecialAccount', 'Lines.1.FixedAsset', 'Lines.1.IntangibleAsset',
            'Lines.1.LineDescription', 'Lines.1.Qty', 'Lines.1.SalesUnitPrice', 'Lines.1.CurrencyAmount',
            'Lines.1.DiscountPercentage', 'Lines.1.DiscountAmount', 'Lines.1.TaxCode', 'Lines.1.Project',
            'Lines.1.Division', 'HasLineNumber', 'HasLineDescription', 'Discount', 'DiscountType',
            'AmountsIncludeTax', 'Rounding', 'RoundingMethod', 'WithholdingTax', 'WithholdingTaxType',
            'WithholdingTaxPercentage', 'WithholdingTaxAmount', 'EarlyPaymentDiscount', 'EarlyPaymentDiscountType',
            'EarlyPaymentDiscountRate', 'EarlyPaymentDiscountAmount', 'EarlyPaymentDiscountDays', 'LatePaymentFees',
            'LatePaymentFeesPercentage', 'TotalAmountInWords', 'TotalAmountInBaseCurrency', 'Bilingual',
            'HasSalesInvoiceCustomTitle', 'SalesInvoiceCustomTitle', 'HasSalesInvoiceCustomTheme',
            'SalesInvoiceCustomTheme', 'AutomaticReference', 'HideDueDate', 'HideBalanceDue', 'ClosedInvoice',
            'ShowItemImages', 'ShowTaxAmountColumn', 'AlsoActsAsDeliveryNote', 'SalesInventoryLocation',
            'HasSalesInvoiceFooters', 'HasRelay', 'Relay'
        ])

        # Map values with error handling
        print("Mapping values...")
        output_df['IssueDate'] = pd.to_datetime(df['Order Date'], dayfirst=True).dt.strftime('%Y-%m-%d')
        output_df['Reference'] = df['Bill No'].astype(str)
        output_df['Customer'] = df['Table Name'].apply(map_customer)
        output_df['Lines.1.Account'] = ACCOUNT_NAME
        output_df['Lines.1.SalesUnitPrice'] = pd.to_numeric(df['Total'], errors='coerce')

        # Set default values
        output_df['Description'] = "Daily Sales"
        output_df['ExchangeRateIsInverse'] = False
        output_df['HasLineNumber'] = False
        output_df['HasLineDescription'] = False
        output_df['Discount'] = False
        output_df['DiscountType'] = 'Percentage'
        output_df['AmountsIncludeTax'] = False
        output_df['Rounding'] = False
        output_df['RoundingMethod'] = 'None'
        output_df['WithholdingTax'] = False
        output_df['WithholdingTaxType'] = 'Rate'
        output_df['EarlyPaymentDiscount'] = False
        output_df['EarlyPaymentDiscountType'] = 'Percentage'
        output_df['LatePaymentFees'] = False
        output_df['TotalAmountInWords'] = False
        output_df['TotalAmountInBaseCurrency'] = False
        output_df['Bilingual'] = False
        output_df['HasSalesInvoiceCustomTitle'] = True
        output_df['SalesInvoiceCustomTitle'] = 'Invoice'
        output_df['HasSalesInvoiceCustomTheme'] = False
        output_df['AutomaticReference'] = False
        output_df['HideDueDate'] = False
        output_df['HideBalanceDue'] = False
        output_df['ClosedInvoice'] = False
        output_df['ShowItemImages'] = False
        output_df['ShowTaxAmountColumn'] = False
        output_df['AlsoActsAsDeliveryNote'] = False
        output_df['HasSalesInvoiceFooters'] = False
        output_df['HasRelay'] = False

        # Fill remaining columns with empty values
        output_df = output_df.fillna('')

        # Generate output in same directory as input file
        output_dir = os.path.dirname(input_file)
        output_filename = f'output_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
        output_path = os.path.join(output_dir, output_filename)

        print(f"Saving output to {output_path}...")
        output_df.to_csv(output_path, index=False)

        print(f"\nProcess completed successfully!")
        print(f"Total records processed: {len(df)}")
        print(f"Output file created: {output_filename}")

    except Exception as e:
        print(f"\nError: An unexpected error occurred: {str(e)}")
        print("The program will now exit.")
        return


if __name__ == "__main__":
    print("Sales Order Processing Tool")
    print("==========================")

    try:
        input_file = select_input_file()
        process_sales_data(input_file)
    except Exception as e:
        print(f"Error: {str(e)}")

    input("\nPress Enter to exit...")
