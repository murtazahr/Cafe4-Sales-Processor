import io

from flask import Flask, request, send_file, render_template, jsonify
import pandas as pd
import os
import logging
from datetime import datetime
from werkzeug.utils import secure_filename
import traceback

app = Flask(__name__)

# Configure logging
logging.basicConfig(
    filename='sales_processor.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Configure upload folder
UPLOAD_FOLDER = 'uploads'
if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max file size

ACCOUNT_NAME = 'b8437235-5411-4cc9-ad8c-427baeea081f'

# Your existing customer mapping and functions here
customer_mapping = {
    'Walk-ins': '2f2f06f6-f59a-40b5-ae57-f67f4ebf19fb',
    'Matsya Guest - Room 1': '8cfb50cb-382f-4672-b117-71ee5d186c44',
    'Matsya Guest - Room 2': '900b174b-7b33-4cae-b944-065c88785da5',
    'Matsya Guest - Room 3': 'e89b8ce7-f0b7-4dce-97e4-880214f395d3',
    'Matsya Guest - Room 4': '3e51c457-324a-482c-b2bd-86c08ebcd067',
    'Matsya Guest - Room 5': 'a1128788-76e3-43d7-9ff7-664771fdb296',
    'Matsya Guest - Room 6': 'a758268d-d12c-487b-aff1-20d3a2226517',
    'Matsya Guest - Room 7': 'a75c7139-16a0-4065-85d4-67e4f142c5f6',
    'Matsya Guest - Room 8': '89444e06-a160-42b6-9153-8de7d5648a61',
    'Matsya Guest - Room 9': 'fb35c66d-b6b7-4513-b299-65fc580a2671',
    'Captain Hooks': '3a627e83-2867-4f9a-80cd-44322d6152bb',
    'Havelock Experience': 'da1a2619-0790-44ad-84b2-70e2210a3e89',
    'Island Quest': 'daf44eff-90a6-45c3-bc9b-80114dfcce18',
    'Abdul Haseeb': '468c229b-40f3-4efe-9bed-51d1a5f06ac9'
}


# Your existing normalize_table_name function
def normalize_table_name(table_name):
    if not isinstance(table_name, str):
        return str(table_name)
    return table_name.replace('\u00A0', ' ').strip()


normalized_customer_mapping = {normalize_table_name(k): v for k, v in customer_mapping.items()}


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


def process_sales_data(df):
    """Process the sales data DataFrame"""
    try:
        logging.info("Starting data processing")

        # Filter out summary rows
        df = df[df['Order Id'].notna() & (df['Order Id'] != '')]

        # Create a proper copy
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

        # Create output dataframe
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

        # Map values
        output_df['IssueDate'] = pd.to_datetime(df['Order Date'], dayfirst=True).dt.strftime('%Y-%m-%d')
        output_df['Reference'] = df['Bill No'].astype(str)
        output_df['Customer'] = df['Table Name'].apply(map_customer)
        output_df['Lines.1.Account'] = ACCOUNT_NAME
        output_df['Lines.1.SalesUnitPrice'] = df['Total']

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

        return output_df

    except Exception as e:
        logging.error(f"Error processing data: {str(e)}\n{traceback.format_exc()}")
        raise


@app.route('/')
def home():
    return render_template('index.html')


@app.route('/process', methods=['POST'])
def process_file():
    try:
        if 'file' not in request.files:
            return jsonify({'error': 'No file uploaded'}), 400

        file = request.files['file']
        if file.filename == '':
            return jsonify({'error': 'No file selected'}), 400

        if not file.filename.endswith('.xlsx'):
            return jsonify({'error': 'Invalid file type. Please upload an Excel file.'}), 400

        # Read file content
        file_content = file.read()

        # Process file
        df = pd.read_excel(io.BytesIO(file_content), header=5, engine='openpyxl')

        # Process the dataframe
        output_df = process_sales_data(df)

        # Convert to CSV in memory
        csv_data = output_df.to_csv(index=False).encode('utf-8')

        # Create BytesIO object
        mem = io.BytesIO()
        mem.write(csv_data)
        mem.seek(0)

        # Return the CSV file from memory
        return send_file(
            mem,
            as_attachment=True,
            download_name=f'output_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv',
            mimetype='text/csv'
        )

    except Exception as e:
        logging.error(f"Error in process_file: {str(e)}\n{traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 10000))
    app.run(host='0.0.0.0', port=port)
