import os
import csv
import re
from datetime import datetime
from bs4 import BeautifulSoup

# Configuration
INPUT_FILE = 'table_bcv.html'
OUTPUT_DIR = 'output'
TODAY = datetime.now().strftime('%Y-%m-%d')
OUTPUT_FILE = os.path.join(OUTPUT_DIR, f'bcv_{TODAY}.csv')

# Spanish to Month Number mapping
MONTH_MAP = {
    'enero': 1, 'febrero': 2, 'marzo': 3, 'abril': 4,
    'mayo': 5, 'junio': 6, 'julio': 7, 'agosto': 8,
    'septiembre': 9, 'octubre': 10, 'noviembre': 11, 'diciembre': 12
}

# Output English mapping for the specific format requested (01-Feb-2026)
# using a manual map ensures consistency regardless of the system locale.
OUT_MONTH_MAP = {
    1: 'Jan', 2: 'Feb', 3: 'Mar', 4: 'Apr', 5: 'May', 6: 'Jun',
    7: 'Jul', 8: 'Aug', 9: 'Sep', 10: 'Oct', 11: 'Nov', 12: 'Dec'
}

def clean_currency(text_value):
    """
    Extracts the number from strings like 'Bs.S 396,36'
    Returns: float
    """
    # Remove 'Bs.S', whitespace, and non-breaking spaces
    clean = text_value.replace('Bs.S', '').replace('\xa0', '').strip()
    # Extract only the number part (ignoring percentage arrows if captured)
    # Regex looks for digits, a comma, and digits
    match = re.search(r'(\d+,\d+)', clean)
    if match:
        # Replace decimal comma with dot
        return float(match.group(1).replace(',', '.'))
    return 0.0

def parse_spanish_date(date_str):
    """
    Parses 'Miércoles, 18 de febrero de 2026' into a datetime object.
    """
    # Split by comma to ignore the Day Name (Miércoles)
    parts = date_str.split(',')
    if len(parts) < 2:
        return None
    
    # Take the second part: " 18 de febrero de 2026"
    clean_part = parts[1].strip()
    date_components = clean_part.split(' de ')
    
    if len(date_components) != 3:
        return None
        
    day = int(date_components[0])
    month_name = date_components[1].lower()
    year = int(date_components[2])
    
    month_num = MONTH_MAP.get(month_name, 1)
    
    return datetime(year, month_num, day)

def main():
    # 1. Ensure output directory exists
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # 2. Read the HTML file
    if not os.path.exists(INPUT_FILE):
        print(f"Error: {INPUT_FILE} not found.")
        return

    with open(INPUT_FILE, 'r', encoding='utf-8') as f:
        soup = BeautifulSoup(f, 'html.parser')

    extracted_data = []

    # 3. Locate the table body rows
    rows = soup.find('tbody').find_all('tr')

    for row in rows:
        cols = row.find_all('td')
        if len(cols) < 3:
            continue

        # Extract Raw Text
        date_raw = cols[0].get_text(strip=True)
        
        # Note: The currency value is inside the first <span> in the cell
        # We target the specific span to avoid grabbing the percentage text
        usd_raw = cols[1].find('span').get_text(strip=True)
        euro_raw = cols[2].find('span').get_text(strip=True)

        # Process Data
        date_obj = parse_spanish_date(date_raw)
        usd_val = clean_currency(usd_raw)
        euro_val = clean_currency(euro_raw)

        if date_obj:
            extracted_data.append({
                'date_obj': date_obj,
                'usd': usd_val,
                'euro': euro_val
            })

    # 4. Sort data by date ascending (Oldest to Newest)
    extracted_data.sort(key=lambda x: x['date_obj'])

    # 5. Write to CSV
    with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['Dia', 'USD', 'EURO']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames, quoting=csv.QUOTE_MINIMAL)

        writer.writeheader()
        
        for entry in extracted_data:
            # Format date as requested: 01-Feb-2026
            d = entry['date_obj']
            formatted_date = f"{d.day:02d}-{OUT_MONTH_MAP[d.month]}-{d.year}"
            
            writer.writerow({
                'Dia': formatted_date,
                'USD': entry['usd'],
                'EURO': entry['euro']
            })

    print(f"Successfully processed {len(extracted_data)} records.")
    print(f"File saved to: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()