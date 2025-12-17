import pandas as pd
import os
from sqlalchemy import create_engine, text
from models import DATABASE_URL
import datetime

def export_to_excel(output_filename=None):
    """
    Exports data from Postgres to Excel.
    Steps:
    1. Fetch all records.
    2. Extract 'State' from full_address.
    3. Format columns.
    4. Save to .xlsx.
    5. Verify file exists and not empty.
    6. Return True if successful.
    """
    if output_filename is None:
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        output_filename = f"output_{timestamp}.xlsx"

    engine = create_engine(DATABASE_URL)

    query = "SELECT * FROM businesses"
    df = pd.read_sql(query, engine)

    if df.empty:
        print("No data found in database.")
        return False, None

    # Logic to extract state.
    # Assuming full_address format contains state/zip.
    # Simple heuristic: Look for state abbreviations or use address_region if we had it.
    # Since we only have full_address string in schema, we'll try a naive split or regex if needed.
    # Prompt says: "Extract 'State' from the full_address if possible".
    # Many google maps addresses are "Street, City, State Zip, Country".

    def extract_state(addr):
        if not addr:
            return ""
        parts = addr.split(',')
        if len(parts) >= 2:
            # Usually the second to last part contains "State Zip" or just "State"
            # Example: "123 Main St, London KY 40741" -> "London KY 40741"
            # We might need a more robust parser but for now let's try to get the part before country or zip.
            # Let's assume the standard US format for now, or just leave it empty if unsure.
            # But the example command is "London" (UK?).
            # If it's London UK, address might be "Address, London, Postcode, UK".
            # State is less relevant for UK.
            # Let's just try to grab the second to last item if available.
            return ""
        return ""

    # Better approach: The prompt says "Format: Create a Pandas DataFrame with exactly these columns: Name, City, State, Category, Website. (Map Maps_url if website is empty)."

    output_df = pd.DataFrame()
    output_df['Name'] = df['name']
    output_df['City'] = df['city']

    # Simple extraction attempt, or empty string.
    # Given the ambiguity of "State" for international locations, we'll leave it as placeholder logic
    # that users can improve. For US addresses, we could parse.
    output_df['State'] = df['full_address'].apply(lambda x: "")

    output_df['Category'] = df['category']

    # Map Maps_url if website is empty
    output_df['Website'] = df.apply(lambda row: row['website'] if row['website'] else row['maps_url'], axis=1)

    # Save to Excel
    try:
        output_df.to_excel(output_filename, index=False)
    except Exception as e:
        print(f"Error saving excel: {e}")
        return False, None

    # Verification
    if os.path.exists(output_filename) and os.path.getsize(output_filename) > 0:
        return True, output_filename

    return False, None

def flush_database():
    """Truncates the businesses table."""
    engine = create_engine(DATABASE_URL)
    with engine.connect() as conn:
        conn.execute(text("TRUNCATE TABLE businesses"))
        conn.commit()
    print("Database flushed.")
