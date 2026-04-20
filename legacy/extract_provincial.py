import pandas as pd
import logging
from pathlib import Path
from decimal import Decimal

# Configure logging to catch errors without crashing silently
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class ProvincialTransformer:
    """
    Handles cleaning, transformation, and auto-categorization of bank data.
    """

    COLUMN_MAPPING = {
        "Fecha": "Fecha",
        "Month": None,
        "Month-week": None,
        "Week": None,
        "Referencia": "Descripción",
        "Concepto": None,
        "Categoría": None,
        "Monto": "Monto",
        "Tipo": None,
        "Tasa del día": None,
        "Monto (BCV)": None,
        "Tasa USDT": None,
        "Monto (USDT)": None,
        "Comentarios": None,
    }

    # Hardcoded map to ensure English output regardless of PC language settings
    MONTH_MAP = {
        1: "Jan",
        2: "Feb",
        3: "Mar",
        4: "Apr",
        5: "May",
        6: "Jun",
        7: "Jul",
        8: "Aug",
        9: "Sep",
        10: "Oct",
        11: "Nov",
        12: "Dec",
    }

    def clean_currency(self, value: str) -> Decimal:
        """
        Parses "-3.095,16", "Bs. -900,00" or "Bs -900,00" into Decimal.
        """
        if pd.isna(value) or str(value).strip() == "":
            return Decimal("0.00")

        # 1. Convert to string, lowercase, and strip outer whitespace
        clean_val = str(value).lower().strip()

        # 2. Remove "bs." and "bs" prefixes (and any resulting whitespace)
        clean_val = clean_val.replace("bs.", "").replace("bs", "").strip()

        # 3. Remove thousands separator (.) first!
        # Example: "1.966,55" -> "1966,55"
        clean_val = clean_val.replace(".", "")

        # 4. Replace decimal separator (,) with standard dot (.)
        # Example: "1966,55" -> "1966.55"
        clean_val = clean_val.replace(",", ".")

        try:
            return Decimal(clean_val)
        except Exception:
            # We log the *original* value to know exactly what failed
            logging.error(f"Could not parse currency value: '{value}'")
            return Decimal("0.00")

    def format_date(self, date_str: str) -> str:
        """
        Parses d/mm/yyyy -> dd-Mmm-yyyy (English forced).
        """
        try:
            dt = pd.to_datetime(date_str, dayfirst=True)
            # manual formatting to guarantee English months
            month_str = self.MONTH_MAP[dt.month]
            return f"{dt.day:02d}-{month_str}-{dt.year}"
        except Exception as e:
            logging.error(f"Date parse error for {date_str}: {e}")
            return date_str

    def apply_categorization_rules(self, row):
        """
        Logic to auto-fill Concepto and Categoría based on Referencia.
        """
        ref = str(row["Referencia"]).upper()

        # Rule 1: Comision Pago Movil
        if ref.startswith("COM. PAGO MO"):
            return pd.Series(["Comisión pago móvil", "Commissions"])

        # Default: Return existing empty values (or whatever was there)
        return pd.Series(["", ""])

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        # 1. Reverse order
        df = df.iloc[::-1].reset_index(drop=True)

        # 2. Clean Currency (to Decimal)
        df["Monto"] = df["Monto"].astype(str).map(self.clean_currency)

        # 3. Format Dates
        df["Fecha"] = df["Fecha"].apply(self.format_date)

        # 4. Map Columns
        output_df = pd.DataFrame()
        for target_col, source_col in self.COLUMN_MAPPING.items():
            if source_col and source_col in df.columns:
                output_df[target_col] = df[source_col]
            else:
                output_df[target_col] = ""

        # 5. Apply Auto-Categorization
        # We apply this to the output dataframe columns
        output_df[["Concepto", "Categoría"]] = output_df.apply(
            self.apply_categorization_rules, axis=1
        )

        return output_df


class FileOrchestrator:
    """
    Handles File I/O operations.
    Follows OCP (Open/Closed Principle): New file sources can be added by extending this.
    """

    def __init__(self, input_dir: str = "inputs", output_dir: str = "output"):
        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir)
        self._ensure_directories()

    def _ensure_directories(self):
        self.output_dir.mkdir(parents=True, exist_ok=True)
        if not self.input_dir.exists():
            self.input_dir.mkdir(parents=True)
            logging.warning(f"Created missing input directory: {self.input_dir}")

    def get_input_files(self, pattern: str) -> list[Path]:
        return list(self.input_dir.glob(pattern))

    def save_output(self, df: pd.DataFrame, base_name: str):
        # Generate paths
        csv_path = self.output_dir / f"clean_{base_name}.csv"
        json_path = self.output_dir / f"clean_{base_name}.json"

        # Save CSV (index=False to avoid saving the row numbers)
        df.to_csv(csv_path, index=False, encoding="utf-8")

        # Save JSON (orient='records' creates a list of objects, best for APIs)
        df.to_json(json_path, orient="records", date_format="iso", indent=4)

        logging.info(f"Saved: {csv_path.name} and {json_path.name}")


def run_extraction():
    orchestrator = FileOrchestrator()
    transformer = ProvincialTransformer()

    # 1. Find Files
    # Get all CSVs. We don't filter by "provincial_" here strictly
    # so we can catch misnamed files if needed, but sticking to pattern is safer.
    files = orchestrator.get_input_files("provincial_*.csv")

    if not files:
        logging.warning("No files found in 'inputs/' matching 'provincial_*.csv'.")
        return

    # 2. Select Latest File (Sort by modification time, descending)
    # This solves the "Order" and "Latest File" requirement
    latest_file = max(files, key=lambda f: f.stat().st_mtime)

    logging.info(f"Selected latest file: {latest_file.name}")

    try:
        # 3. Extract
        raw_df = pd.read_csv(latest_file, sep=";", encoding="utf-8-sig")

        # 4. Transform
        clean_df = transformer.transform(raw_df)

        # 5. Load (Save)
        # Robust naming: If filename starts with provincial_, remove it.
        # Otherwise just use the whole stem.
        if latest_file.stem.lower().startswith("provincial_"):
            file_suffix = latest_file.stem[11:]  # cuts off "provincial_"
        else:
            file_suffix = latest_file.stem

        orchestrator.save_output(clean_df, f"provincial_{file_suffix}")

    except Exception as e:
        logging.error(f"Failed to process {latest_file.name}: {e}", exc_info=True)


if __name__ == "__main__":
    run_extraction()
