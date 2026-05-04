"""
ISEC stock code → NSE trading symbol mapping.

ICICI Direct uses internal ISEC codes (e.g. "STABAN" for SBI).
Groww API uses standard NSE symbols (e.g. "SBIN").

⚠️  VERIFY these before live trading — NSE symbols can change
    due to mergers/renames. Cross-check at:
    https://www.nseindia.com/get-quotes/equity
"""

# ISEC code → NSE trading symbol
# Each entry: ISEC_CODE: "NSE_SYMBOL"  # Full company name
ISEC_TO_NSE: dict[str, str] = {
    "LTINFO":  "LTIM",          # LTIMindtree (formerly L&T Infotech)
    "TATMOT":  "TATAMOTORS",    # Tata Motors
    "SHRTRA":  "SHRIRAMFIN",    # Shriram Finance (formerly SRTRANSFIN)
    "ADAENT":  "ADANIENT",      # Adani Enterprises
    "MAHMAH":  "M&M",           # Mahindra & Mahindra
    "ONGC":    "ONGC",          # Oil & Natural Gas Corp
    "NTPC":    "NTPC",          # NTPC Ltd
    "MARUTI":  "MARUTI",        # Maruti Suzuki
    "EICMOT":  "EICHERMOT",     # Eicher Motors
    "TECMAH":  "TECHM",         # Tech Mahindra
    "INDBA":   "INDUSINDBK",    # IndusInd Bank
    "HINDAL":  "HINDALCO",      # Hindalco Industries
    "BAJFI":   "BAJFINANCE",    # Bajaj Finance
    "HCLTEC":  "HCLTECH",       # HCL Technologies
    "HDFSTA":  "HDFCBANK",      # HDFC Bank
    "POWGRI":  "POWERGRID",     # Power Grid Corporation
    "BAFINS":  "BAJAJFINSV",    # Bajaj Finserv
    "JSWSTE":  "JSWSTEEL",      # JSW Steel
    "SUNPHA":  "SUNPHARMA",     # Sun Pharmaceutical
    "TITIND":  "TITAN",         # Titan Company
    "SBILIF":  "SBILIFE",       # SBI Life Insurance
    "GRASIM":  "GRASIM",        # Grasim Industries
    "HERHON":  "HEROMOTOCO",    # Hero MotoCorp
    "ADAPOR":  "ADANIPORTS",    # Adani Ports & SEZ
    "AXIBAN":  "AXISBANK",      # Axis Bank
    "COALIN":  "COALINDIA",     # Coal India
    "BHAAIR":  "BHARTIARTL",    # Bharti Airtel
    "LARTOU":  "LT",            # Larsen & Toubro
    "BHAPET":  "BPCL",          # Bharat Petroleum
    "CIPLA":   "CIPLA",         # Cipla
    "TATGLO":  "TATACONSUM",    # Tata Consumer Products (formerly Tata Global)
    "TATSTE":  "TATASTEEL",     # Tata Steel
    "KOTMAH":  "KOTAKBANK",     # Kotak Mahindra Bank
    "STABAN":  "SBIN",          # State Bank of India
}

# Reverse mapping for reference
NSE_TO_ISEC: dict[str, str] = {v: k for k, v in ISEC_TO_NSE.items()}


def to_nse(isec_code: str) -> str:
    """Convert ISEC code to NSE symbol. Returns input unchanged if not found."""
    return ISEC_TO_NSE.get(isec_code, isec_code)


def to_isec(nse_symbol: str) -> str:
    """Convert NSE symbol to ISEC code. Returns input unchanged if not found."""
    return NSE_TO_ISEC.get(nse_symbol, nse_symbol)
