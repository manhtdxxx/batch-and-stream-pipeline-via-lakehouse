from vnstock import Finance, Listing, Quote, Company
import pandas as pd
from typing import List, Union, Optional
from datetime import datetime
import time
import os
import warnings
warnings.filterwarnings("ignore")

# HELPER FUNCTION
def get_objects(symbols: list[str], obj_type: str, source: str = "VCI") -> List[Union[Company, Quote, Finance]]:
    if obj_type.lower() == "company":
        cls = Company
    elif obj_type.lower() == "quote":
        cls = Quote
    elif obj_type.lower() == "finance":
        cls = Finance
    else:
        raise ValueError(f"Unknown obj_type: {obj_type}")
    return [cls(symbol=s, source=source) for s in symbols]

# GET SYMBOL
def get_symbols(symbol_group:str, source: str = "VCI") -> list[str]:
    print(f"Fetching symbols for group {symbol_group}...")
    listing = Listing(source=source)
    symbols = listing.symbols_by_group(symbol_group).to_list() # returns pd.Series, then converts to list
    print(f"{len(symbols)} symbols fetched.")
    return symbols

# GET COMPANY RELATED TO SYMBOL
def get_company_df(symbols: list[str], source: str = "VCI") -> pd.DataFrame:
    print("Fetching company basic info...")
    listing = Listing(source=source)
    df_company = listing.symbols_by_industries()
    # filter company related to symbol
    df_company = df_company[df_company["symbol"].isin(symbols)].reset_index(drop=True)
    # select cols and sort by symbol
    cols_to_select = ["symbol", "organ_name", "icb_code1", "icb_code2", "icb_code3", "icb_code4"]
    df_company = df_company[cols_to_select].sort_values(by=["symbol"], ascending=[True]).reset_index(drop=True)
    print(f"Company basic info fetched ({len(df_company)} rows).")
    return df_company

# GET INDUSTRY
def get_industry_df(source: str = "VCI") -> pd.DataFrame:
    print("Fetching industry info...")
    listing = Listing(source=source)
    df_industry = listing.industries_icb()
    # select cols and sort by code
    cols_to_select = ["icb_code", "level", "icb_name", "en_icb_name"]
    df_industry = df_industry[cols_to_select].sort_values(by=["icb_code"], ascending=[True]).reset_index(drop=True)
    print(f"Industry info fetched ({len(df_industry)} rows).")
    return df_industry

# GET COMPANY INFO
def get_company_info_df(company_objects: List[Company]) -> pd.DataFrame:
    print("Fetching company detailed info...")
    dfs = []
    for obj in company_objects:
        df = obj.overview()
        df["symbol"] = obj.symbol
        dfs.append(df)
    df_all = pd.concat(dfs, axis=0, ignore_index=True)
    print(f"Company detailed info fetched ({len(df_all)} rows).")
    return df_all

# GET OHLCV (OPEN - HIGH - LOW - CLOSE - VOLUME)
def get_ohlcv_df(quote_objects: list[Quote], start_date: Union[str, datetime], end_date: Optional[Union[str, datetime]] = None, interval: str = "1d") -> pd.DataFrame:

    if isinstance(start_date, datetime):
        start_date = start_date.strftime("%Y-%m-%d")
    if end_date is None:
        end_date = datetime.today().strftime("%Y-%m-%d")
    elif isinstance(end_date, datetime):
        end_date = end_date.strftime("%Y-%m-%d")

    print(f"Fetching OHLCV ({interval}) from {start_date} to {end_date}...")
    dfs = []
    for obj in quote_objects:
        df = obj.history(start=start_date, end=end_date, interval=interval)
        df["symbol"] = obj.symbol
        dfs.append(df)
    df_all = pd.concat(dfs, axis=0, ignore_index=True)
    # select cols and sort by time
    cols_to_select = ["symbol", "time", "open", "high", "low", "close", "volume"]
    df_all = df_all[cols_to_select].sort_values(by=["time"], ascending=[True]).reset_index(drop=True)
    print(f"OHLCV ({interval}) fetched ({len(df_all)} rows).")
    return df_all 

# GET FINANCIAL RATIOS
def get_ratio_df(finance_objects: list[Finance], start_year: int, end_year: Optional[int] = None) -> pd.DataFrame:
    print(f"Fetching financial ratios from {start_year} to {end_year if end_year else 'latest'}...")
    dfs = []
    for obj in finance_objects:
        df = obj.ratio(period='year', lang='en')
        dfs.append(df)
    df_all = pd.concat(dfs, axis=0, ignore_index=True)
    # 2 headers for a col, so I pick one 
    if isinstance(df_all.columns, pd.MultiIndex):
        df_all.columns = [(col[1] if col[1] != "" else col[0]).strip() for col in df_all.columns]
    # filter by year
    if end_year is None:
        df_all = df_all[df_all["yearReport"] >= start_year]
    else:
        df_all = df_all[(df_all["yearReport"] >= start_year) & (df_all[df_all["yearReport"] <= end_year])]
    # select cols and sort by year
    cols_to_select = ["ticker", "yearReport", 
                      "ROE (%)", "ROA (%)", "Net Profit Margin (%)", "Financial Leverage", 
                      "Market Capital (Bn. VND)", "Outstanding Share (Mil. Shares)", 
                      "Dividend yield (%)", "EPS (VND)", "BVPS (VND)",
                      "P/E", "P/B", "P/S", "P/Cash Flow"
                    ]
    df_all = df_all[cols_to_select].sort_values(by=["yearReport"], ascending=[True]).reset_index(drop=True)
    print(f"Financial ratios fetched ({len(df_all)} rows).")
    return df_all


# GET ALL DATA RELATED TO SYMBOL (COMPANY, INDUSTRY, OHLCV, FINANCIAL RATIOS)
def get_all_data(symbol_group):
    symbols = get_symbols(symbol_group=symbol_group)
    company_objects = get_objects(symbols, "company")
    quote_objects = get_objects(symbols, "quote")
    finance_objects = get_objects(symbols, "finance")

    # Get Company
    df_company = get_company_df(symbols)
    df_company_info = get_company_info_df(company_objects)
    df_company_info = df_company_info[["symbol", "issue_share"]]
    df_company_v2 = df_company.merge(df_company_info, on="symbol", how="inner")
    time.sleep(30)
    # Get Industry
    df_industry = get_industry_df()
    time.sleep(30)
    # Get OHLCV
    start_date = "2020-01-01"
    end_date = datetime.today().strftime("%Y-%m-%d")
    df_ohlcv_1d = get_ohlcv_df(quote_objects, start_date=start_date, end_date=end_date, interval="1D")
    time.sleep(60)
    df_ohlcv_1m = get_ohlcv_df(quote_objects, start_date=start_date, end_date=end_date, interval="1m")
    time.sleep(120)
    # Get Financial Ratios
    start_year = 2020
    df_ratio = get_ratio_df(finance_objects, start_year)

    return df_industry, df_company_v2, df_ohlcv_1d, df_ohlcv_1m, df_ratio


# EXPORT TO CSV
def export_to_csv(df: pd.DataFrame, file_name: str):
    data_folder = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(data_folder, file_name)
    print(f"Exporting {file_path}...")
    df.to_csv(file_path, index=False)
    print(f"CSV export completed: {file_path}")


# ======== MAIN ========
if __name__ == "__main__":
    
    symbol_group = "VN30"
    df_industry, df_company, df_ohlcv_1d, df_ohlcv_1m, df_ratio = get_all_data(symbol_group)

    export_to_csv(df_industry, "industry.csv")
    export_to_csv(df_company, "company.csv")
    export_to_csv(df_ohlcv_1d, "ohlcv_1d.csv")
    export_to_csv(df_ohlcv_1m, "ohlcv_1m.csv")
    export_to_csv(df_ratio, "financial_ratio.csv")

    print("All Done!")
