import pandas as pd


def update_df(df: pd.DataFrame, df_update: pd.DataFrame) -> pd.DataFrame:
    if not df_update.empty:
        if df.empty:
            return df_update
        else:
            return pd.concat((df, df_update))
    else:
        return df