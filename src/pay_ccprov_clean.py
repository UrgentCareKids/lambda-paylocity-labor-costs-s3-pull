import os
import pandas as pd

def clean_ccprov_and_ccstaff(ccprov_paths, ccstaff_paths, out_dir="/tmp"):
    """
    ccprov_paths: list[str] local .xls paths for ccprov files
    ccstaff_paths: list[str] local .xls paths for ccstaff files
    returns: (ccprov_csv_path, ccstaff_csv_path)
    """

    def _merge_clean_excel(paths):
        dfs = []
        for p in paths:
            df = pd.read_excel(p, skiprows=5, engine='openpyxl')
            df_cleaned = df.drop(df.columns[2], axis=1)
            df_cleaned.reset_index(drop=True, inplace=True)
            dfs.append(df_cleaned)
        return pd.concat(dfs, ignore_index=True)

    ccprov_df = _merge_clean_excel(ccprov_paths)
    ccstaff_df = _merge_clean_excel(ccstaff_paths)

    ccprov_out = os.path.join(out_dir, "cleaned_clinic_ccprov.csv")
    ccstaff_out = os.path.join(out_dir, "cleaned_clinic_ccstaff.csv")

    ccprov_df.to_csv(ccprov_out, index=False)
    ccstaff_df.to_csv(ccstaff_out, index=False)

    return ccprov_out, ccstaff_out
