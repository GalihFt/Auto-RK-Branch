# algo_general.py
import pandas as pd
import numpy as np
import utils  # Import file utils.py

def process_branch_reconciliation(df_subset, branch_name):
    """
    Logika Standar untuk Semua Cabang (Kecuali logic khusus Depo)
    """
    df_subset = df_subset.reset_index(drop=True)

    # --- 2. MATCH BS ---
    balanced_keperluan_bs = (
        df_subset[df_subset["Jenis Dokumen"]=="BS"]
        .groupby("Keperluan")[["Debet", "Kredit"]]
        .sum()
        .query("Debet == Kredit")
        .index
    )
    df_match_bs = df_subset[
        (df_subset["Jenis Dokumen"]=="BS") &
        (df_subset["Keperluan"].isin(balanced_keperluan_bs))
    ]
    df_subset = df_subset[~df_subset.index.isin(df_match_bs.index)]

    # --- 3. MATCH KEPERLUAN ---
    balanced_keperluan = (
        df_subset
        .groupby("Keperluan")[["Debet", "Kredit"]]
        .sum()
        .query("Debet == Kredit")
        .index
    )
    df_match = df_subset[df_subset["Keperluan"].isin(balanced_keperluan)]
    df_subset = df_subset[~df_subset.index.isin(df_match.index)]

    # --- 4. PEMBAYARAN ATAS NOTA ---
    mask_nota = df_subset["Keperluan"].str.contains("PEMBAYARAN ATAS NOTA", na=False)
    df_nota = df_subset[mask_nota].copy()
    df_subset = df_subset[~mask_nota].copy()

    # --- 5. PENARIKAN DANA ---
    mask_dana = (
        df_subset["Keperluan"].str.contains("PENARIKAN DANA VIA ATM", na=False) &
        df_subset["Keperluan"].str.contains("MANDIRI SMART ACCOUNT", na=False)
    )
    df_dana = df_subset[mask_dana].copy()
    df_subset = df_subset[~mask_dana].copy()

    # --- 6. JMU ASD_ASK ---
    REGEX_BKK_BKM_ID = r'(?:ID)?BK[KM]\s*[:\-]?\s*(\d+/\d{4})'
    mask_keyword = (df_subset["Keperluan"].str.contains("JMU ASD", na=False) | 
                    df_subset["Keperluan"].str.contains("JMU ASK", na=False))
    df_asd_temp = df_subset[mask_keyword].copy()

    if not df_asd_temp.empty:
        df_asd_temp["KODE"] = df_asd_temp["Keperluan"].str.extract(REGEX_BKK_BKM_ID)[0]
        replacement_values = df_asd_temp.index.astype(str).values
        df_asd_temp["KODE"] = df_asd_temp["KODE"].fillna(pd.Series(replacement_values, index=df_asd_temp.index))
        sum_per_kode = df_asd_temp.groupby('KODE')['Net'].transform('sum')
        mask_balanced = (sum_per_kode.abs() < 1e-9)
        df_asd = df_asd_temp[mask_balanced].copy()
        df_subset = df_subset[~df_subset.index.isin(df_asd.index)].copy()
    else:
        df_asd = pd.DataFrame(columns=df_subset.columns)
    df_asd = df_asd.drop(columns=["KODE"])

    # --- 7. BKK (ID & NO) ---
    df_matched_bkk = pd.DataFrame()
    REGEX_BKK_ID = r'(?:ID)?BKK\s*[:\-]?\s*(\d+/\d{4})'
    try: all_matches_bkk = df_subset["Keperluan"].str.extractall(REGEX_BKK_ID)[0].unstack()
    except: all_matches_bkk = pd.DataFrame()
    
    if not all_matches_bkk.empty:
        for layer in all_matches_bkk.columns:
            if df_subset.empty: break
            df_subset["KODE"] = df_subset.index.map(all_matches_bkk[layer])
            list_candidates = df_subset[df_subset["KODE"].notna()]["KODE"].unique()
            if len(list_candidates) == 0: continue
            
            # Note: Logic General masih pakai Jenis Dokumen 'VO' sesuai kode asli
            sum_id = df_subset[(df_subset["ID Dokumen"].isin(list_candidates)) & (df_subset["Jenis Dokumen"]=='VO')].groupby("ID Dokumen")["Net"].sum()
            sum_kode = df_subset[df_subset["KODE"].isin(list_candidates)].groupby("KODE")["Net"].sum()
            
            total_sum = sum_id.add(sum_kode, fill_value=0)
            valid_ids = total_sum[total_sum.abs() <= 1].index.tolist()
            if not valid_ids: continue

            mask_valid_bkk = (df_subset["ID Dokumen"].isin(valid_ids) | df_subset["KODE"].isin(valid_ids))
            df_current_valid = df_subset[mask_valid_bkk].copy()
            df_matched_bkk = pd.concat([df_matched_bkk, df_current_valid])
            df_subset = df_subset[~mask_valid_bkk].copy()
            
    # BKK NO (Simpel)
    REGEX_BKK_NO = r'\bNOBKK\s*:\s*([A-Z]{2}\.\d+/\d{2}/\d{4})\b'
    try: all_matches_bkk_no = df_subset["Keperluan"].str.extractall(REGEX_BKK_NO)[0].unstack()
    except: all_matches_bkk_no = pd.DataFrame()
    
    if not all_matches_bkk_no.empty:
        for layer in all_matches_bkk_no.columns:
            if df_subset.empty: break
            df_subset["KODE"] = df_subset.index.map(all_matches_bkk_no[layer])
            list_candidates = df_subset[df_subset["KODE"].notna()]["KODE"].unique()
            if len(list_candidates) == 0: continue

            sum_id = df_subset[(df_subset["ID Dokumen"].isin(list_candidates)) & (df_subset["Jenis Dokumen"]=='VO')].groupby("Nomor Dokumen")["Net"].sum()
            sum_kode = df_subset[df_subset["KODE"].isin(list_candidates)].groupby("KODE")["Net"].sum()
            
            total_sum = sum_id.add(sum_kode, fill_value=0)
            valid_ids = total_sum[total_sum.abs() <= 1].index.tolist()
            if not valid_ids: continue

            mask_valid_bkk = (df_subset["Nomor Dokumen"].isin(valid_ids) | df_subset["KODE"].isin(valid_ids))
            df_current_valid = df_subset[mask_valid_bkk].copy()
            df_matched_bkk = pd.concat([df_matched_bkk, df_current_valid])
            df_subset = df_subset[~mask_valid_bkk].copy()
            
    if "KODE" in df_matched_bkk.columns: df_matched_bkk.drop(columns=["KODE"],inplace=True)

    # --- 8. BKM (ID & NO) ---
    df_matched_bkm = pd.DataFrame()
    REGEX_BKM_ID = r'(?:ID)?BKM\s*[:\-]?\s*(\d+/\d{4})'
    try: all_matches_bkm = df_subset["Keperluan"].str.extractall(REGEX_BKM_ID)[0].unstack()
    except: all_matches_bkm = pd.DataFrame()
    
    if not all_matches_bkm.empty:
        for layer in all_matches_bkm.columns:
            if df_subset.empty: break
            df_subset["KODE"] = df_subset.index.map(all_matches_bkm[layer])
            list_candidates = df_subset[df_subset["KODE"].notna()]["KODE"].unique()
            if len(list_candidates) == 0: continue

            # Note: Logic General masih pakai Jenis Dokumen 'VI'
            sum_id = df_subset[(df_subset["ID Dokumen"].isin(list_candidates)) & (df_subset["Jenis Dokumen"]=='VI')].groupby("ID Dokumen")["Net"].sum()
            sum_kode = df_subset[df_subset["KODE"].isin(list_candidates)].groupby("KODE")["Net"].sum()
            
            total_sum = sum_id.add(sum_kode, fill_value=0)
            valid_ids = total_sum[total_sum.abs() <= 1].index.tolist()
            if not valid_ids: continue

            mask_valid_bkm = (df_subset["ID Dokumen"].isin(valid_ids) | df_subset["KODE"].isin(valid_ids))
            df_current_valid = df_subset[mask_valid_bkm].copy()
            df_matched_bkm = pd.concat([df_matched_bkm, df_current_valid])
            df_subset = df_subset[~mask_valid_bkm].copy()

    # BKM NO
    REGEX_BKM_NO = r'\bNOBKM\s*:\s*([A-Z]{2}\.\d+/\d{2}/\d{4})\b'
    try: all_matches_bkm_no = df_subset["Keperluan"].str.extractall(REGEX_BKM_NO)[0].unstack()
    except: all_matches_bkm_no = pd.DataFrame()
    
    if not all_matches_bkm_no.empty:
        for layer in all_matches_bkm_no.columns:
            if df_subset.empty: break
            df_subset["KODE"] = df_subset.index.map(all_matches_bkm_no[layer])
            list_candidates = df_subset[df_subset["KODE"].notna()]["KODE"].unique()
            if len(list_candidates) == 0: continue

            sum_id = df_subset[(df_subset["ID Dokumen"].isin(list_candidates)) & (df_subset["Jenis Dokumen"]=='VI')].groupby("Nomor Dokumen")["Net"].sum()
            sum_kode = df_subset[df_subset["KODE"].isin(list_candidates)].groupby("KODE")["Net"].sum()
            
            total_sum = sum_id.add(sum_kode, fill_value=0)
            valid_ids = total_sum[total_sum.abs() <= 1].index.tolist()
            if not valid_ids: continue

            mask_valid_bkm = (df_subset["Nomor Dokumen"].isin(valid_ids) | df_subset["KODE"].isin(valid_ids))
            df_current_valid = df_subset[mask_valid_bkm].copy()
            df_matched_bkm = pd.concat([df_matched_bkm, df_current_valid])
            df_subset = df_subset[~mask_valid_bkm].copy()

    if "KODE" in df_matched_bkm.columns: df_matched_bkm.drop(columns=["KODE"],inplace=True)

    # --- 11. MANDIRI SMART ACCOUNT ---
    mask_SA = df_subset["Keperluan"].str.contains(r"^(?:MANDIRI SMART ACCOUNT|PENARIKAN DANA VIA)", case=False, na=False)
    df_SA = df_subset[mask_SA].copy()
    if "KODE" in df_SA.columns: df_SA.drop(columns="KODE", inplace=True)
    df_subset = df_subset[~mask_SA].copy()

    # --- 12. Jurnal MATCH ---
    df_subset['KODE'] = np.nan
    df_subset['KODE'] = df_subset['Keperluan'].str.extract(r'^((?:JM[UH]|[A-Z]{2}\.)\d\S*)')[0]
    df_subset['KODE'] = np.where(df_subset["KODE"].isna(), df_subset["Nomor Dokumen"], df_subset["KODE"])
    sum_per_kode = df_subset.groupby('KODE')['Net'].transform('sum')
    mask_jurnal = (df_subset['KODE'].notna()) & (sum_per_kode.abs() < 1e-9)
    df_jurnal = df_subset[mask_jurnal].copy()
    df_subset.drop(columns=['KODE'], inplace=True)
    df_subset = df_subset[~mask_jurnal]

    # --- 13. ATK ---
    mask_atk = (df_subset["Sumber Dokumen"].str.contains(r"PO\.", na=False) & (df_subset["Jenis Dokumen"] == "TTT"))
    df_atk = df_subset[mask_atk].copy()
    df_subset = df_subset[~mask_atk].copy()

    # --- 14. OFFSET PAIR ---
    df_result = utils.find_offset_pairs(df_subset.copy())
    df_matched_tanggal = df_result[df_result['Is_Matched'] == True].sort_values(by='Match_ID')
    index_tanggal = df_matched_tanggal.index
    df_matched_tanggal.drop(columns=["Match_ID", "Is_Matched"], inplace=True)
    df_subset = df_subset[~df_subset.index.isin(index_tanggal)]

    # --- 15. RECON (OR-TOOLS) ---
    df_recon = utils.reconcile_global_no_group(df_subset, net_col='Net', tolerance=1)
    df_recon = df_recon[df_recon["Match_ID"].notna()]
    df_recon.drop(columns="Match_ID", inplace=True)
    df_subset = df_subset[~df_subset.index.isin(df_recon.index)]

    # --- 16. GANTUNG (MODIFIED) ---
    df_gantung = pd.concat([df_atk, df_subset], axis=0)

    # Pisahkan Gantung Cabang vs Karet berdasarkan 'Tempat Pembayaran'
    if not df_gantung.empty and "Tempat Pembayaran" in df_gantung.columns:
        # Filter: Jika mengandung nama cabang (branch_name) maka masuk Gantung Cabang
        mask_cabang = df_gantung["Tempat Pembayaran"].astype(str).str.upper().str.contains(branch_name, na=False)
        df_gantung_cabang = df_gantung[mask_cabang].copy()
        df_gantung_karet = df_gantung[~mask_cabang].copy()
    else:
        df_gantung_cabang = pd.DataFrame(columns=df_gantung.columns)
        df_gantung_karet = df_gantung.copy()

    # Return Result
    return [
        ("DATA GANTUNG KARET (PUSAT)", df_gantung_karet),
        (f"DATA GANTUNG {branch_name}", df_gantung_cabang),
        ("MATCH BS", df_match_bs),
        ("MATCH KEPERLUAN", df_match),
        ("NOTA", df_nota),
        ("PENARIKAN DANA", df_dana),
        ("JMU ASD/ASK", df_asd),
        ("BKK", df_matched_bkk),
        ("BKM", df_matched_bkm),
        ("MANDIRI SA", df_SA),
        ("JURNAL MATCH", df_jurnal),
        ("OFFSET PAIRS", df_matched_tanggal),
        ("RECON OR-TOOLS", df_recon)
    ]