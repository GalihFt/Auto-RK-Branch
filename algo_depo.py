# algo_depo.py
import pandas as pd
import numpy as np
import streamlit as st
import utils # Import utils.py

TARGET_BRANCH = "DEPO"

def process_core_depo(df_subset, branch_name):
    """
    Core Logic KHUSUS DEPO (Mengandung Regex & Filter Spesifik Depo)
    """
    df_subset = df_subset.reset_index(drop=True)

    # --- MATCH BS ---
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

    # --- MATCH KEPERLUAN ---
    balanced_keperluan = (
        df_subset
        .groupby("Keperluan")[["Debet", "Kredit"]]
        .sum()
        .query("Debet == Kredit")
        .index
    )
    df_match = df_subset[df_subset["Keperluan"].isin(balanced_keperluan)]
    df_subset = df_subset[~df_subset.index.isin(df_match.index)]

    # --- NOTA ---
    mask_nota = df_subset["Keperluan"].str.contains("PEMBAYARAN ATAS NOTA", na=False)
    df_nota = df_subset[mask_nota].copy()
    df_subset = df_subset[~mask_nota].copy()

    # --- PENARIKAN DANA ---
    mask_dana = (
        df_subset["Keperluan"].str.contains("PENARIKAN DANA VIA ATM", na=False) &
        df_subset["Keperluan"].str.contains("MANDIRI SMART ACCOUNT", na=False)
    )
    df_dana = df_subset[mask_dana].copy()
    df_subset = df_subset[~mask_dana].copy()

    # --- DEPO SPECIAL FILTER ---
    pola_pengotor = r"IDBKM|NOBKM|IDBKK|NOBKK|CABANG:"
    mask_kotor = df_subset["Keperluan"].str.contains(pola_pengotor, regex=True, na=False)
    
    mask_cabang_dp = (
        (df_subset["Keperluan"].str.contains("PEMBAYARAN DPP TUNAI|PEMBAYARAN DPP GIRO") 
         | (df_subset["Keperluan"].str.contains("KODE LAWAN RO") & ~mask_kotor))
        & (df_subset["Dibayarkan (ke/dari)"] == "SPIL KARET")
    )
    mask_pusat_dp = (
        ((df_subset["Keperluan"].str.contains("KODE LAWAN RI") & ~mask_kotor)
         & (df_subset["Dibayarkan (ke/dari)"] == "RELASI"))
        | (df_subset["Keperluan"].str.contains("PENERIMAAN GIRO DENGAN VA") 
           & (df_subset["Dibayarkan (ke/dari)"] == "-"))
    )
    df_dp = df_subset[mask_cabang_dp | mask_pusat_dp].copy()
    df_subset = df_subset[~df_subset.index.isin(df_dp.index)].copy()

    # --- JMU ASD/ASK ---
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

    # --- BKK (ID & NO) ---
    df_matched_bkk = pd.DataFrame()
    # (Simplified for brevity - ID)
    REGEX_BKK_ID = r'(?:ID)?BKK\s*[:\-]?\s*(\d+/\d{4})'
    try: all_matches_bkk = df_subset["Keperluan"].str.extractall(REGEX_BKK_ID)[0].unstack()
    except: all_matches_bkk = pd.DataFrame()
    if not all_matches_bkk.empty:
        df_subset["ID Dokumen"] = df_subset["ID Dokumen"].astype(str)
        for layer in all_matches_bkk.columns:
            if df_subset.empty: break
            df_subset["KODE"] = df_subset.index.map(all_matches_bkk[layer])
            candidates = df_subset[df_subset["KODE"].notna()]["KODE"].unique()
            if len(candidates) == 0: continue
            sum_id = df_subset[df_subset["ID Dokumen"].isin(candidates)].groupby("ID Dokumen")["Net"].sum()
            sum_kode = df_subset[df_subset["KODE"].isin(candidates)].groupby("KODE")["Net"].sum()
            total = sum_id.add(sum_kode, fill_value=0)
            valid = total[total.abs() <= 1].index.tolist()
            if not valid: continue
            mask = (df_subset["ID Dokumen"].isin(valid) | df_subset["KODE"].isin(valid))
            df_matched_bkk = pd.concat([df_matched_bkk, df_subset[mask]])
            df_subset = df_subset[~mask].copy()

    # (NO)
    REGEX_BKK_NO = r'\bNOBKK\s*:\s*([A-Z]{2}\.\d+/\d{2}/\d{4})\b'
    try: all_matches_bkk_no = df_subset["Keperluan"].str.extractall(REGEX_BKK_NO)[0].unstack()
    except: all_matches_bkk_no = pd.DataFrame()
    if not all_matches_bkk_no.empty:
        df_subset["Nomor Dokumen"] = df_subset["Nomor Dokumen"].astype(str)
        for layer in all_matches_bkk_no.columns:
            if df_subset.empty: break
            df_subset["KODE"] = df_subset.index.map(all_matches_bkk_no[layer])
            candidates = df_subset[df_subset["KODE"].notna()]["KODE"].unique()
            if len(candidates) == 0: continue
            sum_id = df_subset[df_subset["Nomor Dokumen"].isin(candidates)].groupby("Nomor Dokumen")["Net"].sum()
            sum_kode = df_subset[df_subset["KODE"].isin(candidates)].groupby("KODE")["Net"].sum()
            if sum_id.empty and sum_kode.empty: continue
            total = sum_id.add(sum_kode, fill_value=0)
            valid = total[total.abs() <= 1].index.tolist()
            if not valid: continue
            mask = (df_subset["Nomor Dokumen"].isin(valid) | df_subset["KODE"].isin(valid))
            df_matched_bkk = pd.concat([df_matched_bkk, df_subset[mask]])
            df_subset = df_subset[~mask].copy()
    if "KODE" in df_matched_bkk.columns: df_matched_bkk.drop(columns=["KODE"],inplace=True)

    # --- BKM (ID & NO) ---
    df_matched_bkm = pd.DataFrame()
    # (ID)
    REGEX_BKM_ID = r'(?:ID)?BKM\s*[:\-]?\s*(\d+/\d{4})'
    try: all_matches_bkm = df_subset["Keperluan"].str.extractall(REGEX_BKM_ID)[0].unstack()
    except: all_matches_bkm = pd.DataFrame()
    if not all_matches_bkm.empty:
        df_subset["ID Dokumen"] = df_subset["ID Dokumen"].astype(str)
        for layer in all_matches_bkm.columns:
            if df_subset.empty: break
            df_subset["KODE"] = df_subset.index.map(all_matches_bkm[layer])
            candidates = df_subset[df_subset["KODE"].notna()]["KODE"].unique()
            if len(candidates) == 0: continue
            sum_id = df_subset[df_subset["ID Dokumen"].isin(candidates)].groupby("ID Dokumen")["Net"].sum()
            sum_kode = df_subset[df_subset["KODE"].isin(candidates)].groupby("KODE")["Net"].sum()
            total = sum_id.add(sum_kode, fill_value=0)
            valid = total[total.abs() <= 1].index.tolist()
            if not valid: continue
            mask = (df_subset["ID Dokumen"].isin(valid) | df_subset["KODE"].isin(valid))
            df_matched_bkm = pd.concat([df_matched_bkm, df_subset[mask]])
            df_subset = df_subset[~mask].copy()
    
    # (NO)
    REGEX_BKM_NO = r'\bNOBKM\s*:\s*([A-Z]{2}\.\d+/\d{2}/\d{4})\b'
    try: all_matches_bkm_no = df_subset["Keperluan"].str.extractall(REGEX_BKM_NO)[0].unstack()
    except: all_matches_bkm_no = pd.DataFrame()
    if not all_matches_bkm_no.empty:
        df_subset["Nomor Dokumen"] = df_subset["Nomor Dokumen"].astype(str)
        for layer in all_matches_bkm_no.columns:
            if df_subset.empty: break
            df_subset["KODE"] = df_subset.index.map(all_matches_bkm_no[layer])
            candidates = df_subset[df_subset["KODE"].notna()]["KODE"].unique()
            if len(candidates) == 0: continue
            sum_id = df_subset[df_subset["Nomor Dokumen"].isin(candidates)].groupby("Nomor Dokumen")["Net"].sum()
            sum_kode = df_subset[df_subset["KODE"].isin(candidates)].groupby("KODE")["Net"].sum()
            if sum_id.empty and sum_kode.empty: continue
            total = sum_id.add(sum_kode, fill_value=0)
            valid = total[total.abs() <= 1].index.tolist()
            if not valid: continue
            mask = (df_subset["Nomor Dokumen"].isin(valid) | df_subset["KODE"].isin(valid))
            df_matched_bkm = pd.concat([df_matched_bkm, df_subset[mask]])
            df_subset = df_subset[~mask].copy()
    if "KODE" in df_matched_bkm.columns: df_matched_bkm.drop(columns=["KODE"],inplace=True)

    # --- SA & JURNAL ---
    mask_SA = df_subset["Keperluan"].str.contains(r"^(?:MANDIRI SMART ACCOUNT|PENARIKAN DANA VIA)", case=False, na=False)
    df_SA = df_subset[mask_SA].copy()
    df_subset = df_subset[~mask_SA].copy()

    df_subset['KODE'] = np.nan
    df_subset['KODE'] = df_subset['Keperluan'].str.extract(r'^((?:JM[UH]|[A-Z]{2}\.)\d\S*)')[0]
    df_subset['KODE'] = np.where(df_subset["KODE"].isna(), df_subset["Nomor Dokumen"], df_subset["KODE"])
    sum_per_kode = df_subset.groupby('KODE')['Net'].transform('sum')
    mask_jurnal = (df_subset['KODE'].notna()) & (sum_per_kode.abs() < 1e-9)
    df_jurnal = df_subset[mask_jurnal].copy()
    df_subset.drop(columns=['KODE'], inplace=True)
    df_subset = df_subset[~mask_jurnal]

    # --- ATK ---
    mask_atk = (df_subset["Sumber Dokumen"].str.contains(r"PO\.", na=False) & (df_subset["Jenis Dokumen"] == "TTT"))
    df_atk = df_subset[mask_atk].copy()
    df_subset = df_subset[~mask_atk].copy()

    # --- OFFSET & RECON ---
    df_result = utils.find_offset_pairs(df_subset.copy())
    df_matched_tanggal = df_result[df_result['Is_Matched'] == True].sort_values(by='Match_ID')
    index_tanggal = df_matched_tanggal.index
    df_matched_tanggal.drop(columns=["Match_ID", "Is_Matched"], inplace=True)
    df_subset = df_subset[~df_subset.index.isin(index_tanggal)]

    df_recon = utils.reconcile_global_no_group(df_subset, net_col='Net', tolerance=1)
    df_recon = df_recon[df_recon["Match_ID"].notna()]
    df_recon.drop(columns="Match_ID", inplace=True)
    df_subset = df_subset[~df_subset.index.isin(df_recon.index)]

    df_gantung = pd.concat([df_atk, df_subset], axis=0)

    return [
        ("DATA GANTUNG", df_gantung),
        ("DATA VA", df_dp),
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

def run_segmented_depo_logic(df_all):
    """
    Fungsi Utama Depo (Rolling 10 Hari)
    """
    # 1. Konversi Tanggal
    date_col = 'Tanggal Kasir'
    if date_col not in df_all.columns:
        if 'Tanggal' in df_all.columns: date_col = 'Tanggal'
        else:
            st.warning("Kolom 'Tanggal Kasir' tidak ditemukan. Mode Segmentasi dimatikan.")
            return process_core_depo(df_all, TARGET_BRANCH)

    try:
        df_all[date_col] = pd.to_datetime(df_all[date_col], dayfirst=True, errors='coerce')
    except Exception as e:
        st.error(f"Gagal konversi tanggal: {e}")
        return process_core_depo(df_all, TARGET_BRANCH)

    # 2. Definisikan Masking untuk 3 Segmen
    mask_seg1 = (df_all[date_col].dt.day <= 10)
    mask_seg2 = (df_all[date_col].dt.day > 10) & (df_all[date_col].dt.day <= 20)
    mask_seg3 = (df_all[date_col].dt.day > 20)

    segments = [
        ("Segmen 1 (Tgl 1-10)", df_all[mask_seg1].copy()),
        ("Segmen 2 (Tgl 11-20)", df_all[mask_seg2].copy()),
        ("Segmen 3 (Tgl > 20)", df_all[mask_seg3].copy())
    ]

    # 3. Loop Iterasi
    final_matches_collection = {}
    carry_over_gantung = pd.DataFrame()

    for seg_name, seg_df in segments:
        st.info(f"🔹 Memproses {seg_name}...")
        
        input_df = pd.concat([carry_over_gantung, seg_df], ignore_index=True)
        
        if input_df.empty:
            st.write("   ↳ Data kosong, skip.")
            continue
            
        st.write(f"   ↳ Input: {len(input_df)} baris (Carry Over: {len(carry_over_gantung)} + Baru: {len(seg_df)})")
        
        # Call Internal Function
        results = process_core_depo(input_df, TARGET_BRANCH)
        
        for title, df_res in results:
            if title == "DATA GANTUNG":
                carry_over_gantung = df_res.copy()
            else:
                if not df_res.empty:
                    if title not in final_matches_collection:
                        final_matches_collection[title] = []
                    final_matches_collection[title].append(df_res)
    
    st.success(f"✅ Selesai Semua Segmen. Total Gantung Akhir: {len(carry_over_gantung)} baris.")

    # 4. MEMISAHKAN GANTUNG KARET VS CABANG (DEPO)
    if not carry_over_gantung.empty and "Tempat Pembayaran" in carry_over_gantung.columns:
        mask_cabang = carry_over_gantung["Tempat Pembayaran"].astype(str).str.upper().str.contains(TARGET_BRANCH, na=False)
        df_gantung_cabang = carry_over_gantung[mask_cabang].copy()
        df_gantung_karet = carry_over_gantung[~mask_cabang].copy()
    else:
        df_gantung_cabang = pd.DataFrame(columns=carry_over_gantung.columns)
        df_gantung_karet = carry_over_gantung.copy()

    # 5. Konsolidasi Final Output
    final_output = []
    final_output.append(("DATA GANTUNG KARET (PUSAT)", df_gantung_karet))
    final_output.append((f"DATA GANTUNG {TARGET_BRANCH}", df_gantung_cabang))

    urutan_kategori = [
        "DATA VA", "MATCH BS", "MATCH KEPERLUAN", "NOTA", 
        "PENARIKAN DANA", "JMU ASD/ASK", "BKK", "BKM", "MANDIRI SA", 
        "JURNAL MATCH", "OFFSET PAIRS", "RECON OR-TOOLS"
    ]

    for cat in urutan_kategori:
        if cat in final_matches_collection:
            merged = pd.concat(final_matches_collection[cat], ignore_index=True)
            final_output.append((cat, merged))

    return final_output