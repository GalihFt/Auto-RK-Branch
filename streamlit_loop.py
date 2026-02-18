import streamlit as st
import pandas as pd
import numpy as np
import io
import re
from ortools.linear_solver import pywraplp

# ==========================================
# 1. DEFINISI MAPPING CABANG
# ==========================================
BRANCH_MAPPING = {
    "AMBON": "HUTANG/PIUTANG AFILIASI AMBON",
    "BALIKPAPAN": "HUTANG/PIUTANG AFILIASI BPP",
    "BANGKA": "HUTANG/PIUTANG AFILIASI BANGKA",
    "BANJARMASIN": "HUTANG/PIUTANG AFILIASI BMS",
    "BATAM": "HUTANG/PIUTANG AFILIASI BATAM",
    "BATULICIN": "HUTANG/PIUTANG AFILIASI BTL",
    "BAU - BAU": "HUTANG/PIUTANG AFILIASI BAU-BAU",
    "BERAU": "HUTANG/PIUTANG AFILIASI BERAU",
    "BIAK": "HUTANG/PIUTANG AFILIASI BIA",
    "BINTUNI": "HUTANG/PIUTANG AFILIASI BINTUNI",
    "BITUNG": "HUTANG/PIUTANG AFILIASI BITUNG",
    "BUNGKU": "HUTANG/PIUTANG AFILIASI BUNGKU",
    "DEPO": "HUTANG/PIUTANG AFILIASI DEPO",
    "FAK - FAK": "HUTANG/PIUTANG AFILIASI FAK-FAK",
    "GORONTALO": "HUTANG/PIUTANG AFILIASI GORONTALO",
    # "JAKARTA": "HUTANG/PIUTANG AFILIASI JKT",
    "JAYAPURA": "HUTANG/PIUTANG AFILIASI JYP",
    "KAIMANA": "HUTANG/PIUTANG AFILIASI KAIMANA",
    "KENDARI": "HUTANG/PIUTANG AFILIASI KENDARI",
    "KETAPANG": "HUTANG/PIUTANG AFILIASI KTG",
    "LUWUK": "HUTANG/PIUTANG AFILIASI LUWUK",
    "MAKASSAR": "HUTANG/PIUTANG AFILIASI MAKASSAR",
    "MANOKWARI": "HUTANG/PIUTANG AFILIASI MRI",
    "MEDAN": "HUTANG/PIUTANG AFILIASI MDN",
    "MERAUKE": "HUTANG/PIUTANG AFILIASI MKE",
    "NABIRE": "HUTANG/PIUTANG AFILIASI NABIRE",
    "NUNUKAN": "HUTANG/PIUTANG AFILIASI NUNUKAN",
    "PADANG": "HUTANG/PIUTANG AFILIASI PADANG",
    "PALEMBANG": "HUTANG/PIUTANG AFILIASI PALEMBANG",
    "PALU": "HUTANG/PIUTANG AFILIASI PALU",
    "PEKANBARU": "HUTANG/PIUTANG AFILIASI PEKAN BARU",
    "PONTIANAK": "HUTANG/PIUTANG AFILIASI PONTIANAK",
    "SAMARINDA": "HUTANG/PIUTANG AFILIASI SMD",
    "SAMPIT": "HUTANG/PIUTANG AFILIASI SAMPIT",
    "SEMARANG": "HUTANG/PIUTANG AFILIASI SEMARANG",
    "SERUI": "HUTANG/PIUTANG AFILIASI SERUI",
    "SORONG": "HUTANG/PIUTANG AFILIASI SRG",
    #"SURABAYA": "HUTANG/PIUTANG AFILIASI SBY",
    "TARAKAN": "HUTANG/PIUTANG AFILIASI TRK",
    "TERNATE": "HUTANG/PIUTANG AFILIASI TERNATE",
    "TIMIKA": "HUTANG/PIUTANG AFILIASI TIMIKA",
    "TUAL": "HUTANG/PIUTANG AFILIASI TUAL"
}

# ==========================================
# 2. HELPER FUNCTIONS (CORE ALGO - TIDAK DIUBAH)
# ==========================================

def load_excel_with_header_detection(file, sheet_idx):
    try:
        df_raw = pd.read_excel(file, sheet_name=sheet_idx, header=None)
        header_idx = None
        for idx, row in df_raw.iterrows():
            if row.count() >= 5: 
                header_idx = idx
                break
        if header_idx is None:
            st.error(f"Tidak dapat menemukan header valid (>= 5 kolom terisi) di Sheet index {sheet_idx}.")
            return None

        raw_headers = df_raw.iloc[header_idx].values
        headers_clean = [str(x).strip() if pd.notna(x) else f"Unnamed_{i}" for i, x in enumerate(raw_headers)]
        
        seen_counts = {}
        final_headers = []
        for col in headers_clean:
            if col in seen_counts:
                seen_counts[col] += 1
                final_headers.append(f"{col}.{seen_counts[col]}")
            else:
                seen_counts[col] = 0
                final_headers.append(col)

        df_final = df_raw.iloc[header_idx + 1:].copy()
        df_final.columns = final_headers
        df_final.reset_index(drop=True, inplace=True)
        
        cols_to_check = ["Dibayarkan (ke/dari)", "Keperluan"]
        existing_cols = [c for c in cols_to_check if c in df_final.columns]
        if existing_cols:
            df_final.dropna(subset=existing_cols, inplace=True)
        
        return df_final

    except Exception as e:
        st.error(f"Error reading sheet {sheet_idx}: {str(e)}")
        return None

def find_offset_pairs(df):
    df['Net'] = df['Net'].astype(float).round(2)
    df['Tanggal Delivery'] = pd.to_datetime(df['Tanggal Delivery'], dayfirst=True)
    df['Match_ID'] = None        
    df['Is_Matched'] = False      
    match_counter = 1
    
    for tanggal, group in df.groupby('Tanggal Delivery'):
        positives = group[(group['Net'] > 0) & (~group['Is_Matched'])]
        for idx_pos, row_pos in positives.iterrows():
            if df.at[idx_pos, 'Is_Matched']: continue
            target_val = -row_pos['Net']
            current_loc = row_pos['Tempat Pembayaran']
            candidates = df[
                (df['Net'] == target_val) & 
                (df['Tanggal Delivery'] == tanggal) &
                (df['Is_Matched'] == False) &
                (df['Tempat Pembayaran'] != current_loc)
            ]
            if not candidates.empty:
                idx_neg = candidates.index[0]
                match_id = f"MATCH_{match_counter:04d}"
                df.at[idx_pos, 'Is_Matched'] = True
                df.at[idx_pos, 'Match_ID'] = match_id
                df.at[idx_neg, 'Is_Matched'] = True
                df.at[idx_neg, 'Match_ID'] = match_id
                match_counter += 1
    return df

def solve_subset_sum(values, tolerance=1.0, time_limit_ms=7000):
    solver = pywraplp.Solver.CreateSolver('SCIP')
    if not solver: return []
    n = len(values)
    x = [solver.IntVar(0, 1, f'x_{i}') for i in range(n)]
    constraint = solver.RowConstraint(-tolerance, tolerance, 'sum_constraint')
    for i in range(n): constraint.SetCoefficient(x[i], values[i])
    min_items = solver.RowConstraint(2, solver.infinity(), 'min_items')
    for i in range(n): min_items.SetCoefficient(x[i], 1)
    objective = solver.Objective()
    for i in range(n): objective.SetCoefficient(x[i], 1)
    objective.SetMaximization()
    solver.SetTimeLimit(time_limit_ms)
    status = solver.Solve()
    selected_indices = []
    if status == pywraplp.Solver.OPTIMAL or status == pywraplp.Solver.FEASIBLE:
        for i in range(n):
            if x[i].solution_value() > 0.5: selected_indices.append(i)
    return selected_indices

def reconcile_global_no_group(df, net_col='Net', tolerance=1.0):
    df = df.copy()
    df[net_col] = pd.to_numeric(df[net_col], errors='coerce').fillna(0)
    if 'Match_ID' not in df.columns: df['Match_ID'] = None
    match_counter = 1
    while True:
        unmatched_indices = df[df['Match_ID'].isnull()].index.tolist()
        current_values = df.loc[unmatched_indices, net_col].tolist()
        if len(current_values) < 2: break
        local_indices = solve_subset_sum(current_values, tolerance=tolerance, time_limit_ms=5000)
        if not local_indices: break 
        real_indices = [unmatched_indices[i] for i in local_indices]
        match_id = f"GLOBAL_MATCH_{match_counter:04d}"
        df.loc[real_indices, 'Match_ID'] = match_id
        match_counter += 1
    return df

def sort_by_tempat(df):
    if 'Net' in df.columns:
        df = df.copy()
        df["_abs_net"] = df["Net"].abs()
        df = df.sort_values(by=["_abs_net", "Tempat Pembayaran"])
        df = df.drop(columns="_abs_net")
    return df

# ==========================================
# 3. CORE LOGIC WRAPPED AS FUNCTION
# ==========================================
def process_branch_reconciliation(df_subset, branch_name):
    """
    Fungsi ini menjalankan ALGORITMA UTAMA REKONSILIASI
    Input: DataFrame gabungan (sudah difilter per cabang)
    Output: List of DataFrames (hasil per kategori)
    """
    
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
    mask_asd = (
        df_subset["Keperluan"].str.contains("JMU ASD", na=False) &
        df_subset["Keperluan"].str.contains("JMU ASK", na=False)
    )
    df_asd = df_subset[mask_asd].copy()
    df_subset = df_subset[~mask_asd].copy()

    # --- 7. BKK (ID) ---
    REGEX_BKK_ID = r'(?:ID)?BKK\s*[:\-]?\s*(\d+/\d{4})'
    try:
        all_matches_bkk = df_subset["Keperluan"].str.extractall(REGEX_BKK_ID)[0].unstack()
    except KeyError:
        all_matches_bkk = pd.DataFrame()

    df_matched_bkk_id = pd.DataFrame()
    if not all_matches_bkk.empty:
        for layer in all_matches_bkk.columns:
            if df_subset.empty: break
            df_subset["KODE"] = df_subset.index.map(all_matches_bkk[layer])
            list_candidates = df_subset[df_subset["KODE"].notna()]["KODE"].unique()
            if len(list_candidates) == 0: continue

            sum_id = df_subset[df_subset["ID Dokumen"].isin(list_candidates)].groupby("ID Dokumen")["Net"].sum()
            sum_kode = df_subset[df_subset["KODE"].isin(list_candidates)].groupby("KODE")["Net"].sum()
            
            total_sum = sum_id.add(sum_kode, fill_value=0)
            valid_ids = total_sum[total_sum.abs() <= 1].index.tolist()
            if not valid_ids: continue

            mask_valid_bkk = (df_subset["ID Dokumen"].isin(valid_ids) | df_subset["KODE"].isin(valid_ids))
            df_current_valid = df_subset[mask_valid_bkk].copy()
            df_matched_bkk_id = pd.concat([df_matched_bkk_id, df_current_valid])
            df_subset = df_subset[~mask_valid_bkk].copy()

    # --- 8. BKK (NO) ---
    REGEX_BKK_NO = r'\bNOBKK\s*:\s*([A-Z]{2}\.\d+/\d{2}/\d{4})\b'
    try:
        all_matches_bkk_no_raw = df_subset["Keperluan"].str.extractall(REGEX_BKK_NO)[0].unstack()
    except (KeyError, IndexError):
        all_matches_bkk_no_raw = pd.DataFrame()
    df_matched_bkk_no = pd.DataFrame()

    if not all_matches_bkk_no_raw.empty and all_matches_bkk_no_raw.notna().sum().sum() > 0:
        for layer in all_matches_bkk_no_raw.columns:
            if df_subset.empty: break
            df_subset["KODE"] = df_subset.index.map(all_matches_bkk_no_raw[layer])
            list_candidates = df_subset[df_subset["KODE"].notna()]["KODE"].unique()
            if len(list_candidates) == 0: continue
            sum_id = df_subset[df_subset["Nomor Dokumen"].isin(list_candidates)].groupby("Nomor Dokumen")["Net"].sum()
            sum_kode = df_subset[df_subset["KODE"].isin(list_candidates)].groupby("KODE")["Net"].sum()
            if sum_id.empty and sum_kode.empty: continue
            total_sum = sum_id.add(sum_kode, fill_value=0)
            valid_ids = total_sum[total_sum.abs() <= 1].index.tolist()
            if not valid_ids: continue
            mask_valid_bkk = (df_subset["Nomor Dokumen"].isin(valid_ids) | df_subset["KODE"].isin(valid_ids))
            df_current_valid = df_subset[mask_valid_bkk].copy()
            df_matched_bkk_no = pd.concat([df_matched_bkk_no, df_current_valid])
            df_subset = df_subset[~mask_valid_bkk].copy()
            
    df_matched_bkk = pd.concat([df_matched_bkk_id, df_matched_bkk_no])
    if "KODE" in df_matched_bkk.columns: df_matched_bkk.drop(columns=["KODE"],inplace=True)

    # --- 9. BKM (ID) ---
    REGEX_BKM_ID = r'(?:ID)?BKM\s*[:\-]?\s*(\d+/\d{4})'
    try:
        all_matches_bkm = df_subset["Keperluan"].str.extractall(REGEX_BKM_ID)[0].unstack()
    except KeyError:
        all_matches_bkm = pd.DataFrame()
    df_matched_bkm_id = pd.DataFrame()
    if not all_matches_bkm.empty:
        for layer in all_matches_bkm.columns:
            if df_subset.empty: break
            df_subset["KODE"] = df_subset.index.map(all_matches_bkm[layer])
            list_candidates = df_subset[df_subset["KODE"].notna()]["KODE"].unique()
            if len(list_candidates) == 0: continue
            sum_id = df_subset[df_subset["ID Dokumen"].isin(list_candidates)].groupby("ID Dokumen")["Net"].sum()
            sum_kode = df_subset[df_subset["KODE"].isin(list_candidates)].groupby("KODE")["Net"].sum()
            total_sum = sum_id.add(sum_kode, fill_value=0)
            valid_ids = total_sum[total_sum.abs() <= 1].index.tolist()
            if not valid_ids: continue
            mask_valid_bkm = (df_subset["ID Dokumen"].isin(valid_ids) | df_subset["KODE"].isin(valid_ids))
            df_current_valid = df_subset[mask_valid_bkm].copy()
            df_matched_bkm_id = pd.concat([df_matched_bkm_id, df_current_valid])
            df_subset = df_subset[~mask_valid_bkm].copy()

    # --- 10. BKM (NO) ---
    REGEX_BKM_NO = r'\bNOBKM\s*:\s*([A-Z]{2}\.\d+/\d{2}/\d{4})\b'
    try:
        all_matches_bkm_no_raw = df_subset["Keperluan"].str.extractall(REGEX_BKM_NO)[0].unstack()
    except (KeyError, IndexError):
        all_matches_bkm_no_raw = pd.DataFrame()
    df_matched_bkm_no = pd.DataFrame()
    if not all_matches_bkm_no_raw.empty and all_matches_bkm_no_raw.notna().sum().sum() > 0:
        for layer in all_matches_bkm_no_raw.columns:
            if df_subset.empty: break
            df_subset["KODE"] = df_subset.index.map(all_matches_bkm_no_raw[layer])
            list_candidates = df_subset[df_subset["KODE"].notna()]["KODE"].unique()
            if len(list_candidates) == 0: continue
            sum_id = df_subset[df_subset["Nomor Dokumen"].isin(list_candidates)].groupby("Nomor Dokumen")["Net"].sum()
            sum_kode = df_subset[df_subset["KODE"].isin(list_candidates)].groupby("KODE")["Net"].sum()
            if sum_id.empty and sum_kode.empty: continue
            total_sum = sum_id.add(sum_kode, fill_value=0)
            valid_ids = total_sum[total_sum.abs() <= 1].index.tolist()
            if not valid_ids: continue
            mask_valid_bkm = (df_subset["Nomor Dokumen"].isin(valid_ids) | df_subset["KODE"].isin(valid_ids))
            df_current_valid = df_subset[mask_valid_bkm].copy()
            df_matched_bkm_no = pd.concat([df_matched_bkm_no, df_current_valid])
            df_subset = df_subset[~mask_valid_bkm].copy()
    
    df_matched_bkm = pd.concat([df_matched_bkm_id, df_matched_bkm_no])
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
    df_result = find_offset_pairs(df_subset.copy())
    df_matched_tanggal = df_result[df_result['Is_Matched'] == True].sort_values(by='Match_ID')
    index_tanggal = df_matched_tanggal.index
    df_matched_tanggal.drop(columns=["Match_ID", "Is_Matched"], inplace=True)
    df_subset = df_subset[~df_subset.index.isin(index_tanggal)]

    # --- 15. RECON (OR-TOOLS) ---
    df_recon = reconcile_global_no_group(df_subset, net_col='Net', tolerance=1)
    df_recon = df_recon[df_recon["Match_ID"].notna()]
    df_recon.drop(columns="Match_ID", inplace=True)
    df_subset = df_subset[~df_subset.index.isin(df_recon.index)]

    # --- 16. GANTUNG ---
    df_gantung = pd.concat([df_atk, df_subset], axis=0)

    # Return Result
    return [
        ("DATA GANTUNG", df_gantung),
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

# ==========================================
# 4. MAIN STREAMLIT APP
# ==========================================

st.set_page_config(page_title="Multi-Branch Auto RK", layout="wide")
st.title("Cocokan Hutang/Piutang Afiliasi Cabang")

st.markdown("""
**Instruksi:**
1. Upload file Excel yang berisi 2 Sheet (**Sheet 1**: Tempat Pembayaran Karet [Hutang/Piutang Afiliasi All Cabang], **Sheet 2**: Tempat Pembayaran All Cabang [Hutang/Piutang Afiliasi Karet]).
2. **Pilih Cabang** yang ingin diproses di *sidebar*.
3. Klik tombol proses. Hasil akan disimpan dalam 1 File Excel dengan **Sheet per Cabang**.
4. **Crosscheck Net yang tidak Nol** dan Cocokkan ke Data Gantung.
""")

uploaded_file = st.file_uploader("Upload File Excel", type=['xlsx'])

# --- SIDEBAR: MULTI-SELECT BRANCH ---
st.sidebar.header("Konfigurasi Cabang")
available_branches = list(BRANCH_MAPPING.keys())


# --- FITUR SELECT ALL ---
select_all = st.sidebar.checkbox("Pilih Semua Cabang")

if select_all:
    # Jika Select All dicentang, gunakan semua cabang dan kunci widget multiselect
    selected_branches = available_branches
    st.sidebar.multiselect(
        "Daftar Cabang Terpilih:",
        options=available_branches,
        default=available_branches,
        disabled=True
    )
    st.sidebar.success(f"✅ {len(selected_branches)} Cabang dipilih otomatis.")
else:
    # Jika tidak, user pilih manual
    selected_branches = st.sidebar.multiselect(
        "Pilih Cabang yang akan diproses:",
        options=available_branches
    )

if uploaded_file is not None:
    if st.button("Mulai Proses"):
        
        if not selected_branches:
            st.error("Mohon pilih setidaknya satu cabang di sidebar.")
            st.stop()

        progress_bar = st.progress(0)
        status_text = st.empty()
        
        try:
            # --- 1. LOAD GLOBAL DATA ---
            status_text.text("Membaca data Sheet 1 (Pusat) dan Sheet 2 (Cabang)...")
            
            df_pusat_global = load_excel_with_header_detection(uploaded_file, 0)
            df_cabang_global = load_excel_with_header_detection(uploaded_file, 1)

            if df_pusat_global is None or df_cabang_global is None:
                st.stop()
            
            # --- PREPARE EXCEL WRITER ---
            output = io.BytesIO()
            writer = pd.ExcelWriter(output, engine='xlsxwriter')
            workbook = writer.book

            total_branches = len(selected_branches)
            
            # --- LOOPING PER CABANG ---
            for i, branch_name in enumerate(selected_branches):
                status_text.text(f"Memproses Cabang: {branch_name} ({i+1}/{total_branches})")
                
                # Update progress bar
                progress_step = int((i / total_branches) * 90)
                progress_bar.progress(progress_step)

                # --- FILTER DATA PER CABANG ---
                target_kode = BRANCH_MAPPING[branch_name] # Contoh: "HUTANG/PIUTANG AFILIASI TRK"
                target_tempat = branch_name               # Contoh: "TARAKAN"

                # Filter Pusat: Berdasarkan Kolom "Nama Kode" (Jika ada)
                # Asumsi: Kolom filtering di pusat adalah "Nama Kode"
                if "Nama Kode" in df_pusat_global.columns:
                    df_pusat_filter = df_pusat_global[df_pusat_global["Nama Kode"] == target_kode].copy()
                else:
                    st.warning(f"Kolom 'Nama Kode' tidak ditemukan di Sheet Pusat. Menggunakan semua data pusat untuk {branch_name}.")
                    df_pusat_filter = df_pusat_global.copy()

                # Filter Cabang: Berdasarkan Kolom "Tempat Pembayaran"
                if "Tempat Pembayaran" in df_cabang_global.columns:
                    # Filter contains/exact match (case insensitive safe)
                    mask_cabang = df_cabang_global["Tempat Pembayaran"].astype(str).str.upper() == target_tempat
                    df_cabang_filter = df_cabang_global[mask_cabang].copy()
                else:
                    st.warning(f"Kolom 'Tempat Pembayaran' tidak ditemukan di Sheet Cabang. Menggunakan semua data cabang untuk {branch_name}.")
                    df_cabang_filter = df_cabang_global.copy()

                # --- GABUNG & PRE-PROCESS ---
                if df_pusat_filter.empty and df_cabang_filter.empty:
                    st.warning(f"Data kosong untuk cabang {branch_name}. Skip.")
                    continue
                
                df_all = pd.concat([df_pusat_filter, df_cabang_filter]).copy()
                df_all.reset_index(inplace=True, drop=True)

                cols_num = ['Debet', 'Kredit']
                for col in cols_num:
                    if col in df_all.columns:
                        df_all[col] = pd.to_numeric(df_all[col], errors='coerce').fillna(0)
                
                if "Debet" in df_all.columns and "Kredit" in df_all.columns:
                    df_all["Net"] = df_all["Debet"] - df_all["Kredit"]
                else:
                    st.error(f"Kolom Debet/Kredit hilang di data {branch_name}")
                    continue

                # --- RUN CORE ALGORITHM ---
                # Panggil fungsi inti yang sudah dibungkus
                results_list = process_branch_reconciliation(df_all, branch_name)

                # --- WRITE TO SHEET (PER CABANG) ---
                # Nama Sheet Excel max 31 karakter
                sheet_title = branch_name[:30] 
                worksheet = workbook.add_worksheet(sheet_title)
                writer.sheets[sheet_title] = worksheet
                
                row_pointer = 0
                
                # Header Judul Cabang
                worksheet.write_string(row_pointer, 0, f"Cabang: {branch_name}")
                row_pointer += 2

                for title, df in results_list:
                    if not df.empty:
                        df_sorted = sort_by_tempat(df)
                        
                        # Hitung Total Row
                        total_row = pd.DataFrame(columns=df_sorted.columns)
                        total_row.loc[0] = None 
                        if 'Debet' in df_sorted.columns: total_row.loc[0, 'Debet'] = df_sorted['Debet'].sum()
                        if 'Kredit' in df_sorted.columns: total_row.loc[0, 'Kredit'] = df_sorted['Kredit'].sum()
                        if 'Net' in df_sorted.columns: total_row.loc[0, 'Net'] = df_sorted['Net'].sum()
                        col_label = 'Keperluan' if 'Keperluan' in df_sorted.columns else df_sorted.columns[0]
                        total_row.loc[0, col_label] = 'TOTAL'
                        
                        df_to_write = pd.concat([df_sorted, total_row], ignore_index=True)

                        # Tulis Judul Kategori
                        worksheet.write_string(row_pointer, 0, title)
                        row_pointer += 1

                        if title == "DATA GANTUNG":
                            styler = df_to_write.style.set_properties(**{
                                'background-color': '#FFFF00',
                                'color': 'black',
                                'border-color': 'black'
                            })
                            styler.to_excel(writer, sheet_name=sheet_title, startrow=row_pointer, index=False)
                        else:
                            df_to_write.to_excel(writer, sheet_name=sheet_title, startrow=row_pointer, index=False)
                        
                        row_pointer += len(df_to_write) + 3

            writer.close()
            processed_data = output.getvalue()
            
            progress_bar.progress(100)
            status_text.text("Selesai! Silakan download hasil.")

            st.success(f"Rekonsiliasi {len(selected_branches)} Cabang Selesai!")
            st.download_button(
                label="Download Hasil Multi-Cabang (.xlsx)",
                data=processed_data,
                file_name= "Output_RK_Cabang.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

        except Exception as e:
            st.error(f"Terjadi kesalahan Runtime: {e}")
            st.exception(e)