# utils.py
import pandas as pd
import streamlit as st
from ortools.linear_solver import pywraplp

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
    tgl_col = 'Tanggal Delivery' if 'Tanggal Delivery' in df.columns else 'Tanggal Kasir'
    
    if tgl_col in df.columns:
        df[tgl_col] = pd.to_datetime(df[tgl_col], dayfirst=True, errors='coerce')
        df['Match_ID'] = None        
        df['Is_Matched'] = False      
        match_counter = 1
        
        for tanggal, group in df.groupby(tgl_col):
            if pd.isna(tanggal): continue
            positives = group[(group['Net'] > 0) & (~group['Is_Matched'])]
            for idx_pos, row_pos in positives.iterrows():
                if df.at[idx_pos, 'Is_Matched']: continue
                target_val = -row_pos['Net']
                current_loc = row_pos['Tempat Pembayaran']
                candidates = df[
                    (df['Net'] == target_val) & 
                    (df[tgl_col] == tanggal) &
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