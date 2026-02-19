# app.py
import streamlit as st
import pandas as pd
import io
import utils
import algo_general
import algo_depo

# ==========================================
# DEFINISI MAPPING CABANG
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
    "TARAKAN": "HUTANG/PIUTANG AFILIASI TRK",
    "TERNATE": "HUTANG/PIUTANG AFILIASI TERNATE",
    "TIMIKA": "HUTANG/PIUTANG AFILIASI TIMIKA",
    "TUAL": "HUTANG/PIUTANG AFILIASI TUAL"
}

st.set_page_config(page_title="Multi-Branch Auto RK", layout="wide")
st.title("Cocokan Hutang/Piutang Afiliasi Cabang")
st.markdown("""
**Instruksi:**
1. Upload file Excel yang berisi 2 Sheet 
    * **Sheet 1**: Tempat Pembayaran Karet [COA: Hutang/Piutang Afiliasi (All) Cabang], 
    * **Sheet 2**: Tempat Pembayaran All Cabang [COA: Hutang/Piutang Afiliasi Karet].
2. **Pilih Cabang** yang ingin diproses di *sidebar*.
3. Klik tombol proses, hasil akan disimpan dalam 1 File Excel dengan Sheet per Cabang.
4. **PASTIKAN CROSSCHECK NET TIAP SUB DATA ADALAH NOL** jika tidak, maka bisa cocokkan dengan data gantungan.
""")

uploaded_file = st.file_uploader("Upload File Excel", type=['xlsx'])

# --- SIDEBAR ---
st.sidebar.header("Konfigurasi Cabang")
available_branches = list(BRANCH_MAPPING.keys())

select_all = st.sidebar.checkbox("Pilih Semua Cabang")
if select_all:
    selected_branches = available_branches
    st.sidebar.multiselect("Daftar Cabang:", options=available_branches, default=available_branches, disabled=True)
else:
    selected_branches = st.sidebar.multiselect("Pilih Cabang:", options=available_branches)

if uploaded_file and st.button("Mulai Proses"):
    if not selected_branches:
        st.error("Mohon pilih cabang.")
        st.stop()

    progress_bar = st.progress(0)
    status_text = st.empty()
    
    try:
        status_text.text("Membaca data...")
        df_pusat_global = utils.load_excel_with_header_detection(uploaded_file, 0)
        df_cabang_global = utils.load_excel_with_header_detection(uploaded_file, 1)

        if df_pusat_global is None or df_cabang_global is None: st.stop()
        
        output = io.BytesIO()
        writer = pd.ExcelWriter(output, engine='xlsxwriter')
        workbook = writer.book

        total_branches = len(selected_branches)

        for i, branch_name in enumerate(selected_branches):
            status_text.text(f"Memproses: {branch_name} ({i+1}/{total_branches})")
            progress_bar.progress(int((i / total_branches) * 90))

            # --- FILTER DATA ---
            target_kode = BRANCH_MAPPING.get(branch_name, "")
            
            # Filter Pusat
            if "Nama Kode" in df_pusat_global.columns:
                df_pusat_filter = df_pusat_global[df_pusat_global["Nama Kode"] == target_kode].copy()
            else:
                df_pusat_filter = df_pusat_global.copy()

            # Filter Cabang
            if "Tempat Pembayaran" in df_cabang_global.columns:
                # Use strict equality inside mask loop if standard, or specific if depo
                mask_cabang = df_cabang_global["Tempat Pembayaran"].astype(str).str.upper() == branch_name
                df_cabang_filter = df_cabang_global[mask_cabang].copy()
            else:
                df_cabang_filter = df_cabang_global.copy()

            if df_pusat_filter.empty and df_cabang_filter.empty:
                continue
            
            df_all = pd.concat([df_pusat_filter, df_cabang_filter]).copy()
            df_all.reset_index(inplace=True, drop=False)

            # --- CLEAN NUMBERS ---
            cols_num = ['Debet', 'Kredit']
            for col in cols_num:
                if col in df_all.columns:
                    df_all[col] = pd.to_numeric(df_all[col], errors='coerce').fillna(0)
            if "Debet" in df_all.columns and "Kredit" in df_all.columns:
                df_all["Net"] = df_all["Debet"] - df_all["Kredit"]
            else:
                st.error(f"Kolom Net Error di {branch_name}")
                continue

            # --- SWITCH LOGIC: DEPO VS GENERAL ---
            if branch_name == "DEPO":
                # Panggil Modul Khusus Depo
                results_list = algo_depo.run_segmented_depo_logic(df_all)
                sheet_label_suffix = " (Rolling)"
            else:
                # Panggil Modul General
                results_list = algo_general.process_branch_reconciliation(df_all, branch_name)
                sheet_label_suffix = ""

            # --- WRITE OUTPUT ---
            sheet_title = branch_name[:30]
            worksheet = workbook.add_worksheet(sheet_title)
            writer.sheets[sheet_title] = worksheet
            
            row_pointer = 0
            worksheet.write_string(row_pointer, 0, f"Cabang: {branch_name}{sheet_label_suffix}")
            row_pointer += 2

            for title, df in results_list:
                if not df.empty:
                    df_sorted = utils.sort_by_tempat(df)
                    
                    # Total Row
                    total_row = pd.DataFrame(columns=df_sorted.columns)
                    total_row.loc[0] = None 
                    if 'Debet' in df_sorted.columns: total_row.loc[0, 'Debet'] = df_sorted['Debet'].sum()
                    if 'Kredit' in df_sorted.columns: total_row.loc[0, 'Kredit'] = df_sorted['Kredit'].sum()
                    if 'Net' in df_sorted.columns: total_row.loc[0, 'Net'] = df_sorted['Net'].sum()
                    col_label = 'Keperluan' if 'Keperluan' in df_sorted.columns else df_sorted.columns[0]
                    total_row.loc[0, col_label] = 'TOTAL'
                    
                    df_to_write = pd.concat([df_sorted, total_row], ignore_index=True)

                    worksheet.write_string(row_pointer, 0, title)
                    row_pointer += 1

                    # Style Yellow untuk Data Gantung
                    if "GANTUNG" in title.upper():
                        styler = df_to_write.style.set_properties(**{'background-color': '#FFFF00', 'color': 'black', 'border-color': 'black'})
                        styler.to_excel(writer, sheet_name=sheet_title, startrow=row_pointer, index=False)
                    else:
                        df_to_write.to_excel(writer, sheet_name=sheet_title, startrow=row_pointer, index=False)
                    
                    row_pointer += len(df_to_write) + 3

        writer.close()
        processed_data = output.getvalue()
        progress_bar.progress(100)
        st.success("Selesai!")
        st.download_button(label="Download Hasil (.xlsx)", data=processed_data, file_name="Output_RK.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    except Exception as e:
        st.error(f"Runtime Error: {e}")