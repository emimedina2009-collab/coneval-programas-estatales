import pandas as pd
import zipfile
from pathlib import Path

DOWNLOADS = Path.home() / "Downloads"
OUTPUT = Path.home() / "Documents" / "coneval_limpieza"
OUTPUT.mkdir(exist_ok=True)

cve_ent = {
    "Aguascalientes": "01", "Baja California": "02", "Baja California Sur": "03",
    "Campeche": "04", "Coahuila": "05", "Colima": "06", "Chiapas": "07",
    "Chihuahua": "08", "Ciudad de Mexico": "09", "Durango": "10",
    "Guanajuato": "11", "Guerrero": "12", "Hidalgo": "13", "Jalisco": "14",
    "Estado de Mexico": "15", "Michoacan": "16", "Morelos": "17",
    "Nayarit": "18", "Nuevo Leon": "19", "Oaxaca": "20", "Puebla": "21",
    "Queretaro": "22", "Quintana Roo": "23", "San Luis Potosi": "24",
    "Sinaloa": "25", "Sonora": "26", "Tabasco": "27", "Tamaulipas": "28",
    "Tlaxcala": "29", "Veracruz": "30", "Yucatan": "31", "Zacatecas": "32"
}

archivos = {
    2010: DOWNLOADS / "INVENTARIO_ESTATAL_CONEVAL_2010.zip",
    2011: DOWNLOADS / "INVENTARIO_ESTATAL_CONEVAL_2011.zip",
    2012: DOWNLOADS / "INVENTARIO_ESTATAL_CONEVAL_2012.zip",
    2013: DOWNLOADS / "INVENTARIO_ESTATAL_CONEVAL_2013.zip",
    2014: DOWNLOADS / "INVENTARIO_ESTATAL_CONEVAL_2014.zip",
    2016: DOWNLOADS / "INVENTARIO_ESTATAL_CONEVAL_2016.zip",
    2018: DOWNLOADS / "IE_2018.zip",
    2021: DOWNLOADS / "IE_2021.zip",
}

def limpiar_nombre(nombre):
    if not isinstance(nombre, str):
        return nombre
    nombre = nombre.strip()
    for a, b in zip("áéíóúÁÉÍÓÚñÑ", "aeiouAEIOUnN"):
        nombre = nombre.replace(a, b)
    reemplazos = {
        "Distrito Federal": "Ciudad de Mexico",
        "Mexico": "Estado de Mexico",
        "Coahuila de Zaragoza": "Coahuila",
        "Michoacan de Ocampo": "Michoacan",
        "Veracruz de Ignacio de la Llave": "Veracruz",
        "Veracruz Ignacio de la Llave": "Veracruz",
    }
    if nombre in reemplazos:
        return reemplazos[nombre]
    if nombre.startswith("Distrito Federal"):
        return "Ciudad de Mexico"
    return nombre

def procesar_anio(anio, ruta):
    print("Procesando " + str(anio) + "...")
    tmp = OUTPUT / "tmp"
    tmp.mkdir(exist_ok=True)
    with zipfile.ZipFile(ruta) as zf:
        excel_files = [f for f in zf.namelist() if f.endswith(".xlsx") or f.endswith(".xls")]
        if not excel_files:
            print("  No se encontro Excel en " + str(anio))
            return None
        zf.extract(excel_files[0], tmp)
        excel_path = tmp / excel_files[0]
    xf = pd.ExcelFile(excel_path)
    hoja = xf.sheet_names[0]
    df_raw = xf.parse(hoja, header=None)
    header_row = 0
    for i, row in df_raw.iterrows():
        if any("entidad" in str(v).lower() for v in row):
            header_row = i
            break
    df = xf.parse(hoja, header=header_row)
    df.columns = [str(c).strip() for c in df.columns]
    col_entidad = [c for c in df.columns if "entidad" in c.lower()]
    if not col_entidad:
        return None
    df = df.rename(columns={col_entidad[0]: "entidad"})
    col_presup = [c for c in df.columns if "ejercido" in c.lower()]
    if col_presup:
        df = df.rename(columns={col_presup[0]: "presupuesto_ejercido_mdp"})
    df = df[df["entidad"].notna()]
    df = df[~df["entidad"].astype(str).str.strip().str.upper().str.startswith("NOTA")]
    df = df[~df["entidad"].astype(str).str.lower().str.contains("entidad|inventario")]
    df["entidad"] = df["entidad"].apply(limpiar_nombre)
    df["anio"] = anio
    df["cve_ent"] = df["entidad"].map(cve_ent)
    df = df.replace({"No disponible": pd.NA, "ND": pd.NA, "N/D": pd.NA})
    if "presupuesto_ejercido_mdp" in df.columns:
        df["presupuesto_ejercido_mdp"] = pd.to_numeric(df["presupuesto_ejercido_mdp"], errors="coerce")
    sin_clave = df[df["cve_ent"].isna()]["entidad"].unique()
    if len(sin_clave) > 0:
        print("  Sin cve_ent: " + str(sin_clave))
    print("  Filas: " + str(len(df)) + " | Estados: " + str(df["cve_ent"].nunique()))
    return df

dfs = []
for anio, ruta in archivos.items():
    if not ruta.exists():
        alt = ruta.parent / (ruta.stem + " (1)" + ruta.suffix)
        ruta = alt if alt.exists() else ruta
    if not ruta.exists():
        print("No encontrado: " + ruta.name)
        continue
    df = procesar_anio(anio, ruta)
    if df is not None:
        dfs.append(df)

panel = pd.concat(dfs, ignore_index=True)

resumen = panel.groupby(["cve_ent", "entidad", "anio"]).agg(
    num_programas=("entidad", "count")
).reset_index()

if "presupuesto_ejercido_mdp" in panel.columns:
    presup = panel.groupby(["cve_ent", "entidad", "anio"])["presupuesto_ejercido_mdp"].sum(min_count=1).reset_index()
    resumen = resumen.merge(presup, on=["cve_ent", "entidad", "anio"])
    resumen["presupuesto_por_programa"] = resumen["presupuesto_ejercido_mdp"] / resumen["num_programas"]

from datetime import date
hoy = date.today().strftime("%Y%m%d")

csv_path = OUTPUT / (hoy + "_coneval_panel_v1.csv")
resumen.to_csv(csv_path, index=False, encoding="utf-8")
print("CSV guardado: " + str(csv_path))

nas = resumen.isna().sum().reset_index()
nas.columns = ["variable", "n_missing"]
nas["pct_missing"] = (nas["n_missing"] / len(resumen) * 100).round(1)
nas.to_csv(OUTPUT / (hoy + "_reporte_nas_v1.csv"), index=False)

nas_por_anio = resumen.groupby("anio")["presupuesto_ejercido_mdp"].apply(lambda x: x.isna().sum()).reset_index()
nas_por_anio.columns = ["anio", "estados_sin_presupuesto"]
nas_por_anio["total_estados"] = resumen.groupby("anio")["entidad"].count().values
nas_por_anio["pct_sin_presupuesto"] = (nas_por_anio["estados_sin_presupuesto"] / nas_por_anio["total_estados"] * 100).round(1)
nas_por_anio.to_csv(OUTPUT / (hoy + "_nas_por_anio_v1.csv"), index=False)

print("NAs por anio:")
print(nas_por_anio.to_string(index=False))
print("Panel: " + str(resumen.shape[0]) + " filas x " + str(resumen.shape[1]) + " columnas")
print("Estados: " + str(resumen["entidad"].nunique()))
