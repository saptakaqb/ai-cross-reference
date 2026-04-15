#!/usr/bin/env python3
"""
make_kubler_compatible_samples.py  (V13 — vectorized)
Assembles competitor_unified.csv from all raw manufacturer sources.
Usage:
    python make_kubler_compatible_samples.py
    python make_kubler_compatible_samples.py --dry-run
"""
import argparse, re, sys, warnings
from pathlib import Path
import numpy as np
import pandas as pd
warnings.filterwarnings("ignore")

KUBLER_PPR_DISCRETE = frozenset({
    1,2,4,5,10,12,14,20,25,28,30,32,36,50,60,64,80,
    100,120,125,150,180,200,240,250,256,300,342,360,375,
    400,500,512,600,625,720,800,900,1000,1024,1200,1250,
    1500,1800,2000,2048,2500,3000,3600,4000,4096,5000,
})

UNIFIED_COLS = [
    "manufacturer","part_number","product_family","encoder_type",
    "sensing_method","source_pdf","order_pattern",
    "resolution_ppr","ppr_range_min","ppr_range_max","is_programmable",
    "output_circuit_canonical","output_signals","num_output_channels",
    "max_output_freq_hz","supply_voltage_min_v","supply_voltage_max_v",
    "output_current_ma","power_consumption_typ_mw",
    "reverse_polarity_protection","short_circuit_protection",
    "housing_diameter_mm","shaft_diameter_mm","shaft_type",
    "flange_type","connection_type","connector_pins",
    "ip_rating","operating_temp_min_c","operating_temp_max_c",
    "max_speed_rpm_peak","shock_resistance","vibration_resistance",
    "weight_g","startup_torque_ncm","shaft_load_radial_n",
    "shaft_load_axial_n","moment_of_inertia",
    "oc_shaft_type","oc_flange","oc_ppr","oc_interface","oc_connector",
]

KUBLER_HOUSING = {
    "KIS40":40.,"KIH40":40.,"5000":58.,"5020":58.,
    "K58I_shaft":58.,"K58I_hollow":58.,
    "K58I-PR_shaft":58.,"K58I-PR_hollow":58.,
    "K80I":80.,"K80I-PR":80.,"A020":109.,"A02H":109.,
}
EPC_HOUSING = {
    "858S":58.,"802S":50.8,"755A_shaft":38.1,
    "755A_hollow":38.1,"776":109.,"260":50.8,
}
NIDEC_HOUSING = {"AV20":63.5,"AV25":63.5,"AV30B":76.2,"AV44":44.}
NIDEC_SHAFT_DIA = {"A":10.,"B":9.525,"C":12.,"N":10.,"P":12.7,"R":9.}

def _vvoltage(s):
    def p(t):
        n=[float(x) for x in re.findall(r"\d+(?:\.\d+)?",str(t)) if float(x)<1000]
        return (n[0],n[-1]) if len(n)>=2 else ((n[0],n[0]) if n else (float("nan"),float("nan")))
    r=s.fillna("").astype(str).map(p)
    return r.map(lambda x:x[0]), r.map(lambda x:x[1])

def _vip(s):
    def p(t):
        m=re.search(r"IP\s*(\d{2,3})",str(t),re.I)
        return float(m.group(1)) if m else float("nan")
    return s.fillna("").map(p)

def _vtemp(s):
    def p(t):
        n=[float(x) for x in re.findall(r"[+-]?\d+(?:\.\d+)?",str(t))]
        return (n[0],n[-1]) if len(n)>=2 else (float("nan"),float("nan"))
    r=s.fillna("").map(p)
    return r.map(lambda x:x[0]), r.map(lambda x:x[1])

def _vspeed(s):
    def p(t):
        m=re.search(r"(\d[\d,]*)",str(t))
        return float(m.group(1).replace(",","")) if m else float("nan")
    return s.fillna("").map(p)

def _vkg2g(s):
    def p(t):
        n=re.findall(r"\d+(?:\.\d+)?",str(t))
        return float(n[0])*1000 if n else float("nan")
    return s.fillna("").map(p)

def _vload(s):
    def p(t):
        m=re.search(r"(\d+(?:\.\d+)?)",str(t))
        return float(m.group(1)) if m else float("nan")
    return s.fillna("").map(p)

def _vcircuit(s):
    def c(v):
        if pd.isna(v): return float("nan")
        sl=str(v).strip().lower()
        if "sin" in sl and "cos" in sl: return "Sin/Cos"
        if "ttl" in sl and "htl" in sl: return "TTL/HTL Universal"
        if "pp/ld" in sl or ("push" in sl and "pull" in sl and "line" in sl): return "PP/LD Universal"
        if "rs422" in sl or "rs-422" in sl or "line driver" in sl or "ttl" in sl: return "TTL RS422"
        if "htl" in sl: return "HTL"
        if "push" in sl and "pull" in sl: return "Push-Pull"
        if "open collector" in sl: return "Open Collector"
        if "npn" in sl: return "NPN Open Collector"
        if "pnp" in sl: return "PNP Open Collector"
        return str(v).strip()
    return s.map(c)

def _ensure(df):
    for c in UNIFIED_COLS:
        if c not in df.columns: df[c]=float("nan")
    return df[UNIFIED_COLS].copy()

def _stratified_sample(df, groups, n):
    df2 = df.copy().reset_index(drop=True)
    df2["_sc"] = df2.notna().sum(axis=1)
    # Sort best rows first so groupby.head() picks the richest ones
    df2 = df2.sort_values("_sc", ascending=False)
    nb = max(1, df2.groupby(groups, observed=True).ngroups)
    pb = max(1, n // nb)
    # groupby.head() preserves all columns (unlike apply in pandas 2.x)
    s = df2.groupby(groups, observed=True).head(pb).reset_index(drop=True)
    if len(s) < n:
        rest = df2[~df2.index.isin(s.index)]
        s = pd.concat([s, rest.head(n - len(s))], ignore_index=True)
    return s.head(n).drop(columns=["_sc"])[UNIFIED_COLS]

# ── Kübler ───────────────────────────────────────────────────────────────────
def load_kubler(path):
    print(f"[Kübler] Loading {path} ...")
    df=pd.read_csv(path,low_memory=False)
    print(f"[Kübler] Raw: {len(df):,}")
    out=pd.DataFrame(index=df.index)
    fam=df.get("product_family",df.get("family",pd.Series(dtype=str))).astype(str)
    out["manufacturer"]="Kubler"
    out["part_number"]=df.get("part_number",df.get("product_code"))
    out["product_family"]=fam
    out["encoder_type"]="Incremental"
    out["sensing_method"]="Optical"
    out["source_pdf"]=df.get("source_pdf")
    out["order_pattern"]=df.get("order_pattern")
    out["resolution_ppr"]=pd.to_numeric(df.get("resolution_ppr"),errors="coerce")
    out["ppr_range_min"]=pd.to_numeric(df.get("ppr_range_min"),errors="coerce")
    out["ppr_range_max"]=pd.to_numeric(df.get("ppr_range_max"),errors="coerce")
    out["is_programmable"]=out["ppr_range_min"].notna()
    circ=df.get("output_circuits",df.get("interface_canonical",pd.Series(dtype=str)))
    out["output_circuit_canonical"]=_vcircuit(circ)
    out["output_signals"]=df.get("output_signals")
    out["max_output_freq_hz"]=pd.to_numeric(df.get("max_output_frequency_hz"),errors="coerce")
    v1,v2=_vvoltage(df.get("supply_voltage",pd.Series(dtype=str)))
    out["supply_voltage_min_v"]=v1; out["supply_voltage_max_v"]=v2
    out["ip_rating"]=_vip(df.get("protection_rating",pd.Series(dtype=str)))
    t1,t2=_vtemp(df.get("operating_temp_range",pd.Series(dtype=str)))
    out["operating_temp_min_c"]=t1; out["operating_temp_max_c"]=t2
    out["shaft_diameter_mm"]=pd.to_numeric(df.get("shaft_diameter_mm"),errors="coerce")
    out["max_speed_rpm_peak"]=_vspeed(df.get("max_speed_rpm",pd.Series(dtype=str)))
    out["weight_g"]=_vkg2g(df.get("weight_kg",pd.Series(dtype=str)))
    out["shaft_load_radial_n"]=_vload(df.get("shaft_load_radial_n",pd.Series(dtype=str)))
    out["shaft_load_axial_n"]=_vload(df.get("shaft_load_axial_n",pd.Series(dtype=str)))
    out["shock_resistance"]=df.get("shock_resistance")
    out["vibration_resistance"]=df.get("vibration_resistance")
    out["connection_type"]=df.get("connection_type")
    out["housing_diameter_mm"]=fam.map(KUBLER_HOUSING)
    miss=out["housing_diameter_mm"].isna()
    nums=fam[miss].str.extract(r"(\d{2,3})")[0]
    if miss.any() and len(nums) > 0:
        filled = nums.apply(lambda x: float(x) if pd.notna(x) and 25<=int(x)<=150 else float("nan"))
        out.loc[miss, "housing_diameter_mm"] = filled.values
    out["shaft_type"]=np.where(fam.str.lower().str.contains("hollow|kih|a02h"),"Hollow","Solid")
    out["oc_ppr"]=out["resolution_ppr"]
    out["oc_interface"]=df.get("output_circuits")
    out=_ensure(out)
    print(f"[Kübler] Output: {len(out):,}")
    return out

# ── EPC ──────────────────────────────────────────────────────────────────────
def load_epc(path,target_n=200_000):
    print(f"[EPC] Loading {path} ...")
    df=pd.read_csv(path,low_memory=False)
    print(f"[EPC] Raw: {len(df):,}")
    out=pd.DataFrame(index=df.index)
    fam=df.get("product_family",df.get("family",pd.Series(dtype=str))).astype(str)
    out["manufacturer"]="EPC"
    out["part_number"]=df.get("part_number",df.get("product_code"))
    out["product_family"]=fam
    out["encoder_type"]="Incremental"
    out["sensing_method"]=df.get("encoder_type","Optical").fillna("Optical").astype(str).str.capitalize()
    out["source_pdf"]=df.get("source_pdf")
    out["order_pattern"]=df.get("order_pattern")
    out["resolution_ppr"]=pd.to_numeric(df.get("resolution_ppr"),errors="coerce")
    out["ppr_range_min"]=float("nan"); out["ppr_range_max"]=float("nan")
    out["is_programmable"]=False
    circ=df.get("output_circuits",df.get("interface_canonical",pd.Series(dtype=str)))
    out["output_circuit_canonical"]=_vcircuit(circ)
    out["output_signals"]=df.get("output_signals")
    out["max_output_freq_hz"]=pd.to_numeric(df.get("max_output_frequency_hz"),errors="coerce")
    out["output_current_ma"]=pd.to_numeric(df.get("permissible_load_per_channel"),errors="coerce")
    v1,v2=_vvoltage(df.get("supply_voltage",pd.Series(dtype=str)))
    out["supply_voltage_min_v"]=v1; out["supply_voltage_max_v"]=v2
    out["ip_rating"]=_vip(df.get("protection_rating",pd.Series(dtype=str)))
    t1,t2=_vtemp(df.get("operating_temp_range",pd.Series(dtype=str)))
    out["operating_temp_min_c"]=t1; out["operating_temp_max_c"]=t2
    out["shaft_diameter_mm"]=pd.to_numeric(df.get("shaft_diameter_mm"),errors="coerce")
    out["max_speed_rpm_peak"]=_vspeed(df.get("max_speed_rpm",pd.Series(dtype=str)))
    out["weight_g"]=_vkg2g(df.get("weight_kg",pd.Series(dtype=str)))
    out["shaft_load_radial_n"]=_vload(df.get("shaft_load_radial_n",pd.Series(dtype=str)))
    out["shaft_load_axial_n"]=_vload(df.get("shaft_load_axial_n",pd.Series(dtype=str)))
    out["shock_resistance"]=df.get("shock_resistance")
    out["vibration_resistance"]=df.get("vibration_resistance")
    out["connection_type"]=df.get("connection_type")
    out["housing_diameter_mm"]=fam.map(EPC_HOUSING)
    out["shaft_type"]=np.where(fam.isin(["755A_hollow","776","260"]),"Hollow","Solid")
    out["oc_ppr"]=out["resolution_ppr"]
    out["oc_interface"]=df.get("output_circuits")
    out=_ensure(out)
    if len(out)>target_n:
        out=_stratified_sample(out,["product_family","output_circuit_canonical"],target_n)
    print(f"[EPC] Output: {len(out):,}")
    return out

# ── Nidec ────────────────────────────────────────────────────────────────────
def load_nidec(path,target_n=60_000):
    print(f"[Nidec] Loading {path} ...")
    df=pd.read_csv(path,low_memory=False)
    print(f"[Nidec] Raw: {len(df):,}")
    df["_ppr"]=pd.to_numeric(df["resolution_ppr"],errors="coerce")
    df=df[df["_ppr"].isin(KUBLER_PPR_DISCRETE)].copy()
    print(f"[Nidec] After PPR filter: {len(df):,}")
    sp=df["param_shaft"].astype(str).str.upper().str.strip()
    is_hol=sp.isin(["H","T","1","5"])
    out=pd.DataFrame(index=df.index)
    out["manufacturer"]="Nidec"
    out["part_number"]=df["product_code"]
    out["product_family"]=df["family"]
    out["encoder_type"]="Incremental"
    out["sensing_method"]=df["encoder_technology"].str.capitalize()
    out["order_pattern"]=df["product_code"]
    out["resolution_ppr"]=df["_ppr"]
    out["ppr_range_min"]=float("nan"); out["ppr_range_max"]=float("nan")
    out["is_programmable"]=False
    out["output_circuit_canonical"]="TTL RS422"
    out["output_signals"]=df["output_signals"]
    out["output_current_ma"]=pd.to_numeric(df["current_ma"],errors="coerce")
    khz=pd.to_numeric(df["max_output_frequency_khz"],errors="coerce")
    out["max_output_freq_hz"]=khz*1000
    v1,v2=_vvoltage(df["supply_voltage_vdc"].astype(str))
    out["supply_voltage_min_v"]=v1; out["supply_voltage_max_v"]=v2
    out["ip_rating"]=_vip(df["protection_rating"])
    out["operating_temp_min_c"]=pd.to_numeric(df["operating_temp_min_c"],errors="coerce")
    out["operating_temp_max_c"]=pd.to_numeric(df["operating_temp_max_c"],errors="coerce")
    out["max_speed_rpm_peak"]=pd.to_numeric(df["max_speed_rpm"],errors="coerce")
    wkg=pd.to_numeric(df["weight_kg"],errors="coerce")
    out["weight_g"]=wkg*1000
    out["shaft_load_radial_n"]=pd.to_numeric(df["shaft_load_radial_n"],errors="coerce")
    out["shaft_load_axial_n"]=pd.to_numeric(df["shaft_load_axial_n"],errors="coerce")
    sg=pd.to_numeric(df["shock_g"],errors="coerce")
    vg=pd.to_numeric(df["vibration_g"],errors="coerce")
    out["shock_resistance"]=sg.map(lambda x:f"{x:.0f} g" if pd.notna(x) else float("nan"))
    out["vibration_resistance"]=vg.map(lambda x:f"{x:.0f} g" if pd.notna(x) else float("nan"))
    out["shaft_type"]=np.where(is_hol,"Hollow","Solid")
    solid_dia=sp.map(NIDEC_SHAFT_DIA)
    bore=pd.to_numeric(df.get("param_rotor_bore",pd.Series(dtype=float)),errors="coerce")
    out["shaft_diameter_mm"]=np.where(is_hol,bore,solid_dia)
    out["housing_diameter_mm"]=df["family"].map(NIDEC_HOUSING)
    out["oc_ppr"]=df.get("param_ppr"); out["oc_interface"]=df.get("param_line_driver")
    out["oc_shaft_type"]=sp; out["oc_connector"]=df.get("param_connector")
    out=_ensure(out)
    out=_stratified_sample(out,["product_family","resolution_ppr"],target_n)
    print(f"[Nidec] Output: {len(out):,}")
    return out

# ── Lika + Wachendorff from existing unified ─────────────────────────────────
def load_lika_wachendorff(path):
    print(f"[Lika/Wachendorff] From: {path} ...")
    df=pd.read_csv(path,low_memory=False)
    lw=df[df["manufacturer"].isin(["Lika","Wachendorff"])].copy()
    for c in UNIFIED_COLS:
        if c not in lw.columns: lw[c]=float("nan")
    lw=lw[UNIFIED_COLS]
    for m,cnt in lw["manufacturer"].value_counts().items():
        print(f"  [{m}] {cnt:,} rows")
    return lw

# ── Sick ─────────────────────────────────────────────────────────────────────
def load_sick(path):
    print(f"[Sick] Loading {path} ...")
    df=pd.read_csv(path,low_memory=False)
    print(f"[Sick] Raw: {len(df):,}")
    out=pd.DataFrame(index=df.index)
    pn_col=df.get("Part no.:","").fillna(df.get("product_name",""))
    if hasattr(pn_col,"fillna"): pn=pn_col
    else: pn=pd.Series(pn_col,index=df.index)
    out["manufacturer"]="Sick"
    out["part_number"]=pn
    out["product_family"]=pn.astype(str).str.split("-").str[0].str.strip()
    out["encoder_type"]="Incremental"
    ppr_raw=df.get("Pulses per revolution",pd.Series(dtype=str)).fillna("")
    out["resolution_ppr"]=pd.to_numeric(ppr_raw.astype(str).str.replace(",","",regex=False),errors="coerce")
    prog_raw=df.get("Programmable/configurable",pd.Series(dtype=str)).fillna("")
    out["is_programmable"]=prog_raw.str.contains("yes|✔",case=False,na=False)
    dip_cols=[c for c in df.columns if "DIP switch" in str(c) and "PPR" in str(c)]
    if dip_cols:
        def _dip(row):
            v=[]
            for c in dip_cols:
                v+=[int(x) for x in re.findall(r"\d+",str(row.get(c,""))) if int(x)<50000]
            return (min(v),max(v)) if v else (float("nan"),float("nan"))
        rng=df.apply(_dip,axis=1)
        out["ppr_range_min"]=np.where(out["is_programmable"],rng.map(lambda x:x[0]),float("nan"))
        out["ppr_range_max"]=np.where(out["is_programmable"],rng.map(lambda x:x[1]),float("nan"))
    # Sick output circuit: derive from 1st letter after dash in product name
    #   B=HTL/Push-Pull, T=TTL RS422, S=SinCos, R=TTL/HTL Universal
    pn_circ = pn.astype(str).str.extract(r'-([BTSRA])')[0]
    sick_circuit_map = {'B':'HTL','T':'TTL RS422','S':'Sin/Cos','R':'TTL/HTL Universal','A':'TTL RS422'}
    derived = pn_circ.map(sick_circuit_map)
    # Override with Output function column where available
    of_col = df.get("Output function", pd.Series(dtype=str))
    of_map = {
        'A and B output':       'Push-Pull',
        'A and Direction Output':'Push-Pull',
        'CW and CCW output':    'Push-Pull',
        'Channel A, error':     'TTL RS422',
    }
    explicit = of_col.map(of_map)
    out["output_circuit_canonical"] = explicit.combine_first(derived)
    v1,v2=_vvoltage(df.get("Supply voltage",pd.Series(dtype=str)))
    out["supply_voltage_min_v"]=v1; out["supply_voltage_max_v"]=v2
    out["ip_rating"]=_vip(df.get("Enclosure rating",pd.Series(dtype=str)))
    t1,t2=_vtemp(df.get("Operating temperature range",pd.Series(dtype=str)))
    out["operating_temp_min_c"]=t1; out["operating_temp_max_c"]=t2
    spd=df.get("Operating speed",df.get("Maximum operating speed",pd.Series(dtype=str)))
    out["max_speed_rpm_peak"]=pd.to_numeric(spd,errors="coerce")
    out["weight_g"]=pd.to_numeric(df.get("Unit weight",df.get("Net unit weight",pd.Series(dtype=str))),errors="coerce")
    out["output_current_ma"]=pd.to_numeric(df.get("Load current",pd.Series(dtype=str)),errors="coerce")
    out["output_signals"]=df.get("Number of signal channels")
    out["shock_resistance"]=df.get("Resistance to shocks","").astype(str)
    out["vibration_resistance"]=df.get("Resistance to vibration","").astype(str)
    mech=df.get("Mechanical design",pd.Series(dtype=str)).fillna("")
    out["shaft_type"]=np.where(mech.str.contains("hollow",case=False),
        np.where(mech.str.contains("through",case=False),"Through Hollow","Hollow"),
        np.where(mech.str.contains("solid",case=False),"Solid",None))
    housing=pn.astype(str).str.extract(r"(\d{2,3})")[0].apply(
        lambda x:float(x) if pd.notna(x) and 25<=int(x)<=150 else float("nan"))
    out["housing_diameter_mm"]=housing
    # Shaft diameter from dedicated column: '10 mmWith flat' → 10.0
    shaft_raw = df.get("Shaft diameter", pd.Series(dtype=str)).fillna("")
    out["shaft_diameter_mm"] = shaft_raw.str.extract(r"(\d+(?:\.\d+)?)")[0].apply(
        lambda x: float(x) if pd.notna(x) else float("nan"))
    out=_ensure(out)
    print(f"[Sick] Output: {len(out):,}")
    return out

# ── Baumer ───────────────────────────────────────────────────────────────────
def load_baumer(path):
    print(f"[Baumer] Loading {path} ...")
    try: df=pd.read_excel(path,header=1)
    except Exception: df=pd.read_csv(path,low_memory=False)
    print(f"[Baumer] Raw: {len(df):,}")
    fixed_ppr={col:int(re.search(r"\d+",col).group()) for col in df.columns if re.match(r"Pulses\s*=\s*\d+",str(col))}
    out=pd.DataFrame(index=df.index)
    pn=df.get("Product Name",pd.Series(dtype=str))
    out["manufacturer"]="Baumer"
    out["part_number"]=pn
    out["product_family"]=pn.astype(str).str.extract(r"^([A-Za-z0-9]+)")[0]
    out["encoder_type"]=df.get("Enoder Type","Incremental").fillna("Incremental")
    ppr_raw=df.get("Pulses per revolution",pd.Series(dtype=str)).fillna("")
    is_prog=ppr_raw.str.contains(r"[…\.]",na=False)|ppr_raw.str.contains("programmable",case=False,na=False)
    out["is_programmable"]=is_prog
    def _rng(t):
        n=[int(x) for x in re.findall(r"\d+",str(t)) if int(x) <= 100000]
        return (float(min(n)),float(max(n))) if len(n)>=2 else (float("nan"),float("nan"))
    rng=ppr_raw.map(_rng)
    out["ppr_range_min"]=np.where(is_prog,rng.map(lambda x:x[0]),float("nan"))
    out["ppr_range_max"]=np.where(is_prog,rng.map(lambda x:x[1]),float("nan"))
    if fixed_ppr:
        def _fp(row):
            for col,val in fixed_ppr.items():
                if pd.notna(row.get(col)) and str(row.get(col)).strip() not in ("","nan","0"): return float(val)
            return float("nan")
        fp=df.apply(_fp,axis=1)
    else:
        fp=ppr_raw.map(lambda t:float(re.findall(r"\d+",str(t))[0]) if re.findall(r"\d+",str(t)) else float("nan"))
    out["resolution_ppr"]=np.where(~is_prog,fp,float("nan"))
    circ=df.get("Output circuits",df.get("Output circuit",pd.Series(dtype=str)))
    out["output_circuit_canonical"]=_vcircuit(circ)
    out["output_signals"]=df.get("Output signals")
    v1,v2=_vvoltage(df.get("Voltage supply",pd.Series(dtype=str)))
    out["supply_voltage_min_v"]=v1; out["supply_voltage_max_v"]=v2
    ip_raw=df.get("Protection EN 60529",df.get("Enclosure",pd.Series(dtype=str)))
    out["ip_rating"]=_vip(ip_raw)
    temp=df.get("Operating temperature",df.get("Ambient temperature",pd.Series(dtype=str)))
    t1,t2=_vtemp(temp)
    out["operating_temp_min_c"]=t1; out["operating_temp_max_c"]=t2
    spd=df.get("Speed (n)",df.get("Operating speed",pd.Series(dtype=str)))
    out["max_speed_rpm_peak"]=_vspeed(spd)
    blind=df.get("Blind hollow shaft ø16 mm or cone shaft ø17 mm (1",pd.Series(dtype=str))
    hol=df.get("Hollow shaft",pd.Series(dtype=str))
    out["shaft_type"]=np.where(blind.notna()|hol.notna(),"Hollow","Solid")
    load_raw=df.get("Admitted shaft load",pd.Series(dtype=str)).fillna("")
    out["shaft_load_radial_n"]=load_raw.str.extract(r"(\d+)\s*N\s*radial",flags=re.I)[0].astype(float)
    out["shaft_load_axial_n"]=load_raw.str.extract(r"(\d+)\s*N\s*axial",flags=re.I)[0].astype(float)
    housing=pn.astype(str).str.extract(r"(\d{2,3})")[0].apply(
        lambda x:float(x) if pd.notna(x) and 25<=int(x)<=200 else float("nan"))
    out["housing_diameter_mm"]=housing
    out["connection_type"]=df.get("Connection",df.get("Electrical connection"))
    out["shock_resistance"]=df.get("Shock",df.get("Shock resistance"))
    out["vibration_resistance"]=df.get("Vibration",df.get("Vibration resistance"))
    wt_raw=df.get("Weight approx.",pd.Series(dtype=str)).fillna("")
    out["weight_g"]=wt_raw.str.extract(r"(\d+(?:\.\d+)?)")[0].astype(float)
    out["max_speed_rpm_peak"]=_vspeed(df.get("Speed (n)",df.get("Operating speed",pd.Series(dtype=str))))
    out=_ensure(out)
    print(f"[Baumer] Output: {len(out):,}")
    return out

# ── Main ─────────────────────────────────────────────────────────────────────
def main():
    p=argparse.ArgumentParser()
    p.add_argument("--kubler",  default="data/raw/kubler_rules.csv")
    p.add_argument("--epc",     default="data/raw/epc_rules.csv")
    p.add_argument("--nidec",   default="data/raw/nidec_av20_av25_av30b_av44.csv")
    p.add_argument("--unified", default="data/competitor_unified.csv")
    p.add_argument("--sick",    default="data/raw/Sick_incremental_final.csv")
    p.add_argument("--baumer",  default="data/raw/baumer_sample.xlsx")
    p.add_argument("--nidec_n", type=int,default=60_000)
    p.add_argument("--output",  default="data/competitor_unified.csv")
    p.add_argument("--dry-run", action="store_true")
    args=p.parse_args()

    parts=[]
    if Path(args.kubler).exists():  parts.append(load_kubler(args.kubler))
    else: print(f"[WARN] {args.kubler} not found")
    if Path(args.epc).exists():     parts.append(load_epc(args.epc,200_000))
    else: print(f"[WARN] {args.epc} not found")
    if Path(args.nidec).exists():   parts.append(load_nidec(args.nidec,args.nidec_n))
    else: print(f"[WARN] {args.nidec} not found")
    if Path(args.unified).exists():
        lw=load_lika_wachendorff(args.unified)
        if len(lw): parts.append(lw)
    if Path(args.sick).exists():    parts.append(load_sick(args.sick))
    else: print(f"[WARN] {args.sick} not found")
    if Path(args.baumer).exists():  parts.append(load_baumer(args.baumer))
    else: print(f"[WARN] {args.baumer} not found")
    if not parts: print("ERROR: No data."); sys.exit(1)

    combined=pd.concat(parts,ignore_index=True)
    print("\n"+"="*65)
    print("DATABASE SUMMARY")
    print("="*65)
    for mfr,g in combined.groupby("manufacturer",observed=True):
        fams=g["product_family"].nunique()
        nn=g[UNIFIED_COLS].notna().sum(axis=1).mean()
        print(f"  {mfr:<15} {len(g):>8,} rows  | {fams:>3} families | avg {nn:.1f} non-null fields")
    print(f"  {'TOTAL':<15} {len(combined):>8,} rows")
    print("="*65)
    ppr_col=pd.to_numeric(combined["resolution_ppr"],errors="coerce")
    compat=(ppr_col.isin(KUBLER_PPR_DISCRETE)|combined["is_programmable"].astype(bool)|ppr_col.isna())
    print(f"\nPPR Kübler-compatible: {compat.sum():,}/{len(combined):,} ({100*compat.sum()/len(combined):.1f}%)")
    cov=combined[UNIFIED_COLS].notna().mean().sort_values(ascending=False)
    print("\nColumn coverage (top 15):")
    for col,pct in cov.head(15).items():
        print(f"  {col:<35} {pct*100:5.1f}%")
    if args.dry_run:
        print("\n[dry-run] Not writing."); return
    Path(args.output).parent.mkdir(parents=True,exist_ok=True)
    combined.to_csv(args.output,index=False)
    print(f"\n✓ Written: {args.output}  ({len(combined):,} rows × {len(UNIFIED_COLS)} cols)")

if __name__=="__main__":
    main()
