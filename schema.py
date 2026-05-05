# -*- coding: utf-8 -*-
"""
schema.py  —  Unified encoder schema  v12  (V17)
=================================================
V17 additions:
  • is_atex_certified          T1 hard stop: ATEX source cannot match non-ATEX
  • shaft_insulation_v         T0 warning flag
  • corrosion_protection_class T3 soft: ISO 12944-2 environment class
  • sensing_method promoted to T3 (weight 0.04) — optical vs magnetic now scored
"""
import re as _re

def safe_int(v):
    if v is None: return None
    try:
        f = float(v)
        import math as _m
        return None if _m.isnan(f) else int(f)
    except (TypeError, ValueError): return None

CANONICAL_COLUMNS = [
    "manufacturer","part_number","product_family","encoder_type","sensing_method",
    "source_pdf","order_pattern",
    "resolution_ppr","ppr_range_min","ppr_range_max",
    "is_programmable","output_circuit_canonical","output_signals","num_output_channels",
    "max_output_freq_hz","supply_voltage_min_v","supply_voltage_max_v","output_current_ma",
    "power_consumption_typ_mw","reverse_polarity_protection","short_circuit_protection",
    "housing_diameter_mm","shaft_diameter_mm","shaft_type","flange_type","connection_type",
    "connector_pins","ip_rating","operating_temp_min_c","operating_temp_max_c",
    "max_speed_rpm_peak","shock_resistance","vibration_resistance","weight_g",
    "startup_torque_ncm","shaft_load_radial_n","shaft_load_axial_n","moment_of_inertia",
    "oc_shaft_type","oc_flange","oc_ppr","oc_interface","oc_connector",
    "product_url",
    # V17 NEW
    "is_atex_certified",
    "shaft_insulation_v",
    "corrosion_protection_class",
]

COLUMN_MAP = {
    "manufacturer":"Manufacturer","part_number":"Part Number",
    "product_family":"Product Family","encoder_type":"Encoder Type",
    "sensing_method":"Sensing Method","source_pdf":"Source PDF",
    "order_pattern":"Order Pattern","product_url":"Product URL",
    "resolution_ppr":"Resolution (PPR)","ppr_range_min":"PPR Range Min",
    "ppr_range_max":"PPR Range Max","is_programmable":"Programmable",
    "output_circuit_canonical":"Output Circuit","output_signals":"Output Signals",
    "num_output_channels":"Output Channels","max_output_freq_hz":"Max Output Freq (Hz)",
    "supply_voltage_min_v":"Supply Voltage Min (V)","supply_voltage_max_v":"Supply Voltage Max (V)",
    "output_current_ma":"Output Current (mA)","power_consumption_typ_mw":"Power Consumption (mW)",
    "reverse_polarity_protection":"Reverse Polarity Protection",
    "short_circuit_protection":"Short Circuit Protection",
    "housing_diameter_mm":"Housing Diameter (mm)","shaft_diameter_mm":"Shaft / Bore Diameter (mm)",
    "shaft_type":"Shaft Type","flange_type":"Flange Type","connection_type":"Connection Type",
    "connector_pins":"Connector Pins","ip_rating":"IP Rating",
    "operating_temp_min_c":"Operating Temp Min (°C)","operating_temp_max_c":"Operating Temp Max (°C)",
    "max_speed_rpm_peak":"Max Speed (RPM)","shock_resistance":"Shock Resistance",
    "vibration_resistance":"Vibration Resistance","weight_g":"Weight (g)",
    "startup_torque_ncm":"Startup Torque (Ncm)","shaft_load_radial_n":"Shaft Load Radial (N)",
    "shaft_load_axial_n":"Shaft Load Axial (N)","moment_of_inertia":"Moment of Inertia",
    "oc_shaft_type":"OC: Shaft Type","oc_flange":"OC: Flange",
    "oc_ppr":"OC: PPR","oc_interface":"OC: Interface","oc_connector":"OC: Connector",
    "is_atex_certified":"ATEX Certified",
    "shaft_insulation_v":"Shaft Insulation",
    "corrosion_protection_class":"Corrosion Protection",
}

OUTPUT_CIRCUIT_CANONICAL = {
    "ttl":"TTL RS422","TTL":"TTL RS422","TTL RS422":"TTL RS422","ttl rs422":"TTL RS422",
    "rs422":"TTL RS422","RS422":"TTL RS422","RS-422":"TTL RS422","rs-422":"TTL RS422",
    "line driver":"TTL RS422","Line Driver":"TTL RS422","line_driver":"TTL RS422",
    "Line Driver RS422":"TTL RS422","linedriverrs422":"TTL RS422",
    "differential":"TTL RS422","Differential":"TTL RS422","5v ttl":"TTL RS422","5V TTL":"TTL RS422",
    "push-pull":"Push-Pull","Push-Pull":"Push-Pull","push pull":"Push-Pull","Push Pull":"Push-Pull",
    "pushpull":"Push-Pull","PushPull":"Push-Pull","totem pole":"Push-Pull","pp":"Push-Pull",
    "htl":"Push-Pull","HTL":"Push-Pull","push-pull htl":"Push-Pull","Push-Pull HTL":"Push-Pull","10-30v":"Push-Pull",
    "TTL/HTL Universal":"TTL/HTL Universal","ttl/htl universal":"TTL/HTL Universal",
    "TTL HTL Universal":"TTL/HTL Universal","HTL or TTL":"TTL/HTL Universal","htl or ttl":"TTL/HTL Universal",
    "PP/LD Universal":"PP/LD Universal","pp/ld universal":"PP/LD Universal",
    "PP/LD":"PP/LD Universal","universal":"PP/LD Universal",
    "open collector":"Open Collector","Open Collector":"Open Collector",
    "oc":"Open Collector","opencollector":"Open Collector",
    "npn":"NPN Open Collector","NPN":"NPN Open Collector","NPN Open Collector":"NPN Open Collector",
    "pnp":"PNP Open Collector","PNP":"PNP Open Collector","PNP Open Collector":"PNP Open Collector",
    "sin/cos":"Sin/Cos","Sin/Cos":"Sin/Cos","sincos":"Sin/Cos","SinCos":"Sin/Cos",
    "1Vpp":"Sin/Cos","1vpp":"Sin/Cos","1 Vpp Sin/Cos":"Sin/Cos",
    "ssi":"SSI/BiSS","SSI":"SSI/BiSS","biss":"SSI/BiSS","BiSS":"SSI/BiSS","SSI/BiSS":"SSI/BiSS",
    "analog":"Analog","Analog":"Analog","0-10v":"Analog","4-20ma":"Analog",
}

OUTPUT_VOLTAGE_CLASS = {
    "TTL RS422":"low","Open Collector":"low",
    "NPN Open Collector":"low","PNP Open Collector":"low",
    "Push-Pull":"universal","TTL/HTL Universal":"universal","PP/LD Universal":"universal",
    "Sin/Cos":"analog","SSI/BiSS":"digital","Analog":"analog",
    "TTL":"low","RS422":"low","HTL":"high","push-pull":"universal","Sin/cos":"analog",
}

SHAFT_TYPE_CANONICAL = {
    "solid":"Solid","Solid":"Solid","shaft":"Solid","Shaft":"Solid",
    "hollow":"Hollow","Hollow":"Hollow","blind":"Hollow","blind hollow":"Hollow","Blind Hollow":"Hollow",
    "through":"Through Hollow","through hollow":"Through Hollow","Through Hollow":"Through Hollow",
}

FLANGE_TYPE_CANONICAL = {
    "synchro":"Synchro","servo":"Synchro","Synchro":"Synchro",
    "clamping":"Clamping","Clamping":"Clamping",
    "square":"Square","Square":"Square",
    "face mount":"Face Mount","face":"Face Mount","Face Mount":"Face Mount",
    "stator coupler":"Stator Coupler","stator":"Stator Coupler",
    "blind":"Blind Hollow",
}

CONNECTION_TYPE_CANONICAL = {
    "cable":"Cable","Cable":"Cable","axial cable":"Cable","radial cable":"Cable",
    "connector":"Connector","Connector":"Connector",
    "m12":"M12","M12":"M12","m23":"M23","M23":"M23",
    "m17":"M17","M17":"M17","m8":"M8","M8":"M8",
    "terminal":"Terminal","Terminal":"Terminal",
}

IP_HIERARCHY = {20:0,40:1,50:2,54:3,64:4,65:5,66:6,67:7,69:8}

# ISO 12944-2 corrosion class ranks (lower = less protection)
CORROSION_RANK = {
    "c1":1,"c2":2,"c3":3,"c4":4,"c5":5,"c5-m":5,"c5m":5,"cx":6,"im":5,
}

MFR_CANONICAL = {
    "kubler":"Kubler","kuebler":"Kubler","kuebler (kubler)":"Kubler","kübler":"Kubler",
    "epc":"EPC","encoder products company":"EPC","encoder products":"EPC",
    "lika":"Lika","lika electronic":"Lika",
    "wachendorff":"Wachendorff",
    "nidec":"Nidec","nidec (avtron)":"Nidec","avtron":"Nidec",
    "sick":"Sick","baumer":"Baumer","dynapar":"Dynapar","posital":"Posital",
}

def normalise_mfr(raw:str)->str:
    if not raw: return raw
    return MFR_CANONICAL.get(str(raw).strip().lower(),str(raw).strip())

def housing_from_family(s:str)->float|None:
    if not s or str(s) in ("nan","None",""): return None
    m=_re.search(r"(\d{2,3})",str(s))
    if m:
        val=float(m.group(1))
        if 25<=val<=150: return val
    return None

def parse_corrosion_rank(v:str)->int|None:
    """Extract ISO 12944-2 class rank integer from raw string."""
    if not v or str(v).strip().lower() in ("nan","none",""): return None
    v_lo=str(v).lower()
    m=_re.search(r'\bc(x|5-m|5m|[1-5])\b',v_lo)
    if m:
        cls="c"+m.group(1).lower().replace("-","")
        return CORROSION_RANK.get("c"+m.group(1)) or CORROSION_RANK.get(cls)
    return None

def ppr_score(source_ppr,source_prog,source_min,source_max,
              cand_ppr,cand_prog,cand_min,cand_max)->tuple:
    def _f(v):
        try: return float(v) if v and str(v) not in ("nan","None") else None
        except: return None
    src_ppr=_f(source_ppr); cnd_ppr=_f(cand_ppr)
    src_min=_f(source_min); src_max=_f(source_max)
    cnd_min=_f(cand_min);   cnd_max=_f(cand_max)

    if src_ppr and cnd_ppr:
        # Exact or within 1-count rounding
        if src_ppr==cnd_ppr or abs(src_ppr-cnd_ppr)<=1:
            return (1.0,f"Exact match ({int(src_ppr)} PPR)")
        ratio=min(src_ppr,cnd_ppr)/max(src_ppr,cnd_ppr)
        # V17: tighter penalty curve
        if ratio>=0.99: return(0.92,f"Near-exact ({int(src_ppr)} vs {int(cnd_ppr)}, {ratio:.1%})")
        if ratio>=0.95: return(0.78,f"Near match ({int(src_ppr)} vs {int(cnd_ppr)}, {ratio:.1%}) — recalibration needed")
        if ratio>=0.80: return(ratio*0.65,f"Partial match ({ratio:.1%}) — controller scaling required")
        if ratio>=0.50: return(ratio*0.45,f"Weak PPR match ({ratio:.1%})")
        return(0.0,f"PPR mismatch: {int(src_ppr)} vs {int(cnd_ppr)} ({ratio:.1%} < 50%)")

    if src_ppr and cand_prog and cnd_min is not None and cnd_max is not None:
        if cnd_min<=src_ppr<=cnd_max: return(1.0,f"⚙ Programmable — can be set to {int(src_ppr)} PPR")
        return(0.0,f"PPR {int(src_ppr)} outside range [{int(cnd_min)}–{int(cnd_max)}]")

    if source_prog and src_min is not None and src_max is not None and cnd_ppr:
        if src_min<=cnd_ppr<=src_max: return(1.0,f"⚙ Source programmable to {int(cnd_ppr)} PPR")
        return(0.0,f"PPR {int(cnd_ppr)} outside source range [{int(src_min)}–{int(src_max)}]")

    if source_prog and cand_prog:
        s0=src_min or 1; s1=src_max or 0
        c0=cnd_min or 1; c1=cnd_max or 0
        if min(s1,c1)-max(s0,c0)>0: return(1.0,"⚙ Both programmable — ranges overlap")
        return(0.0,"PPR ranges do not overlap")

    return(0.5,"PPR data incomplete — neutral score")


UNIFIED_SCHEMA = {
    # T1 Hard Stops
    "shaft_type":{"tier":1,"weight":0.00,"label":"Shaft Type","col":"shaft_type","unit":"",
        "scoring_rule":"Score=0 if Solid≠Hollow.",
        "rationale":"Physical incompatibility — cannot adapt without redesign."},
    "hollow_bore":{"tier":1,"weight":0.00,"label":"Hollow Bore Diameter","col":"shaft_diameter_mm","unit":"mm",
        "scoring_rule":"Score=0 if |Δbore|>1mm for hollow pairs.",
        "rationale":"Hollow bore must fit the shaft it slides onto."},
    "output_voltage_class":{"tier":1,"weight":0.00,"label":"Output Voltage Class","col":"output_circuit_canonical","unit":"",
        "scoring_rule":"Score=0 if TTL(5V) crossed with HTL(10-30V).",
        "rationale":"Feeding 24V into 5V input destroys the PLC card."},
    "is_atex_certified":{"tier":1,"weight":0.00,"label":"ATEX Certification","col":"is_atex_certified","unit":"",
        "scoring_rule":"Score=0 if source is ATEX but candidate is not.",
        "rationale":"Legal requirement for hazardous zones. Cannot substitute non-ATEX in Ex zones."},
    # T2 Near-Hard
    "resolution_ppr":{"tier":2,"weight":0.25,"label":"Resolution (PPR)","col":"resolution_ppr","unit":"PPR",
        "scoring_rule":"Exact=1.0. ≥99%=0.92. ≥95%=0.78. ≥80%=ratio×0.65. ≥50%=ratio×0.45. <50%=0.0.",
        "rationale":"PPR defines fundamental accuracy."},
    "output_circuit_canonical":{"tier":2,"weight":0.20,"label":"Output Circuit","col":"output_circuit_canonical","unit":"",
        "scoring_rule":"Exact=1.0. Same class=0.6. Adjacent=0.3. Cross-class=0.0.",
        "rationale":"Must match controller input type."},
    "housing_diameter_mm":{"tier":2,"weight":0.15,"label":"Housing / Flange Diameter","col":"housing_diameter_mm","unit":"mm",
        "scoring_rule":"Exact=1.0. Linear decay. >30% diff → cap at 0.75.",
        "rationale":"Flange OD determines mounting hole pattern."},
    "shaft_diameter_mm":{"tier":2,"weight":0.12,"label":"Shaft Diameter","col":"shaft_diameter_mm","unit":"mm",
        "scoring_rule":"Exact=1.0. ≤0.5mm=0.9. Linear decay beyond.",
        "rationale":"Coupling must match both shaft diameters."},
    # T3 Soft
    "ip_rating":{"tier":3,"weight":0.07,"label":"IP Rating","col":"ip_rating","unit":"",
        "scoring_rule":"Candidate IP≥source=1.0. One level below=0.5. Two+=0.0.",
        "rationale":"IP is a minimum protection requirement."},
    "sensing_method":{"tier":3,"weight":0.04,"label":"Sensing Method","col":"sensing_method","unit":"",
        "scoring_rule":"Same=1.0. Optical↔Magnetic=0.3. Other diff=0.5.",
        "rationale":"Optical vs magnetic differ in accuracy and contamination tolerance."},
    "operating_temp":{"tier":3,"weight":0.05,"label":"Operating Temperature","col":"operating_temp_max_c","unit":"°C",
        "scoring_rule":"Hard boundary: candidate max < source max−10°C=0.",
        "rationale":"Outside rated range causes signal jitter, bearing failure."},
    "supply_voltage":{"tier":3,"weight":0.03,"label":"Supply Voltage Range","col":"supply_voltage_min_v","unit":"V",
        "scoring_rule":"Overlap=1.0. ≤5V gap=0.7. No overlap=0.3.",
        "rationale":"Encoder supply must match machine PSU."},
    "max_speed_rpm_peak":{"tier":3,"weight":0.04,"label":"Max Speed (Peak)","col":"max_speed_rpm_peak","unit":"RPM",
        "scoring_rule":"Candidate≥source=1.0. <90% of source → cap total at 0.70.",
        "rationale":"Exceeding rated speed causes mechanical failure."},
    "connection_type":{"tier":3,"weight":0.03,"label":"Connection Type","col":"connection_type","unit":"",
        "scoring_rule":"Exact=1.0. M12↔M23=0.7. Connector↔Cable=0.4. Different=0.2.",
        "rationale":"Connector type affects field replacement ease."},
    "corrosion_protection_class":{"tier":3,"weight":0.02,"label":"Corrosion Protection","col":"corrosion_protection_class","unit":"",
        "scoring_rule":"Same/higher class=1.0. 1 class below=0.7. 2 below=0.4. No rating when source rated=0.3.",
        "rationale":"ISO 12944-2 class required for marine/offshore/chemical environments."},
    # T0 Informational
    "weight_g":             {"tier":0,"weight":0.00,"label":"Weight","col":"weight_g","unit":"g"},
    "startup_torque_ncm":   {"tier":0,"weight":0.00,"label":"Startup Torque","col":"startup_torque_ncm","unit":"Ncm"},
    "num_output_channels":  {"tier":0,"weight":0.00,"label":"Output Channels","col":"num_output_channels","unit":""},
    "max_output_freq_hz":   {"tier":0,"weight":0.00,"label":"Max Output Frequency","col":"max_output_freq_hz","unit":"Hz"},
    "shock_resistance":     {"tier":0,"weight":0.00,"label":"Shock Resistance","col":"shock_resistance","unit":""},
    "vibration_resistance": {"tier":0,"weight":0.00,"label":"Vibration Resistance","col":"vibration_resistance","unit":""},
    "flange_type":          {"tier":0,"weight":0.00,"label":"Flange Type","col":"flange_type","unit":""},
    "connector_pins":       {"tier":0,"weight":0.00,"label":"Connector Pins","col":"connector_pins","unit":""},
    "shaft_load_radial_n":  {"tier":0,"weight":0.00,"label":"Radial Shaft Load","col":"shaft_load_radial_n","unit":"N"},
    "shaft_load_axial_n":   {"tier":0,"weight":0.00,"label":"Axial Shaft Load","col":"shaft_load_axial_n","unit":"N"},
    "ppr_range_min":        {"tier":0,"weight":0.00,"label":"PPR Range Min","col":"ppr_range_min","unit":"PPR"},
    "ppr_range_max":        {"tier":0,"weight":0.00,"label":"PPR Range Max","col":"ppr_range_max","unit":"PPR"},
    "is_programmable":      {"tier":0,"weight":0.00,"label":"Programmable","col":"is_programmable","unit":""},
    "operating_temp_min_c": {"tier":0,"weight":0.00,"label":"Operating Temp Min","col":"operating_temp_min_c","unit":"°C"},
    "operating_temp_max_c": {"tier":0,"weight":0.00,"label":"Operating Temp Max","col":"operating_temp_max_c","unit":"°C"},
    "supply_voltage_min_v": {"tier":0,"weight":0.00,"label":"Supply Voltage Min","col":"supply_voltage_min_v","unit":"V"},
    "supply_voltage_max_v": {"tier":0,"weight":0.00,"label":"Supply Voltage Max","col":"supply_voltage_max_v","unit":"V"},
    "moment_of_inertia":    {"tier":0,"weight":0.00,"label":"Moment of Inertia","col":"moment_of_inertia","unit":""},
    "shaft_insulation_v":   {"tier":0,"weight":0.00,"label":"Shaft Insulation","col":"shaft_insulation_v","unit":"",
        "scoring_rule":"Warning flag only — not scored.",
        "rationale":"Required for VFD motor bearing current protection."},
}

_scored=[m["weight"] for m in UNIFIED_SCHEMA.values() if m["tier"] in (2,3)]
assert abs(sum(_scored)-1.0)<1e-6, f"T2+T3 weights must sum to 1.0, got {sum(_scored):.6f}"
