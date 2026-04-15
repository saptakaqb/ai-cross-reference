# -*- coding: utf-8 -*-
"""
schema.py  —  Unified encoder schema  v11
==========================================
Manufacturers: Kübler · Lika · Wachendorff · Sick · Baumer · EPC · Nidec
"""

import re as _re

CANONICAL_COLUMNS = [
    "manufacturer","part_number","product_family","encoder_type","sensing_method",
    "source_pdf","order_pattern","resolution_ppr","ppr_range_min","ppr_range_max",
    "is_programmable","output_circuit_canonical","output_signals","num_output_channels",
    "max_output_freq_hz","supply_voltage_min_v","supply_voltage_max_v","output_current_ma",
    "power_consumption_typ_mw","reverse_polarity_protection","short_circuit_protection",
    "housing_diameter_mm","shaft_diameter_mm","shaft_type","flange_type","connection_type",
    "connector_pins","ip_rating","operating_temp_min_c","operating_temp_max_c",
    "max_speed_rpm_peak","shock_resistance","vibration_resistance","weight_g",
    "startup_torque_ncm","shaft_load_radial_n","shaft_load_axial_n","moment_of_inertia",
    "oc_shaft_type","oc_flange","oc_ppr","oc_interface","oc_connector",
]

COLUMN_MAP = {
    "manufacturer":"Manufacturer","part_number":"Part Number",
    "product_family":"Product Family","encoder_type":"Encoder Type",
    "sensing_method":"Sensing Method","source_pdf":"Source PDF",
    "order_pattern":"Order Pattern","resolution_ppr":"Resolution (PPR)",
    "ppr_range_min":"PPR Range Min","ppr_range_max":"PPR Range Max",
    "is_programmable":"Programmable","output_circuit_canonical":"Output Circuit",
    "output_signals":"Output Signals","num_output_channels":"Output Channels",
    "max_output_freq_hz":"Max Output Freq (Hz)","supply_voltage_min_v":"Supply Voltage Min (V)",
    "supply_voltage_max_v":"Supply Voltage Max (V)","output_current_ma":"Output Current (mA)",
    "power_consumption_typ_mw":"Power Consumption (mW)",
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

MFR_CANONICAL = {
    "kubler":"Kubler","kuebler":"Kubler","kuebler (kubler)":"Kubler","kübler":"Kubler",
    "epc":"EPC","encoder products company":"EPC","encoder products":"EPC",
    "lika":"Lika","lika electronic":"Lika",
    "wachendorff":"Wachendorff",
    "nidec":"Nidec","nidec (avtron)":"Nidec","avtron":"Nidec",
    "sick":"Sick","baumer":"Baumer","dynapar":"Dynapar",
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

def ppr_score(source_ppr,source_prog,source_min,source_max,
              cand_ppr,cand_prog,cand_min,cand_max)->tuple:
    def _f(v):
        try: return float(v) if v and str(v) not in ("nan","None") else None
        except: return None
    src_ppr=_f(source_ppr); cnd_ppr=_f(cand_ppr)
    src_min=_f(source_min); src_max=_f(source_max)
    cnd_min=_f(cand_min);   cnd_max=_f(cand_max)
    if src_ppr and cnd_ppr:
        ratio=min(src_ppr,cnd_ppr)/max(src_ppr,cnd_ppr)
        if ratio==1.0: return(1.0,f"Exact match ({int(src_ppr)} PPR)")
        if ratio>=0.95: return(0.95,f"Near match ({int(src_ppr)} vs {int(cnd_ppr)}, {ratio:.1%})")
        if ratio>=0.75: return(ratio*0.85,f"Partial match ({ratio:.1%})")
        if ratio>=0.5: return(ratio*0.6,f"Weak match ({ratio:.1%})")
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
    "shaft_type":{"tier":1,"weight":0.00,"label":"Shaft Type","col":"shaft_type","unit":"",
        "scoring_rule":"Score=0 if Solid≠Hollow.","direction":"Binary",
        "rationale":"Physical incompatibility — cannot adapt without redesign."},
    "hollow_bore":{"tier":1,"weight":0.00,"label":"Hollow Bore Diameter","col":"shaft_diameter_mm","unit":"mm",
        "scoring_rule":"Score=0 if |Δbore|>1mm for hollow pairs.","direction":"Binary ±1mm",
        "rationale":"Hollow bore must fit the shaft it slides onto."},
    "output_voltage_class":{"tier":1,"weight":0.00,"label":"Output Voltage Class","col":"output_circuit_canonical","unit":"",
        "scoring_rule":"Score=0 if TTL(5V) crossed with HTL(10-30V). Push-Pull is universal.","direction":"Binary by voltage class",
        "rationale":"Feeding 24V into 5V input destroys the PLC card."},
    "resolution_ppr":{"tier":2,"weight":0.25,"label":"Resolution (PPR)","col":"resolution_ppr","unit":"PPR",
        "scoring_rule":"Exact=1.0. Within 5%=0.85. Within 25%=0.6×ratio. >25%=0.0. Prog range match=1.0.","direction":"Ratio; range containment",
        "rationale":"PPR defines fundamental accuracy. 2× mismatch requires controller reconfiguration."},
    "output_circuit_canonical":{"tier":2,"weight":0.20,"label":"Output Circuit","col":"output_circuit_canonical","unit":"",
        "scoring_rule":"Exact=1.0. Same voltage class=0.6. Adjacent=0.3. Cross-class=0.0.","direction":"Categorical with partial credit",
        "rationale":"Must match controller input type. Mismatch causes missed pulses or hardware damage."},
    "housing_diameter_mm":{"tier":2,"weight":0.15,"label":"Housing / Flange Diameter","col":"housing_diameter_mm","unit":"mm",
        "scoring_rule":"Exact=1.0. ≤1mm=0.9. ≤5mm=0.5. >5mm=0.0. >30% diff → cap at 0.75.","direction":"Continuous; capped >30%",
        "rationale":"Flange OD determines mounting hole pattern. 58mm cannot replace 80mm without adapter."},
    "shaft_diameter_mm":{"tier":2,"weight":0.12,"label":"Shaft Diameter (solid)","col":"shaft_diameter_mm","unit":"mm",
        "scoring_rule":"Exact=1.0. ≤0.5mm=0.9. ≤2mm=0.5. >2mm=0.0.","direction":"Continuous ±0.5mm tolerance",
        "rationale":"Coupling must match both shaft diameters. Mismatch = shaft runout, bearing overload."},
    "ip_rating":{"tier":3,"weight":0.07,"label":"IP Rating","col":"ip_rating","unit":"",
        "scoring_rule":"Candidate IP≥source=1.0. One level below=0.7. Two+=0.3.","direction":"Directional; higher always OK",
        "rationale":"IP is a minimum protection requirement. Over-specifying is always safe."},
    "operating_temp":{"tier":3,"weight":0.06,"label":"Operating Temperature","col":"operating_temp_max_c","unit":"°C",
        "scoring_rule":"Hard boundary: candidate max < source max−10°C=0. Otherwise proportional.","direction":"Directional; hard boundary >10°C",
        "rationale":"Outside rated range causes signal jitter, bearing failure."},
    "supply_voltage":{"tier":3,"weight":0.05,"label":"Supply Voltage Range","col":"supply_voltage_min_v","unit":"V",
        "scoring_rule":"Overlap=1.0. ≤5V gap=0.7. No overlap=0.3.","direction":"Overlap / containment",
        "rationale":"Encoder supply must match machine PSU."},
    "max_speed_rpm_peak":{"tier":3,"weight":0.05,"label":"Max Speed (Peak)","col":"max_speed_rpm_peak","unit":"RPM",
        "scoring_rule":"Candidate≥source=1.0. <90% of source → cap total at 0.70.","direction":"Directional; cap 0.70 if <90%",
        "rationale":"Exceeding rated speed causes mechanical failure."},
    "connection_type":{"tier":3,"weight":0.05,"label":"Connection Type","col":"connection_type","unit":"",
        "scoring_rule":"Exact=1.0. M12↔M23=0.7. Connector↔Cable=0.4. Different=0.2.","direction":"Categorical with adapter adjacency",
        "rationale":"Connector type affects ease of field replacement."},
    "weight_g":{"tier":0,"weight":0.00,"label":"Weight","col":"weight_g","unit":"g",
        "scoring_rule":"Display only.","direction":"Display only","rationale":"Relevant for light-duty applications."},
    "startup_torque_ncm":{"tier":0,"weight":0.00,"label":"Startup Torque","col":"startup_torque_ncm","unit":"Ncm",
        "scoring_rule":"Display only.","direction":"Display only","rationale":"Negligible for most motors."},
    "num_output_channels":{"tier":0,"weight":0.00,"label":"Output Channels (A/B/Z)","col":"num_output_channels","unit":"",
        "scoring_rule":"Display only.","direction":"Display only","rationale":"Z/index pulse for homing."},
    "output_current_ma":{"tier":0,"weight":0.00,"label":"Output Current","col":"output_current_ma","unit":"mA",
        "scoring_rule":"Display only.","direction":"Display only","rationale":"Relevant for long cable runs."},
    "max_output_freq_hz":{"tier":0,"weight":0.00,"label":"Max Output Frequency","col":"max_output_freq_hz","unit":"Hz",
        "scoring_rule":"Display only.","direction":"Display only","rationale":"PPR×max_RPM/60."},
    "shock_resistance":{"tier":0,"weight":0.00,"label":"Shock Resistance","col":"shock_resistance","unit":"",
        "scoring_rule":"Display only.","direction":"Display only","rationale":"Important for mobile/stamping machinery."},
    "vibration_resistance":{"tier":0,"weight":0.00,"label":"Vibration Resistance","col":"vibration_resistance","unit":"",
        "scoring_rule":"Display only.","direction":"Display only","rationale":"Important for high-vibration environments."},
    "sensing_method":{"tier":0,"weight":0.00,"label":"Sensing Method","col":"sensing_method","unit":"",
        "scoring_rule":"Display only.","direction":"Display only","rationale":"Optical vs magnetic."},
    "flange_type":{"tier":0,"weight":0.00,"label":"Flange Type","col":"flange_type","unit":"",
        "scoring_rule":"Display only.","direction":"Display only","rationale":"Mounting pattern info."},
    "shaft_load_radial_n":{"tier":0,"weight":0.00,"label":"Radial Shaft Load","col":"shaft_load_radial_n","unit":"N",
        "scoring_rule":"Display only.","direction":"Display only","rationale":"For belt/gear-driven encoders."},
    "shaft_load_axial_n":{"tier":0,"weight":0.00,"label":"Axial Shaft Load","col":"shaft_load_axial_n","unit":"N",
        "scoring_rule":"Display only.","direction":"Display only","rationale":"For vertical mounting."},
    "ppr_range_min":{"tier":0,"weight":0.00,"label":"PPR Range Min","col":"ppr_range_min","unit":"PPR",
        "scoring_rule":"Used by PPR scoring.","direction":"Scoring input","rationale":"Lower bound of programmable PPR range."},
    "ppr_range_max":{"tier":0,"weight":0.00,"label":"PPR Range Max","col":"ppr_range_max","unit":"PPR",
        "scoring_rule":"Used by PPR scoring.","direction":"Scoring input","rationale":"Upper bound of programmable PPR range."},
    "is_programmable":{"tier":0,"weight":0.00,"label":"Programmable","col":"is_programmable","unit":"",
        "scoring_rule":"Flag used by PPR scoring.","direction":"Boolean flag","rationale":"Encoder supports user-settable PPR."},
    "operating_temp_min_c":{"tier":0,"weight":0.00,"label":"Operating Temp Min","col":"operating_temp_min_c","unit":"°C",
        "scoring_rule":"Used by operating_temp scoring.","direction":"Scoring input","rationale":"Lower operating temperature bound."},
    "operating_temp_max_c":{"tier":0,"weight":0.00,"label":"Operating Temp Max","col":"operating_temp_max_c","unit":"°C",
        "scoring_rule":"Used by operating_temp scoring.","direction":"Scoring input","rationale":"Upper operating temperature bound."},
    "supply_voltage_min_v":{"tier":0,"weight":0.00,"label":"Supply Voltage Min","col":"supply_voltage_min_v","unit":"V",
        "scoring_rule":"Used by supply_voltage scoring.","direction":"Scoring input","rationale":"Lower supply voltage bound."},
    "supply_voltage_max_v":{"tier":0,"weight":0.00,"label":"Supply Voltage Max","col":"supply_voltage_max_v","unit":"V",
        "scoring_rule":"Used by supply_voltage scoring.","direction":"Scoring input","rationale":"Upper supply voltage bound."},
    "moment_of_inertia":{"tier":0,"weight":0.00,"label":"Moment of Inertia","col":"moment_of_inertia","unit":"",
        "scoring_rule":"Display only.","direction":"Display only","rationale":"Relevant for high-acceleration servo systems."},
}

_scored = [m["weight"] for m in UNIFIED_SCHEMA.values() if m["tier"] in (2,3)]
assert abs(sum(_scored)-1.0)<1e-6, f"Tier 2+3 weights must sum to 1.0, got {sum(_scored):.4f}"
