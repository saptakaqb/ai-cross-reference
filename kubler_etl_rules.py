#!/usr/bin/env python3
"""
kubler_etl_rules.py  v3
========================
Generates rule-compliant Kübler encoder rows for 12 families across 4 datasheets.
Output schema matches kubler_final_csv.gz exactly (50 columns).

Each output circuit variant from the datasheet electrical table gets its own
rows with the correct per-circuit specs (supply voltage, power, reverse polarity
protection, signal levels, rise/fall times, load capacity, pulse frequency).

See KUBLER_DATASHEET_RULES.md and ELECTRICAL_SPECS_PER_CIRCUIT.md for full detail.

Usage:
  python kubler_etl_rules.py
  python kubler_etl_rules.py --output data/kubler_rules.csv --dry_run
  python kubler_etl_rules.py --include_us
"""

import argparse, csv, os, sys

COLUMNS = [
    "product_code","order_pattern","family","interface_canonical",
    "param_a_key","param_a_value","param_b_key","param_b_value",
    "param_c_key","param_c_value","param_d_key","param_d_value",
    "param_e_key","param_e_value","param_k_key","param_k_value",
    "param_l_key","param_l_value",
    "manufacturer","product_family","part_number","source_pdf","encoder_type",
    "resolution_ppr","ppr_range_min","ppr_range_max",
    "max_output_frequency_hz","max_speed_rpm","output_circuits","output_signals",
    "connection_type","signal_level_high_v","signal_level_low_v","rise_fall_time",
    "supply_voltage","power_consumption","reverse_polarity_protection",
    "short_circuit_protection","permissible_load_per_channel",
    "shaft_diameter_mm","shaft_material","weight_kg","startup_torque",
    "shaft_load_radial_n","shaft_load_axial_n","moment_of_inertia",
    "protection_rating","operating_temp_range","shock_resistance","vibration_resistance",
]

def _r(): return {c:"" for c in COLUMNS}

def _fill_base(r, family, pdf, pn, pattern):
    r["manufacturer"]=r["product_family"]=r["family"]=family
    r["part_number"]=r["product_code"]=pn
    r["source_pdf"]=pdf; r["encoder_type"]="incremental"
    r["order_pattern"]=pattern; r["shaft_material"]="stainless steel"

def _fill_elec(r, canon, supply_v, power, rev_pol, sigh, sigl, rf, load, freq):
    r["output_circuits"]=r["interface_canonical"]=canon
    r["supply_voltage"]=supply_v; r["power_consumption"]=power
    r["reverse_polarity_protection"]=rev_pol; r["short_circuit_protection"]="yes"
    r["signal_level_high_v"]=sigh; r["signal_level_low_v"]=sigl
    r["rise_fall_time"]=rf; r["permissible_load_per_channel"]=load
    r["max_output_frequency_hz"]=freq

# ── PPR lists ─────────────────────────────────────────────────────────────────
PPR_KIS40 = [10,25,50,60,100,120,150,200,250,360,400,500,512,600,
             1000,1024,1500,1800,2000,2048,2500,2560]
PPR_5000  = [1,2,4,5,10,12,14,20,25,28,30,32,36,50,60,64,80,100,120,125,
             150,180,200,240,250,256,300,342,360,375,400,500,512,600,625,
             720,800,900,1000,1024,1200,1250,1500,1800,2000,2048,2500,
             3000,3600,4000,4096,5000]
PPR_A     = [50,360,512,600,1000,1024,1500,2000,2048,2500,4096,5000]
PPR_A_SC  = [1024,1500,2000,2048,2500,4096,5000]  # SinCos only >= 1024

# ── Per-circuit electrical specs ──────────────────────────────────────────────
# Format: (c_code, c_label, canon, supply_v, power, rev_pol, sig_high, sig_low,
#          rise_fall, load_ch, pulse_freq)

KIS40_CIRCUITS = [
    ("6","RS422(inv)/5V","RS422","5 V DC","typ. 40 mA / max. 90 mA","no",
     "min. 2.5 V","max. 0.5 V","max. 200 ns","max. +/- 20 mA","250 kHz"),
    ("C","RS422(inv)/5-30V","RS422","5 ... 30 V DC","typ. 40 mA / max. 165 mA","yes",
     "min. 2.5 V","max. 0.5 V","max. 200 ns","max. +/- 20 mA","250 kHz"),
    ("4","PP(inv)/10-30V","Push-Pull","10 ... 30 V DC","typ. 50 mA / max. 100 mA","yes",
     "min. +V - 2.0 V","max. 0.5 V","max. 1 us","max. +/- 20 mA","250 kHz"),
    ("8","PP(no-inv)/10-30V","Push-Pull","10 ... 30 V DC","typ. 50 mA / max. 100 mA","yes",
     "min. +V - 2.0 V","max. 0.5 V","max. 1 us","max. +/- 20 mA","250 kHz"),
    ("B","PP(inv)/5-30V","Push-Pull","5 ... 30 V DC","typ. 50 mA / max. 100 mA","yes",
     "min. +V - 2.0 V","max. 0.5 V","max. 1 us","max. +/- 20 mA","250 kHz"),
    ("3","OC(inv)/10-30V","Open Collector","10 ... 30 V DC","100 mA","yes",
     "","","","20 mA sink at 30 V DC","250 kHz"),
    ("7","OC(no-inv)/10-30V","Open Collector","10 ... 30 V DC","100 mA","yes",
     "","","","20 mA sink at 30 V DC","250 kHz"),
    ("A","OC(inv)/5-30V","Open Collector","5 ... 30 V DC","100 mA","yes",
     "","","","20 mA sink at 30 V DC","250 kHz"),
]

# 5000/5020: (c_code, label, canon, supply_v, power, rev_pol, sigh, sigl, rf, load, freq, is_us, is_no_inv_circuit)
S5000_CIRCUITS = [
    ("1","RS422/5-30V","RS422","5 ... 30 V DC","typ. 40 mA / max. 90 mA","yes",
     "min. 2.5 V","max. 0.5 V","max. 200 ns","max. +/- 20 mA","300 kHz",False,False),
    ("4","RS422/5V","RS422","5 V DC (+/-5%)","typ. 40 mA / max. 90 mA","no",
     "min. 2.5 V","max. 0.5 V","max. 200 ns","max. +/- 20 mA","300 kHz",False,False),
    ("2","PP-7272/5-30V","Push-Pull","5 ... 30 V DC","typ. 50 mA / max. 100 mA","no",
     "min. +V - 2.0 V","max. 0.5 V","max. 1 us","max. +/- 20 mA","300 kHz",False,False),
    ("5","PP/10-30V","Push-Pull","10 ... 30 V DC","typ. 50 mA / max. 100 mA","yes",
     "min. +V - 1.0 V","max. 0.5 V","max. 1 us","max. +/- 20 mA","300 kHz",False,True),
    ("8","PP-nocap/5-30V","Push-Pull","5 ... 30 V DC","typ. 50 mA / max. 100 mA","no",
     "min. +V - 2.0 V","max. 0.5 V","max. 1 us","max. +/- 20 mA","300 kHz",True,False),
    ("3","OC/5-30V","Open Collector","5 ... 30 V DC","100 mA","no",
     "","","","20 mA sink at 30 V DC","300 kHz",True,True),
]

# K58I: RS@5-30V(c=2), RS@5V(c=1), PP@5-30V(c=2)
# (iface, supply_c, supply_label, canon, supply_v, power, rev_pol, sigh, sigl, rf, load, freq)
K58I_CIRCUITS = [
    ("RS","2","RS422 5-30V","RS422","5 ... 30 V DC","typ. 40 mA / max. 90 mA","yes",
     "min. 2.5 V","max. 0.5 V","max. 200 ns","max. +/- 20 mA","300 kHz"),
    ("RS","1","RS422 5V","RS422","5 V DC (+/-5%)","typ. 40 mA / max. 90 mA","no",
     "min. 2.5 V","max. 0.5 V","max. 200 ns","max. +/- 20 mA","300 kHz"),
    ("PP","2","PP 5-30V","Push-Pull","5 ... 30 V DC","typ. 40 mA / max. 90 mA","yes",
     "min. 2.5 V (+V-1V)","max. 0.5 V","max. 200 ns","max. +/- 20 mA","300 kHz"),
]

# K58I-PR: RS@5-30V, PP@5-30V (both c=2 only, both rev_pol=YES)
K58IPR_CIRCUITS = [
    ("RS","RS422","5 ... 30 V DC","typ. 40 mA / max. 90 mA","yes",
     "min. 2.5 V","max. 0.5 V","max. 200 ns","max. +/- 20 mA","300 kHz"),
    ("PP","Push-Pull","5 ... 30 V DC","typ. 40 mA / max. 90 mA","yes",
     "min. 2.5 V","max. 0.5 V","max. 200 ns","max. +/- 20 mA","300 kHz"),
]

# K80I: RS@5V, RS@5-30V, PP@5-30V, SC@5V, SC@5-30V
# (iface, supply_c, supply_label, canon, supply_v, power, rev_pol, sigh, sigl, rf, load, freq, is_sincos)
K80I_CIRCUITS = [
    ("RS","1","RS422 5V","RS422","5 V DC (+/-5%)","typ. 40 mA / max. 90 mA","no",
     "min. 2.5 V","max. 0.5 V","max. 200 ns","max. +/- 20 mA","300 kHz",False),
    ("RS","2","RS422 5-30V","RS422","5 ... 30 V DC","typ. 40 mA / max. 90 mA","yes",
     "min. 2.5 V","max. 0.5 V","max. 200 ns","max. +/- 20 mA","300 kHz",False),
    ("PP","2","PP 5-30V","Push-Pull","5 ... 30 V DC","typ. 40 mA / max. 90 mA","yes",
     "min. 2.5 V","max. 0.5 V","max. 200 ns","max. +/- 20 mA","300 kHz",False),
    ("SC","1","SinCos 5V","Sin/Cos","5 V DC (+/-5%)","typ. 65 mA / max. 110 mA","no",
     "1 Vss (+/-20%)","","< 180 kHz (-3dB)","","180 kHz",True),
    ("SC","2","SinCos 5-30V","Sin/Cos","5 ... 30 V DC","typ. 65 mA / max. 110 mA","yes",
     "1 Vss (+/-20%)","","< 180 kHz (-3dB)","","180 kHz",True),
]

# K80I-PR: RS@5-30V, PP@5-30V (no SinCos, no 5V RS)
K80IPR_CIRCUITS = [
    ("RS","RS422","5 ... 30 V DC","typ. 40 mA / max. 90 mA","yes",
     "min. 2.5 V","max. 0.5 V","max. 200 ns","max. +/- 20 mA","300 kHz"),
    ("PP","Push-Pull","5 ... 30 V DC","typ. 40 mA / max. 90 mA","yes",
     "min. 2.5 V","max. 0.5 V","max. 200 ns","max. +/- 20 mA","300 kHz"),
]

# A020 circuits
A020_CIRCUITS = [
    ("1","RS422(inv)/5V","RS422","5 V DC","typ. 40 mA / max. 90 mA","no",
     "min. 2.5 V","max. 0.5 V","max. 200 ns","max. +/- 20 mA","300 kHz",False),
    ("4","RS422(inv)/10-30V","RS422","10 ... 30 V DC","typ. 40 mA / max. 90 mA","yes",
     "min. 2.5 V","max. 0.5 V","max. 200 ns","max. +/- 20 mA","300 kHz",False),
    ("2","PP(no-inv)/10-30V","Push-Pull","10 ... 30 V DC","typ. 50 mA / max. 100 mA","yes",
     "min. +V - 2.0 V","max. 0.5 V","max. 1 us","max. +/- 20 mA","300 kHz",False),
    ("5","PP(inv)/5-30V","Push-Pull","5 ... 30 V DC","typ. 50 mA / max. 100 mA","yes",
     "min. +V - 2.0 V","max. 0.5 V","max. 1 us","max. +/- 20 mA","300 kHz",False),
    ("3","PP(inv)/10-30V","Push-Pull","10 ... 30 V DC","typ. 50 mA / max. 100 mA","yes",
     "min. +V - 2.0 V","max. 0.5 V","max. 1 us","max. +/- 20 mA","300 kHz",False),
    ("A","PP-7272/5-30V","Push-Pull","5 ... 30 V DC","typ. 50 mA / max. 100 mA","yes",
     "min. +V - 2.0 V","max. 0.5 V","max. 1 us","max. +/- 20 mA","300 kHz",False),
    ("8","SinCos/5V","Sin/Cos","5 V DC (+/-5%)","typ. 65 mA / max. 110 mA","no",
     "1 Vpp (+/-20%)","","< 180 kHz (-3dB)","","180 kHz",True),
    ("9","SinCos/10-30V","Sin/Cos","10 ... 30 V DC","typ. 65 mA / max. 110 mA","yes",
     "1 Vpp (+/-20%)","","< 180 kHz (-3dB)","","180 kHz",True),
]

# A02H: PP has HIGHER load (+/-30mA) and different voltage levels vs A020!
# (c_code, label, canon, supply_v, power, rev_pol, sigh, sigl, rf, load, freq, is_sincos, is_us, is_no_inv)
A02H_CIRCUITS = [
    ("1","RS422/5V","RS422","5 V DC (+/-5%)","typ. 40 mA / max. 90 mA","no",
     "min. 2.5 V","max. 0.5 V","max. 200 ns","max. +/- 20 mA","300 kHz",False,False,False),
    ("5","PP(inv)/5-30V","Push-Pull","5 ... 30 V DC","typ. 80 mA / max. 150 mA","yes",
     "min. +V - 3 V","max. 2.5 V","max. 1 us","max. +/- 30 mA","300 kHz",False,False,False),
    ("3","PP(inv)/10-30V","Push-Pull","10 ... 30 V DC","typ. 80 mA / max. 150 mA","yes",
     "min. +V - 3 V","max. 2.5 V","max. 1 us","max. +/- 30 mA","300 kHz",False,False,False),
    ("A","PP-7272/5-30V","Push-Pull","5 ... 30 V DC","typ. 50 mA / max. 100 mA","no",
     "min. +V - 2.0 V","max. 0.5 V","max. 1 us","max. +/- 20 mA","300 kHz",False,False,False),
    ("8","SinCos/5V","Sin/Cos","5 V DC (+/-5%)","typ. 65 mA / max. 110 mA","no",
     "1 Vpp (+/-20%)","","< 180 kHz (-3dB)","","180 kHz",True,False,False),
    ("9","SinCos/10-30V","Sin/Cos","10 ... 30 V DC","typ. 65 mA / max. 110 mA","yes",
     "1 Vpp (+/-20%)","","< 180 kHz (-3dB)","","180 kHz",True,False,False),
    ("D","RS422/5-30V(US)","RS422","5 ... 30 V DC","typ. 40 mA / max. 90 mA","yes",
     "min. 2.5 V","max. 0.5 V","max. 200 ns","max. +/- 20 mA","300 kHz",False,True,False),
]

# ── Connection tables ─────────────────────────────────────────────────────────
KIS40_CONNS = [
    ("1","axial cable, 2m PVC",None),
    ("2","radial cable, 2m PVC",None),
    ("4","radial cable 0.5m PVC, M12 5-pin",5),
    ("6","radial cable 0.5m PVC, M12 8-pin",8),
    ("A","axial cable, special length PVC",None),
    ("B","radial cable, special length PVC",None),
]
# 5000: (code, label, pins, is_us, is_no_inv, is_mil)
S5000_CONNS = [
    ("1","axial cable, 1m PVC",None,False,False,False),
    ("A","axial cable, special length PVC",None,False,False,False),
    ("2","radial cable, 1m PVC",None,False,False,False),
    ("B","radial cable, special length PVC",None,False,False,False),
    ("P","axial M12 5-pin",5,False,True,False),
    ("R","radial M12 5-pin",5,False,True,False),
    ("3","axial M12 8-pin",8,False,False,False),
    ("4","radial M12 8-pin",8,False,False,False),
    ("7","axial M23 12-pin",12,False,False,False),
    ("8","radial M23 12-pin",12,False,False,False),
    ("Y","radial MIL 10-pin",10,False,False,True),
    ("W","radial MIL 7-pin",7,False,True,True),
    ("9","radial MIL 6-pin",6,True,True,True),
    ("L","radial cable+M12 8-pin spec",8,False,False,False),
    ("M","radial cable+M23 12-pin spec",12,False,False,False),
    ("N","radial cable+Sub-D 9-pin spec",9,False,False,False),
]
S5020_CONNS = [
    ("1","radial cable, 1m PVC",None,False,False,False),
    ("A","radial cable, special length PVC",None,False,False,False),
    ("E","tangential cable, 1m PVC",None,False,False,False),
    ("F","tangential cable, special length",None,False,False,False),
    ("R","radial M12 5-pin",5,False,True,False),
    ("2","radial M12 8-pin",8,False,False,False),
    ("4","radial M23 12-pin",12,False,False,False),
    ("6","radial MIL 7-pin",7,False,False,True),
    ("7","radial MIL 10-pin",10,False,False,True),
    ("H","tang 0.3m PVC+M12 8-pin",8,False,False,False),
    ("L","tang cable+M12 8-pin spec",8,False,False,False),
    ("M","tang cable+M23 12-pin spec",12,False,False,False),
    ("N","tang cable+Sub-D 9-pin spec",9,False,False,False),
]
# K58I: (code, label, pins, block_axial, block_cable, block_sincos)
K58I_CONNECTORS = [
    ("1","cable, open-ended",None,False,False,False),
    ("2","M12 8-pin",8,False,False,False),
    ("5","M12 8-pin (spec.assign)",8,False,True,False),
    ("3","M12 5-pin",5,False,False,False),
    ("6","M12 5-pin (spec.assign)",5,False,True,False),
    ("4","M23 12-pin",12,False,False,False),
    ("D","MIL 7-pin",7,True,True,False),
    ("H","MIL 7-pin (spec.assign)",7,True,True,False),
    ("E","MIL 10-pin",10,True,True,False),
    ("J","MIL 10-pin (spec.assign)",10,True,True,False),
]
# K80I: same as K58I but M12-5pin and MIL-7pin block SinCos
K80I_CONNECTORS = [
    ("1","cable, open-ended",None,False,False,False),
    ("2","M12 8-pin",8,False,False,False),
    ("5","M12 8-pin (spec.assign)",8,False,True,False),
    ("3","M12 5-pin",5,False,False,True),   # blocks SinCos
    ("6","M12 5-pin (spec.assign)",5,False,True,True),
    ("4","M23 12-pin",12,False,False,False),
    ("D","MIL 7-pin",7,True,True,True),     # blocks SinCos
    ("H","MIL 7-pin (spec.assign)",7,True,True,True),
    ("E","MIL 10-pin",10,True,True,False),
    ("J","MIL 10-pin (spec.assign)",10,True,True,False),
]
A020_CONNS=[("1","radial cable, 1m PVC",None),("A","radial cable, special length PVC",None),
            ("2","radial M23 12-pin",12),("E","radial M12 8-pin",8)]
# A02H: (code, label, pins, is_us, is_no_inv, ip_override)
A02H_CONNS=[
    ("1","radial cable, 1m PVC",None,False,False,None),
    ("A","radial cable, special length PVC",None,False,False,None),
    ("2","radial M23 12-pin",12,False,False,None),
    ("E","radial M12 8-pin",8,False,False,None),
    ("G","Sub-D 9-pin",9,False,False,40.0),  # IP40
    ("R","radial M12 5-pin",5,False,True,None),
    ("K","MIL 7-pin",7,True,True,None),
    ("D","MIL 10-pin",10,True,False,None),
]

# ── Shaft / bore tables ───────────────────────────────────────────────────────
KIS40_SHAFTS=[("3",6.0),("5",6.35),("6",8.0)]
KIH40_FLANGES=[("2","spring element, long"),("5","stator coupling o46mm")]
KIH40_BORES=[("2",6.0),("3",6.35),("4",8.0)]
S5000_FLANGES=[
    ("5","synchro o50.8mm","IP66/67",False),("6","synchro o50.8mm","IP65",False),
    ("7","clamping o58mm","IP66/67",False),  ("8","clamping o58mm","IP65",False),
    ("A","synchro o58mm","IP66/67",False),   ("B","synchro o58mm","IP65",False),
    ("C","square 63.5mm","IP66/67",False),   ("D","square 63.5mm","IP65",False),
    ("G","Euro o115mm","IP66/67",True),
    ("1","servo o50.8mm","IP66/67",True),("2","servo o50.8mm","IP65",True),
    ("3","square 52.3mm","IP66/67",True),("4","square 52.3mm","IP65",True),
    ("E","servo o63.5mm","IP66/67",True),("F","servo o63.5mm","IP65",True),
]
S5000_SHAFTS=[
    ("1",6.0,False,False),("2",6.35,False,False),("6",8.0,False,False),
    ("3",10.0,False,False),("4",9.525,False,False),("5",12.0,False,False),
    ("7",6.35,True,False),("8",9.525,True,False),("B",11.0,False,True),
]
S5020_FLANGES=[
    ("1","spring long","IP66/67",False),("2","spring long","IP65",False),
    ("3","torque long","IP66/67",False),("4","torque long","IP65",False),
    ("7","stator o65mm","IP66/67",False),("8","stator o65mm","IP65",False),
    ("C","stator o63mm","IP66/67",False),("D","stator o63mm","IP65",False),
    ("5","stator o57.2mm","IP66/67",True),("6","stator o57.2mm","IP65",True),
]
S5020_BORES=[("1",6.0),("2",6.35),("9",8.0),("4",9.525),("3",10.0),
             ("5",12.0),("6",12.7),("A",14.0),("8",15.0),("7",15.875)]
K58I_FLANGES=[("C5","clamping"),("S5","synchro"),("Q5","square 63.5mm"),("E5","Euro"),("V5","servo")]
K58I_SHAFTS=[("06",6.0,"S1"),("08",8.0,"S1"),("10",10.0,"S1"),("12",12.0,"S1"),
             ("1A",6.35,"S1"),("1B",6.35,"S1"),("2A",9.525,"S1"),("2B",9.525,"S1"),("11",11.0,"S3")]
K58I_HOLLOW_BORES=[
    ("06",6.0,True,["H1","H2","C1","C2"]),("08",8.0,True,["H1","H2","C1","C2"]),
    ("10",10.0,True,["H1","H2","C1","C2"]),("12",12.0,True,["H1","H2","C1","C2"]),
    ("14",14.0,True,["H1","C1"]),("15",15.0,True,["H1","C1"]),
    ("1A",6.35,True,["H1","H2","C1","C2"]),("2A",9.525,True,["H1","H2","C1","C2"]),
    ("3A",12.7,True,["H1","H2","C1","C2"]),
    ("16",16.0,False,["H1","H2","C1"]),("20",20.0,False,["H1","H2","C1"]),
    ("22",22.0,False,["H1","C1"]),("24",24.0,False,["H1"]),("25",25.0,False,["H1"]),
    ("4A",15.875,False,["H1","H2","C1"]),("5A",19.05,False,["H1","H2","C1"]),
    ("6A",22.23,False,["H2"]),("7A",25.4,False,["H1"]),
]
K58I_MOUNT_LE15=["15","25","35","45"]
K58I_MOUNT_GT15=["55","65","75"]
K80I_BORES_H1=[("28",28.0),("30",30.0),("35",35.0),("38",38.0),
               ("40",40.0),("42",42.0),("8A",28.575),("9A",31.75)]
K80I_BORES_H2=[("14",14.0),("15",15.0),("16",16.0),("18",18.0),("20",20.0),
               ("25",25.0),("28",28.0),("30",30.0),("32",32.0),("38",38.0),
               ("4A",15.875),("5A",19.05),("6A",22.225),("7A",25.4),
               ("8A",28.575),("9A",31.75)]
K80I_MOUNTS=["18","48","D8","58","68","78"]
A020_FLANGES=[("2","spring short"),("3","spring long"),("5","torque stop long")]
A020_BORES=[("C",20.0),("6",24.0),("5",25.0),("3",28.0),("A",30.0),
            ("2",38.0),("B",40.0),("1",42.0),("4",25.4)]
A02H_FLANGES=[("1","no mount aid",False),("2","spring short",False),
              ("3","spring long",False),("5","torque long",False),("6","torque short",True)]
A02H_BORES=[("C",20.0,False),("6",24.0,False),("5",25.0,False),("3",28.0,False),
            ("A",30.0,False),("H",35.0,False),("2",38.0,False),("B",40.0,False),
            ("1",42.0,False),("4",25.4,False),
            ("D",12.7,True),("E",15.875,True),("F",19.05,True),("G",28.575,True),("N",31.75,True)]

# ── Generators ────────────────────────────────────────────────────────────────
def gen_kis40():
    rows=[]
    for sh_c,sh_mm in KIS40_SHAFTS:
        for c in KIS40_CIRCUITS:
            c_c,c_l,canon,supply_v,power,rev_pol,sigh,sigl,rf,load,freq = c
            for d_c,d_l,_ in KIS40_CONNS:
                for ppr in PPR_KIS40:
                    r=_r(); ppr_s=str(ppr).zfill(4); pn=f"8.KIS40.1{sh_c}{c_c}{d_c}.{ppr_s}"
                    _fill_base(r,"KIS40","KIS40KIH40_en.pdf",pn,"8.KIS40.1{b}{c}{d}.{e}")
                    _fill_elec(r,canon,supply_v,power,rev_pol,sigh,sigl,rf,load,freq)
                    r.update({"param_a_key":"1","param_a_value":"clamping-synchro flange o40mm",
                              "param_b_key":sh_c,"param_b_value":f"o{sh_mm}mm",
                              "param_c_key":c_c,"param_c_value":c_l,
                              "param_d_key":d_c,"param_d_value":d_l,
                              "param_e_key":ppr_s,"param_e_value":str(ppr),
                              "resolution_ppr":ppr,"max_speed_rpm":"4500 min-1",
                              "connection_type":d_l,"shaft_diameter_mm":sh_mm,
                              "weight_kg":"approx. 0.17 kg","startup_torque":"< 0.05 Nm",
                              "shaft_load_radial_n":"40 N","shaft_load_axial_n":"20 N",
                              "protection_rating":"IP64","operating_temp_range":"-20 ... +70 C",
                              "shock_resistance":"1000 m/s2, 6 ms","vibration_resistance":"100 m/s2, 55-2000 Hz"})
                    rows.append(r)
    return rows

def gen_kih40():
    rows=[]
    for a_c,a_l in KIH40_FLANGES:
        for b_c,bore_mm in KIH40_BORES:
            for c in KIS40_CIRCUITS:
                c_c,c_l,canon,supply_v,power,rev_pol,sigh,sigl,rf,load,freq = c
                for d_c,d_l,_ in KIS40_CONNS:
                    for ppr in PPR_KIS40:
                        r=_r(); ppr_s=str(ppr).zfill(4); pn=f"8.KIH40.{a_c}{b_c}{c_c}{d_c}.{ppr_s}"
                        _fill_base(r,"KIH40","KIS40KIH40_en.pdf",pn,"8.KIH40.{a}{b}{c}{d}.{e}")
                        _fill_elec(r,canon,supply_v,power,rev_pol,sigh,sigl,rf,load,freq)
                        r.update({"param_a_key":a_c,"param_a_value":a_l,
                                  "param_b_key":b_c,"param_b_value":f"o{bore_mm}mm hollow",
                                  "param_c_key":c_c,"param_c_value":c_l,
                                  "param_d_key":d_c,"param_d_value":d_l,
                                  "param_e_key":ppr_s,"param_e_value":str(ppr),
                                  "resolution_ppr":ppr,"max_speed_rpm":"4500 min-1",
                                  "connection_type":d_l,"shaft_diameter_mm":bore_mm,
                                  "weight_kg":"approx. 0.17 kg","startup_torque":"< 0.05 Nm",
                                  "shaft_load_radial_n":"40 N","shaft_load_axial_n":"20 N",
                                  "protection_rating":"IP64","operating_temp_range":"-20 ... +70 C",
                                  "shock_resistance":"1000 m/s2, 6 ms","vibration_resistance":"100 m/s2, 55-2000 Hz"})
                        rows.append(r)
    return rows

def gen_5000(include_us=False):
    rows=[]
    for fa_c,fa_l,fa_ip,fa_us in S5000_FLANGES:
        if fa_us and not include_us: continue
        torq="{'IP65':'<0.01Nm','IP66_IP67':'<0.05Nm'}"
        spd="{'IP65':'12000/6000cont','IP66_IP67':'6000/3000cont'}"
        for sh_c,sh_mm,sh_us,sh_G in S5000_SHAFTS:
            if sh_us and not include_us: continue
            if sh_G and fa_c!="G": continue
            if fa_c=="G" and not sh_G: continue
            for c in S5000_CIRCUITS:
                c_c,c_l,canon,supply_v,power,rev_pol,sigh,sigl,rf,load,freq,c_us,c_no_inv = c
                if c_us and not include_us: continue
                for d_c,d_l,d_pins,d_us,d_no_inv,d_mil in S5000_CONNS:
                    if d_us and not include_us: continue
                    if d_no_inv and not c_no_inv: continue
                    _shock="3000 m/s2, 6 ms" if not d_mil else "2500 m/s2, 6 ms"
                    _vib="300 m/s2, 10-2000 Hz" if not d_mil else "100 m/s2, 10-2000 Hz"
                    _temp="-40 ... +85 C" if d_c not in("1","2","A","B") else "-30 ... +85 C (cable fixed)"
                    for ppr in PPR_5000:
                        r=_r(); ppr_s=str(ppr).zfill(4); pn=f"8.5000.{fa_c}{sh_c}{c_c}{d_c}.{ppr_s}"
                        _fill_base(r,"5000","50005020_en.pdf",pn,"8.5000.{a}{b}{c}{d}.{e}")
                        _fill_elec(r,canon,supply_v,power,rev_pol,sigh,sigl,rf,load,freq)
                        r.update({"param_a_key":fa_c,"param_a_value":f"{fa_l},{fa_ip}",
                                  "param_b_key":sh_c,"param_b_value":f"o{sh_mm}mm",
                                  "param_c_key":c_c,"param_c_value":c_l,
                                  "param_d_key":d_c,"param_d_value":d_l,
                                  "param_e_key":ppr_s,"param_e_value":str(ppr),
                                  "resolution_ppr":ppr,"max_speed_rpm":spd,
                                  "connection_type":d_l,"shaft_diameter_mm":sh_mm,
                                  "weight_kg":"approx. 0.4 kg","startup_torque":torq,
                                  "shaft_load_radial_n":"100 N","shaft_load_axial_n":"50 N",
                                  "moment_of_inertia":"approx. 1.8e-6 kgm2",
                                  "protection_rating":fa_ip,"operating_temp_range":_temp,
                                  "shock_resistance":_shock,"vibration_resistance":_vib})
                        rows.append(r)
    return rows

def gen_5020(include_us=False):
    rows=[]
    for fa_c,fa_l,fa_ip,fa_us in S5020_FLANGES:
        if fa_us and not include_us: continue
        torq="{'IP65':'<0.01Nm','IP66_IP67':'<0.05Nm'}"
        spd="{'IP65':'12000/6000cont','IP66_IP67':'6000/3000cont'}"
        for b_c,bore_mm in S5020_BORES:
            for c in S5000_CIRCUITS:
                c_c,c_l,canon,supply_v,power,rev_pol,sigh,sigl,rf,load,freq,c_us,c_no_inv = c
                if c_us and not include_us: continue
                for d_c,d_l,d_pins,d_us,d_no_inv,d_mil in S5020_CONNS:
                    if d_us and not include_us: continue
                    if d_no_inv and not c_no_inv: continue
                    _shock="3000 m/s2, 6 ms" if not d_mil else "2500 m/s2, 6 ms"
                    _vib="300 m/s2, 10-2000 Hz" if not d_mil else "100 m/s2, 10-2000 Hz"
                    _temp="-40 ... +85 C" if d_c not in("1","A","E","F") else "-30 ... +85 C (cable fixed)"
                    for ppr in PPR_5000:
                        r=_r(); ppr_s=str(ppr).zfill(4); pn=f"8.5020.{fa_c}{b_c}{c_c}{d_c}.{ppr_s}"
                        _fill_base(r,"5020","50005020_en.pdf",pn,"8.5020.{a}{b}{c}{d}.{e}")
                        _fill_elec(r,canon,supply_v,power,rev_pol,sigh,sigl,rf,load,freq)
                        r.update({"param_a_key":fa_c,"param_a_value":f"{fa_l},{fa_ip}",
                                  "param_b_key":b_c,"param_b_value":f"o{bore_mm}mm hollow",
                                  "param_c_key":c_c,"param_c_value":c_l,
                                  "param_d_key":d_c,"param_d_value":d_l,
                                  "param_e_key":ppr_s,"param_e_value":str(ppr),
                                  "resolution_ppr":ppr,"max_speed_rpm":spd,
                                  "connection_type":d_l,"shaft_diameter_mm":bore_mm,
                                  "weight_kg":"approx. 0.4 kg","startup_torque":torq,
                                  "shaft_load_radial_n":"100 N","shaft_load_axial_n":"50 N",
                                  "moment_of_inertia":"approx. 6e-6 kgm2",
                                  "protection_rating":fa_ip,"operating_temp_range":_temp,
                                  "shock_resistance":_shock,"vibration_resistance":_vib})
                        rows.append(r)
    return rows

def _gen_k58i_common(family, pdf, prog_max, circuits, hollow=False):
    """Shared generator for K58I shaft/hollow and K58I-PR shaft/hollow."""
    rows=[]
    is_pr = "PR" in family

    def _do_shaft():
        for c in circuits:
            if is_pr:
                iface,canon,supply_v,power,rev_pol,sigh,sigl,rf,load,freq = c
                supply_c="2"
            else:
                iface,supply_c,supply_label,canon,supply_v,power,rev_pol,sigh,sigl,rf,load,freq = c
            for fl_c,fl_l in K58I_FLANGES:
                for sh_c,sh_mm,sh_ver in K58I_SHAFTS:
                    for ip_c,ip_v in [("65",65.0),("6A",67.0)]:
                        spd="12000/6000cont" if ip_v==65.0 else "6000/3000cont"
                        for h_pos in ["A","R"]:
                            for k_c,k_l,k_pins,k_blk_ax,k_blk_cab,_ in K58I_CONNECTORS:
                                if h_pos=="A" and k_blk_ax: continue
                                cable_types=[("1","PVC cable","-30...+80C(fixed)/-5...+80C(flex)"),
                                             ("C","connector on housing","-40...+85C")]
                                if is_pr:
                                    cable_types=[("1","PVC cable","-30...+80C(fixed)"),
                                                 ("2","TPE cable","-40...+110C(fixed)/-25...+110C(flex)"),
                                                 ("C","connector on housing","-40...+110C")]
                                for i_c,i_l,_temp in cable_types:
                                    if i_c in("1","2") and k_blk_cab: continue
                                    if i_c=="C" and k_c=="1": continue
                                    if i_c=="2" and k_c!="1": continue
                                    pn_base=f"K58I.OPR{iface}" if is_pr else f"K58I.O{iface}"
                                    pn=f"{pn_base}.XXXXX.{supply_c}{sh_ver}{fl_c}{sh_c}.{ip_c}{h_pos}{i_c}{k_c}"
                                    r=_r()
                                    _fill_base(r,family,pdf,pn,f"{pn_base}.XXXXX.c_d_e_f.g_h_i_k.l")
                                    _fill_elec(r,canon,supply_v,power,rev_pol,sigh,sigl,rf,load,freq)
                                    r.update({"param_a_key":iface,"param_a_value":iface,
                                              "param_b_key":"XXXXX","param_b_value":f"1-{prog_max} ppr",
                                              "param_c_key":supply_c,"param_c_value":supply_v,
                                              "param_d_key":sh_ver,"param_d_value":f"shaft {sh_c} o{sh_mm}mm",
                                              "param_e_key":fl_c,"param_e_value":fl_l,
                                              "param_k_key":k_c,"param_k_value":k_l,
                                              "ppr_range_min":1,"ppr_range_max":prog_max,
                                              "max_speed_rpm":spd,"connection_type":f"{i_l},{k_l}",
                                              "shaft_diameter_mm":sh_mm,"weight_kg":"approx. 0.4 kg",
                                              "shaft_load_radial_n":"100 N","shaft_load_axial_n":"50 N",
                                              "protection_rating":f"IP{65 if ip_v==65.0 else '66/67'}",
                                              "operating_temp_range":_temp,
                                              "shock_resistance":"3000 m/s2, 6 ms",
                                              "vibration_resistance":"5-8.7Hz+-0.35mm;8.7-200Hz 30m/s2;200-2000Hz 300m/s2"})
                                    rows.append(r)

    def _do_hollow():
        for c in circuits:
            if is_pr:
                iface,canon,supply_v,power,rev_pol,sigh,sigl,rf,load,freq = c
                supply_c="2"
            else:
                iface,supply_c,supply_label,canon,supply_v,power,rev_pol,sigh,sigl,rf,load,freq = c
            for b_c,bore_mm,is_le15,allowed_vers in K58I_HOLLOW_BORES:
                mounts=K58I_MOUNT_LE15 if is_le15 else K58I_MOUNT_GT15
                _shock="3000 m/s2, 6 ms" if is_le15 else "2000 m/s2, 6 ms"
                for version in allowed_vers:
                    for mount in mounts:
                        for ip_c,ip_v in [("65",65.0),("6A",67.0)]:
                            spd="12000/6000cont" if ip_v==65.0 else "6000/3000cont"
                            h_positions=["R"]
                            if version in("H1","H2") and is_le15: h_positions.append("T")
                            for h_pos in h_positions:
                                for k_c,k_l,k_pins,_,k_blk_cab,_ in K58I_CONNECTORS:
                                    cable_types=[("1","PVC cable","-30...+80C(fixed)"),
                                                 ("C","connector on housing","-40...+85C")]
                                    if is_pr:
                                        cable_types=[("1","PVC cable","-30...+80C(fixed)"),
                                                     ("2","TPE cable","-40...+110C(fixed)"),
                                                     ("C","connector on housing","-40...+110C")]
                                    for i_c,i_l,_temp in cable_types:
                                        if i_c in("1","2") and k_blk_cab: continue
                                        if i_c=="C" and k_c=="1": continue
                                        if h_pos=="T" and i_c!="1": continue
                                        if i_c=="2" and k_c!="1": continue
                                        pn_base=f"K58I.OPR{iface}" if is_pr else f"K58I.O{iface}"
                                        pn=f"{pn_base}.XXXXX.{supply_c}{version}{mount}{b_c}.{ip_c}{h_pos}{i_c}{k_c}"
                                        r=_r()
                                        _fill_base(r,family,pdf,pn,f"{pn_base}.XXXXX.c_d_e_f.g_h_i_k.l")
                                        _fill_elec(r,canon,supply_v,power,rev_pol,sigh,sigl,rf,load,freq)
                                        r.update({"param_a_key":iface,"param_a_value":iface,
                                                  "param_b_key":"XXXXX","param_b_value":f"1-{prog_max} ppr",
                                                  "param_c_key":supply_c,"param_c_value":supply_v,
                                                  "param_d_key":version,"param_d_value":version,
                                                  "param_e_key":mount,"param_e_value":f"mount {mount}",
                                                  "param_k_key":k_c,"param_k_value":k_l,
                                                  "ppr_range_min":1,"ppr_range_max":prog_max,
                                                  "max_speed_rpm":spd,"connection_type":f"{i_l},{k_l}",
                                                  "shaft_diameter_mm":bore_mm,"weight_kg":"approx. 0.4 kg",
                                                  "shaft_load_radial_n":"100 N","shaft_load_axial_n":"50 N",
                                                  "protection_rating":f"IP{65 if ip_v==65.0 else '66/67'}",
                                                  "operating_temp_range":_temp,
                                                  "shock_resistance":_shock,
                                                  "vibration_resistance":"5-8.7Hz+-0.35mm;8.7-200Hz 30m/s2;200-2000Hz 300m/s2"})
                                        rows.append(r)

    if hollow: _do_hollow()
    else: _do_shaft()
    return rows

def gen_k58i_shaft():   return _gen_k58i_common("K58I_shaft","K58I_en.pdf",5000,K58I_CIRCUITS,False)
def gen_k58i_hollow():  return _gen_k58i_common("K58I_hollow","K58I_en.pdf",5000,K58I_CIRCUITS,True)
def gen_k58i_pr_shaft():  return _gen_k58i_common("K58I-PR_shaft","K58IPR_en.pdf",36000,K58IPR_CIRCUITS,False)
def gen_k58i_pr_hollow(): return _gen_k58i_common("K58I-PR_hollow","K58IPR_en.pdf",36000,K58IPR_CIRCUITS,True)

def _gen_k80i_common(family, pdf, circuits, prog_max):
    rows=[]
    is_pr="PR" in family
    for bore_list,version in [(K80I_BORES_H1,"H1"),(K80I_BORES_H2,"H2")]:
        for b_c,bore_mm in bore_list:
            for mount in K80I_MOUNTS:
                for c in circuits:
                    if is_pr:
                        iface,canon,supply_v,power,rev_pol,sigh,sigl,rf,load,freq = c
                        is_sincos=False; supply_c="2"
                    else:
                        iface,supply_c,supply_label,canon,supply_v,power,rev_pol,sigh,sigl,rf,load,freq,is_sincos = c
                    for ip_c,ip_v in [("65",65.0),("6A",67.0)]:
                        spd="5000/2500cont" if ip_v==65.0 else "2400/1200cont"
                        for k_c,k_l,k_pins,k_blk_ax,k_blk_cab,k_blk_sc in K80I_CONNECTORS:
                            if is_sincos and k_blk_sc: continue
                            for i_c,i_l in [("1","PVC cable"),("C","connector on housing")]:
                                if i_c=="1" and k_blk_cab: continue
                                if i_c=="C" and k_c=="1": continue
                                _temp="-40...+85C" if i_c=="C" else "-30...+80C(fixed)"
                                ppr_block=[1024] if is_sincos else [None]
                                for _ppr in ppr_block:
                                    pn_base=f"K80I.OPR{iface}" if is_pr else f"K80I.O{iface}"
                                    ppr_seg=str(_ppr).zfill(5) if _ppr else "XXXXX"
                                    pn=f"{pn_base}.{ppr_seg}.{supply_c}{version}{mount}{b_c}.{ip_c}R{i_c}{k_c}"
                                    r=_r()
                                    _fill_base(r,family,pdf,pn,f"{pn_base}.b.c_d_e_f.g_R_i_k.l")
                                    _fill_elec(r,canon,supply_v,power,rev_pol,sigh,sigl,rf,load,freq)
                                    r.update({"param_a_key":iface,"param_a_value":iface,
                                              "param_b_key":ppr_seg,"param_b_value":str(_ppr) if _ppr else f"1-{prog_max} ppr",
                                              "param_c_key":supply_c,"param_c_value":supply_v,
                                              "param_d_key":version,"param_d_value":version,
                                              "param_e_key":mount,"param_k_key":b_c,"param_k_value":f"o{bore_mm}mm",
                                              "max_speed_rpm":spd,"connection_type":f"{i_l},{k_l}",
                                              "shaft_diameter_mm":bore_mm,"weight_kg":"approx. 0.8 kg",
                                              "startup_torque":"< 0.12 Nm",
                                              "shaft_load_radial_n":"200 N","shaft_load_axial_n":"100 N",
                                              "protection_rating":f"IP{65 if ip_v==65.0 else '66/67'}",
                                              "operating_temp_range":_temp,
                                              "shock_resistance":"2000 m/s2, 6 ms",
                                              "vibration_resistance":"5-8.7Hz+-0.35mm;8.7-200Hz 30m/s2;200-2000Hz 150m/s2"})
                                    if _ppr: r["resolution_ppr"]=_ppr
                                    else: r["ppr_range_min"]=1; r["ppr_range_max"]=prog_max
                                    rows.append(r)
    return rows

def gen_k80i():    return _gen_k80i_common("K80I","K80I_en.pdf",K80I_CIRCUITS,5000)
def gen_k80i_pr(): return _gen_k80i_common("K80I-PR","K80IPR_en.pdf",K80IPR_CIRCUITS,36000)

def gen_a020():
    rows=[]
    for a_c,a_l in A020_FLANGES:
        for b_c,bore_mm in A020_BORES:
            for c in A020_CIRCUITS:
                c_c,c_l,canon,supply_v,power,rev_pol,sigh,sigl,rf,load,freq,is_sincos = c
                for d_c,d_l,_ in A020_CONNS:
                    _temp="-40...+70C" if d_c in("2","E") else "-30...+70C(cable fixed)"
                    for ppr in (PPR_A_SC if is_sincos else PPR_A):
                        r=_r(); ppr_s=str(ppr).zfill(4); pn=f"8.A020.{a_c}{b_c}{c_c}{d_c}.{ppr_s}"
                        _fill_base(r,"A020","A020_en.pdf",pn,"8.A020.{a}{b}{c}{d}.{e}")
                        _fill_elec(r,canon,supply_v,power,rev_pol,sigh,sigl,rf,load,freq)
                        r.update({"param_a_key":a_c,"param_a_value":a_l,
                                  "param_b_key":b_c,"param_b_value":f"o{bore_mm}mm hollow",
                                  "param_c_key":c_c,"param_c_value":c_l,
                                  "param_d_key":d_c,"param_d_value":d_l,
                                  "param_e_key":ppr_s,"param_e_value":str(ppr),
                                  "resolution_ppr":ppr,"max_speed_rpm":"3000 min-1 (3500 short-term)",
                                  "connection_type":d_l,"shaft_diameter_mm":bore_mm,
                                  "weight_kg":"approx. 0.7 kg","startup_torque":"< 0.2 Nm",
                                  "protection_rating":"IP65","operating_temp_range":_temp,
                                  "shock_resistance":"1000 m/s2, 6 ms","vibration_resistance":"100 m/s2, 10-2000 Hz"})
                        rows.append(r)
    return rows

def gen_a02h(include_us=False):
    rows=[]
    for a_c,a_l,a_us in A02H_FLANGES:
        if a_us and not include_us: continue
        for b_c,bore_mm,b_us in A02H_BORES:
            if b_us and not include_us: continue
            for c in A02H_CIRCUITS:
                c_c,c_l,canon,supply_v,power,rev_pol,sigh,sigl,rf,load,freq,is_sincos,c_us,c_no_inv = c
                if c_us and not include_us: continue
                for d_c,d_l,d_pins,d_us,d_no_inv,d_ip in A02H_CONNS:
                    if d_us and not include_us: continue
                    if d_no_inv and is_sincos: continue
                    ip_val=d_ip if d_ip else 65.0
                    _temp="-40...+80C" if d_c in("2","E","G","R","K","D") else "-30...+80C(cable fixed)"
                    for ppr in (PPR_A_SC if is_sincos else PPR_A):
                        r=_r(); ppr_s=str(ppr).zfill(4); pn=f"8.A02H.{a_c}{b_c}{c_c}{d_c}.{ppr_s}"
                        _fill_base(r,"A02H","A02H_en.pdf",pn,"8.A02H.{a}{b}{c}{d}.{e}")
                        _fill_elec(r,canon,supply_v,power,rev_pol,sigh,sigl,rf,load,freq)
                        r.update({"param_a_key":a_c,"param_a_value":a_l,
                                  "param_b_key":b_c,"param_b_value":f"o{bore_mm}mm hollow",
                                  "param_c_key":c_c,"param_c_value":c_l,
                                  "param_d_key":d_c,"param_d_value":d_l,
                                  "param_e_key":ppr_s,"param_e_value":str(ppr),
                                  "resolution_ppr":ppr,"max_speed_rpm":"6000 min-1 (2500 at 60C)",
                                  "connection_type":d_l,"shaft_diameter_mm":bore_mm,
                                  "weight_kg":"approx. 0.8 kg","startup_torque":"< 0.2 Nm",
                                  "shaft_load_radial_n":"200 N","shaft_load_axial_n":"100 N",
                                  "protection_rating":f"IP{int(ip_val)}","operating_temp_range":_temp,
                                  "shock_resistance":"2000 m/s2, 6 ms","vibration_resistance":"100 m/s2, 10-2000 Hz"})
                        rows.append(r)
    return rows

# ── Main ─────────────────────────────────────────────────────────────────────
def main():
    p=argparse.ArgumentParser()
    p.add_argument("--output",default="data/kubler_rules.csv")
    p.add_argument("--include_us",action="store_true")
    p.add_argument("--dry_run",action="store_true")
    args=p.parse_args()

    gens=[("KIS40",gen_kis40,{}),("KIH40",gen_kih40,{}),
          ("5000",gen_5000,{"include_us":args.include_us}),
          ("5020",gen_5020,{"include_us":args.include_us}),
          ("K58I_shaft",gen_k58i_shaft,{}),("K58I_hollow",gen_k58i_hollow,{}),
          ("K58I-PR_shaft",gen_k58i_pr_shaft,{}),("K58I-PR_hollow",gen_k58i_pr_hollow,{}),
          ("K80I",gen_k80i,{}),("K80I-PR",gen_k80i_pr,{}),
          ("A020",gen_a020,{}),("A02H",gen_a02h,{"include_us":args.include_us})]

    all_rows=[]; print("="*55)
    print(f"{'Family':<22}{'Rows':>10}"); print("="*55)
    for fam,fn,kw in gens:
        rows=fn(**kw); all_rows.extend(rows)
        print(f"{fam:<22}{len(rows):>10,}")
    print("="*55); print(f"{'TOTAL':<22}{len(all_rows):>10,}"); print("="*55)

    if args.dry_run:
        print("\n[dry-run] Not writing."); return
    os.makedirs(os.path.dirname(args.output) or ".",exist_ok=True)
    with open(args.output,"w",newline="",encoding="utf-8") as f:
        csv.DictWriter(f,fieldnames=COLUMNS).writeheader()
        csv.DictWriter(f,fieldnames=COLUMNS).writerows(all_rows)
    print(f"\nWritten: {args.output}  ({len(all_rows):,} rows x {len(COLUMNS)} cols)")

if __name__=="__main__":
    main()
