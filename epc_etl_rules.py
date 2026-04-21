#!/usr/bin/env python3
"""
epc_etl_rules.py  v2
=====================
Rule-compliant EPC encoder ETL — 6 families, ~220k rows.
Schema matches kubler_final_csv.gz (50 cols).

Scope decisions to keep rows manageable for cross-reference matching:
- Channels: Q (quadrature) and R (quad+index) only — A/K/D excluded as non-standard
- Outputs: OC, PP, HV only — PU/H5/P5 excluded as variants
- Mounts: representative groups, not every variant
- Freq code: derived from CPR, not independently iterated
- Special cable lengths: 1 row (not per-length)
- CE cert: not iterated (match does not depend on CE marking)
"""

import argparse, csv, os

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
def _fill_base(r,fam,pdf,pn,pat):
    r.update({"manufacturer":"EPC","product_family":fam,"family":fam,
              "part_number":pn,"product_code":pn,"source_pdf":pdf,
              "encoder_type":"incremental","order_pattern":pat,
              "shaft_material":"stainless steel"})
def _fill_elec(r,canon,sv,pwr,rp,sigh,sigl,rf,load,freq):
    r.update({"output_circuits":canon,"interface_canonical":canon,
              "supply_voltage":sv,"power_consumption":pwr,
              "reverse_polarity_protection":rp,"short_circuit_protection":"yes",
              "signal_level_high_v":sigh,"signal_level_low_v":sigl,
              "rise_fall_time":rf,"permissible_load_per_channel":load,
              "max_output_frequency_hz":freq})

# ── CPR lists (from datasheets, ≤5000 for Kübler comparability) ──────────────
CPR_STD = [100,200,250,300,360,400,500,512,600,625,720,
           1000,1024,1200,1250,1270,1440,1500,1800,
           2000,2048,2400,2500,3000,3600,4000,4096,5000]
CPR_776 = [60,100,120,240,250,256,500,512,1000,1024,2048,2500,4096]
CPR_260 = [200,250,256,300,360,400,500,512,600,720,1000,1024,
           1200,1250,1500,1800,2000,2048,2500,3000,4000,4096,5000]

# Max freq derived from CPR
def _freq(cpr):
    if cpr > 10000: return "1 MHz"
    if cpr > 6000:  return "500 kHz"
    if cpr > 3000:  return "250 kHz"
    if cpr >= 200:  return "200 kHz"
    return "100 kHz"

# ── Shared output specs (OC, PP, HV) ─────────────────────────────────────────
# (code, label, canon, supply_v, rev_pol, sigh, sigl, rf, load, is_ld)
EPC_OUTS = [
    ("OC","Open Collector","Open Collector","4.75 ... 28 V DC","yes",
     "","< 0.4 V","< 1 us","100 mA sink",False),
    ("PP","Push-Pull","Push-Pull","4.75 ... 28 V DC","yes",
     "~+V","< 0.4 V","< 1 us","max +/- 20 mA",False),
    ("HV","Line Driver (RS422)","TTL RS422","4.75 ... 28 V DC in / 5 V out","yes",
     "min. 2.5 V","max. 0.5 V","< 1 us","max +/- 20 mA",True),
]
EPC_260_OUTS = [  # 260 OC is 20mA not 100mA
    ("OC","Open Collector","Open Collector","4.75 ... 28 V DC","yes",
     "","< 0.4 V","< 1 us","max. 20 mA sink",False),
    ("PP","Push-Pull","Push-Pull","4.75 ... 28 V DC","yes",
     "~+V","< 0.4 V","< 1 us","max +/- 20 mA",False),
    ("HV","Line Driver (RS422)","TTL RS422","4.75 ... 28 V DC in / 5 V out","yes",
     "min. 2.5 V","max. 0.5 V","< 1 us","max +/- 20 mA",True),
]
# Channels: only Q and R
CHANNELS = [("Q","Quadrature A & B"),("R","Quadrature A & B with Index")]

# ═══════════════════════════════════════════════════════════════════════════════
# 858S
# ═══════════════════════════════════════════════════════════════════════════════
def gen_858s():
    rows=[]
    mounts=[("A","20-type clamping flange"),("B","26-type synchro flange")]
    temps=[("S","0 ... +70 C"),("L","-40 ... +70 C"),("H","0 ... +85/100 C")]
    shafts=[("07",6.35,"1/4\""),("20",9.525,"3/8\""),("06",6.0,"6mm"),("21",10.0,"10mm")]
    seals=[("N","IP50"),("1","IP66"),("2","IP64"),("5","IP67")]
    # connectors: G=cable end, G=cable side, J=M12-5pin side, K=M12-8pin side
    conns=[("E","G","cable, 24in end-mount",None),("S","G","cable, 24in side-mount",None),
           ("S","J","M12 5-pin side",5),("S","K","M12 8-pin side",8)]
    for mt,ml in mounts:
        for tp,tl in temps:
            for sh,sh_mm,shl in shafts:
                for ch,chl in CHANNELS:
                    for oc,ol,canon,sv,rp,sigh,sigl,rf,load,is_ld in EPC_OUTS:
                        for sl,sll in seals:
                            for loc,cc,cl,cp in conns:
                                if is_ld and cc=="J": continue  # HV not with 5-pin M12
                                if cc in("J","K") and loc!="S": continue
                                for cpr in CPR_STD:
                                    if tp=="L" and cpr>=3000: continue
                                    r=_r(); pn=f"858S-{mt}{tp}{str(cpr).zfill(5)}{sh}{ch}{oc}{sl}{loc}{cc}"
                                    _fill_base(r,"858S","Model_858S.pdf",pn,"858S-{MOUNT}{TEMP}{CPR}{SHAFT}{CH}{OUT}{SEAL}{LOC}{CONN}")
                                    _fill_elec(r,canon,sv,"100 mA max",rp,sigh,sigl,rf,load,_freq(cpr))
                                    r.update({"param_a_key":mt,"param_a_value":ml,
                                              "param_b_key":sh,"param_b_value":f"{shl} shaft",
                                              "param_c_key":oc,"param_c_value":ol,
                                              "param_d_key":cc,"param_d_value":cl,
                                              "param_e_key":str(cpr),"param_e_value":str(cpr),
                                              "param_k_key":sl,"param_k_value":sll,
                                              "param_l_key":tp,"param_l_value":tl,
                                              "resolution_ppr":cpr,"max_speed_rpm":"8000 min-1",
                                              "connection_type":cl,"shaft_diameter_mm":sh_mm,
                                              "weight_kg":"approx. 0.68 kg",
                                              "startup_torque":"{'IP50':'0.07 Nm','IP66':'0.21 Nm','IP67':'0.49 Nm'}",
                                              "shaft_load_radial_n":"355 N","shaft_load_axial_n":"355 N",
                                              "protection_rating":sll,"operating_temp_range":tl,
                                              "shock_resistance":"75g, 11 ms","vibration_resistance":"20g, 58-500 Hz"})
                                    rows.append(r)
    return rows

# ═══════════════════════════════════════════════════════════════════════════════
# 802S  (same electrical specs as 858S, different housing and mount options)
# ═══════════════════════════════════════════════════════════════════════════════
def gen_802s():
    rows=[]
    # Representative mounts only: 3 groups
    mount_groups=[
        ("F","flange 1.181\" female pilot",False),
        ("S","servo 1.181\" female pilot",False),
        ("J","servo Size25 w/30-shaft adapter",True),   # requires shaft 30
    ]
    temps=[("S","0 ... +70 C"),("L","-40 ... +70 C"),("H","0 ... +85/100 C")]
    shafts=[("07",6.35,"1/4\""),("20",9.525,"3/8\""),("21",10.0,"10mm"),("30",9.525,"3/8\" Size25")]
    seals=[("N","IP50"),("1","IP66"),("2","IP64"),("5","IP67")]
    conns=[("E","G","cable, 24in end-mount",None),("S","G","cable, 24in side-mount",None),
           ("S","J","M12 5-pin side",5),("S","K","M12 8-pin side",8)]
    for mt,ml,needs30 in mount_groups:
        for tp,tl in temps:
            for sh,sh_mm,shl in shafts:
                if needs30 and sh!="30": continue
                if not needs30 and sh=="30": continue
                for ch,chl in CHANNELS:
                    for oc,ol,canon,sv,rp,sigh,sigl,rf,load,is_ld in EPC_OUTS:
                        for sl,sll in seals:
                            for loc,cc,cl,cp in conns:
                                if is_ld and cc=="J": continue
                                if cc in("J","K") and loc!="S": continue
                                for cpr in CPR_STD:
                                    if tp=="L" and cpr>=3000: continue
                                    r=_r(); pn=f"802S-{tp}{sh}{str(cpr).zfill(5)}{ch}{oc}{mt}{sl}{loc}{cc}"
                                    _fill_base(r,"802S","Model_802S.pdf",pn,"802S-{TEMP}{SHAFT}{CPR}{CH}{OUT}{MOUNT}{SEAL}{LOC}{CONN}")
                                    _fill_elec(r,canon,sv,"100 mA max",rp,sigh,sigl,rf,load,_freq(cpr))
                                    r.update({"param_a_key":mt,"param_a_value":ml,
                                              "param_b_key":sh,"param_b_value":f"{shl} shaft",
                                              "param_c_key":oc,"param_c_value":ol,
                                              "param_d_key":cc,"param_d_value":cl,
                                              "param_e_key":str(cpr),"param_e_value":str(cpr),
                                              "param_k_key":sl,"param_k_value":sll,
                                              "param_l_key":tp,"param_l_value":tl,
                                              "resolution_ppr":cpr,"max_speed_rpm":"8000 min-1",
                                              "connection_type":cl,"shaft_diameter_mm":sh_mm,
                                              "weight_kg":"approx. 0.68 kg",
                                              "startup_torque":"{'IP50':'0.07 Nm','IP66':'0.21 Nm','IP67':'0.49 Nm'}",
                                              "shaft_load_radial_n":"355 N","shaft_load_axial_n":"355 N",
                                              "protection_rating":sll,"operating_temp_range":tl,
                                              "shock_resistance":"75g, 11 ms","vibration_resistance":"20g, 58-500 Hz"})
                                    rows.append(r)
    return rows

# ═══════════════════════════════════════════════════════════════════════════════
# 755A shaft
# ═══════════════════════════════════════════════════════════════════════════════
def gen_755a_shaft():
    rows=[]
    shafts=[("07",6.35,"1/4\""),("08",5.0,"5mm"),("06",6.0,"6mm"),
            ("32",6.35,"1/4\" extended (S1/S2/S3 only)"),("20",6.0,"6mm x 0.5\"")]
    mounts=[("S","standard servo",False),("MF","square flange 1.575\"",False),
            ("S1","servo 0.547\" boss (shaft 32)",True)]
    temps=[("S","0 ... +70 C"),("L","-40 ... +70 C"),("H","0 ... +85/100 C")]
    conns=[("S","18in cable",None),("C01","8-pin Molex",8),
           ("J00","18in cable + 5-pin M12",5),("K00","18in cable + 8-pin M12",8)]
    for sh,sh_mm,shl in shafts:
        for mt,ml,needs32 in mounts:
            if needs32 and sh!="32": continue
            if not needs32 and sh=="32": continue
            for tp,tl in temps:
                for ch,chl in CHANNELS:
                    for oc,ol,canon,sv,rp,sigh,sigl,rf,load,is_ld in EPC_OUTS:
                        for cc,cl,cp in conns:
                            if is_ld and cc=="J00": continue
                            for cpr in CPR_STD:
                                if tp=="L" and cpr>=3000: continue
                                r=_r(); pn=f"755AS-{sh}{tp}{str(cpr).zfill(5)}{ch}{oc}{mt}{cc}"
                                _fill_base(r,"755A_shaft","Model_755A.pdf",pn,"755A-{SHAFT}{TEMP}{CPR}{CH}{OUT}{MOUNT}{CONN}")
                                _fill_elec(r,canon,sv,"100 mA max",rp,sigh,sigl,rf,load,_freq(cpr))
                                r.update({"param_a_key":mt,"param_a_value":ml,
                                          "param_b_key":sh,"param_b_value":f"{shl} shaft",
                                          "param_c_key":oc,"param_c_value":ol,
                                          "param_d_key":cc,"param_d_value":cl,
                                          "param_e_key":str(cpr),"param_e_value":str(cpr),
                                          "param_k_key":tp,"param_k_value":tl,
                                          "resolution_ppr":cpr,"max_speed_rpm":"7500 min-1",
                                          "connection_type":cl,"shaft_diameter_mm":sh_mm,
                                          "weight_kg":"approx. 0.088 kg","startup_torque":"0.01 Nm typ",
                                          "shaft_load_radial_n":"22 N","shaft_load_axial_n":"13 N",
                                          "protection_rating":"IP50","operating_temp_range":tl,
                                          "shock_resistance":"50g, 11 ms","vibration_resistance":"10g, 58-500 Hz"})
                                rows.append(r)
    return rows

# ═══════════════════════════════════════════════════════════════════════════════
# 755A hollow bore
# ═══════════════════════════════════════════════════════════════════════════════
def gen_755a_hollow():
    rows=[]
    bores=[("15",4.76,"3/16\""),("16",4.0,"4mm"),("01",6.35,"1/4\""),
           ("18",5.0,"5mm"),("04",6.0,"6mm"),("02",9.525,"3/8\""),
           ("14",8.0,"8mm"),("10",12.7,"1/2\""),("05",10.0,"10mm"),
           ("11",15.875,"5/8\""),("12",12.0,"12mm"),("17",19.05,"3/4\""),("13",14.0,"14mm")]
    mounts=[("S","standard flex mount"),("SF","slotted flex mount")]
    temps=[("S","0 ... +70 C"),("L","-40 ... +70 C"),("H","0 ... +85/100 C")]
    conns=[("S","18in cable",None),("C01","8-pin Molex",8),
           ("J00","18in cable + 5-pin M12",5),("K00","18in cable + 8-pin M12",8)]
    for br,bore_mm,brl in bores:
        for mt,ml in mounts:
            for tp,tl in temps:
                for ch,chl in CHANNELS:
                    for oc,ol,canon,sv,rp,sigh,sigl,rf,load,is_ld in EPC_OUTS:
                        for cc,cl,cp in conns:
                            if is_ld and cc=="J00": continue
                            for cpr in CPR_STD:
                                if tp=="L" and cpr>=3000: continue
                                r=_r(); pn=f"755AH-{br}{tp}{str(cpr).zfill(5)}{ch}{oc}{mt[0]}{cc}"
                                _fill_base(r,"755A_hollow","Model_755A_Hollow_Bore.pdf",pn,"755A-{BORE}{TEMP}{CPR}{CH}{OUT}{MOUNT}{CONN}")
                                _fill_elec(r,canon,sv,"100 mA max",rp,sigh,sigl,rf,load,_freq(cpr))
                                r.update({"param_a_key":mt,"param_a_value":ml,
                                          "param_b_key":br,"param_b_value":f"{brl} bore ({bore_mm}mm)",
                                          "param_c_key":oc,"param_c_value":ol,
                                          "param_d_key":cc,"param_d_value":cl,
                                          "param_e_key":str(cpr),"param_e_value":str(cpr),
                                          "param_k_key":tp,"param_k_value":tl,
                                          "resolution_ppr":cpr,"max_speed_rpm":"7500 min-1",
                                          "connection_type":cl,"shaft_diameter_mm":bore_mm,
                                          "weight_kg":"approx. 0.099 kg","startup_torque":"0.01 Nm typ",
                                          "protection_rating":"IP50","operating_temp_range":tl,
                                          "shock_resistance":"50g, 11 ms","vibration_resistance":"10g, 58-500 Hz"})
                                rows.append(r)
    return rows

# ═══════════════════════════════════════════════════════════════════════════════
# 776 large thru-bore
# ═══════════════════════════════════════════════════════════════════════════════
def gen_776():
    rows=[]
    housings=[("A","enclosed housing"),("B","thru-bore housing")]
    temps=[("S","0 ... +70 C"),("H","0 ... +100 C")]
    bores=[("G",36.5,"1-7/16\""),("C",38.1,"1-1/2\""),("D",41.3,"1-5/8\""),
           ("F",44.5,"1-3/4\""),("E",47.6,"1-7/8\""),
           ("L",35.0,"35mm"),("I",38.0,"38mm"),("J",40.0,"40mm"),("M",42.0,"42mm"),("N",43.0,"43mm")]
    outs=[("OC","Open Collector","Open Collector","4.75 ... 28 V DC","yes","","< 0.4 V","< 1 us","100 mA sink",False),
          ("PP","Push-Pull","Push-Pull","4.75 ... 28 V DC","yes","~+V","< 0.4 V","< 1 us","max +/- 20 mA",False),
          ("HV","Line Driver (RS422)","TTL RS422","4.75 ... 28 V DC in / 5 V out","yes","min. 2.5 V","max. 0.5 V","< 1 us","max +/- 20 mA",True)]
    # connectors: P=cable, 9D=D-sub, Y=7-pin MS (HV compatible), K=M12-8pin
    conns=[("P","cable, 24in gland",None,False),("9D","9-pin D-sub",9,False),
           ("Y","7-pin MS3",7,False),("K","8-pin M12",8,False)]
    for hs,hl in housings:
        for tp,tl in temps:
            for br,bore_mm,brl in bores:
                for ch,chl in CHANNELS:
                    for oc,ol,canon,sv,rp,sigh,sigl,rf,load,is_ld in outs:
                        for cc,cl,cp,_ in conns:
                            # HV only with 7-pin MS (Y); not with 5-pin M12 (J already excluded), not with P/9D
                            if is_ld and cc not in("Y",): continue
                            for cpr in CPR_776:
                                r=_r(); pn=f"776-{hs}{tp}{str(cpr).zfill(4)}{ch}{oc}{br}{cc}"
                                _fill_base(r,"776","Model_776.pdf",pn,"776-{HOUSING}{TEMP}{CPR}{CH}{OUT}{BORE}{CONN}")
                                _fill_elec(r,canon,sv,"100 mA max",rp,sigh,sigl,rf,"200 kHz",load)
                                r.update({"param_a_key":hs,"param_a_value":hl,
                                          "param_b_key":br,"param_b_value":f"{brl} bore ({bore_mm}mm)",
                                          "param_c_key":oc,"param_c_value":ol,
                                          "param_d_key":cc,"param_d_value":cl,
                                          "param_e_key":str(cpr),"param_e_value":str(cpr),
                                          "param_k_key":tp,"param_k_value":tl,
                                          "resolution_ppr":cpr,"max_speed_rpm":"3500 min-1",
                                          "connection_type":cl,"shaft_diameter_mm":bore_mm,
                                          "weight_kg":"0.45 ... 0.68 kg","moment_of_inertia":"3.3e-3 oz-in-sec2",
                                          "protection_rating":"IP50","operating_temp_range":tl,
                                          "shock_resistance":"50g, 11 ms","vibration_resistance":"10g, 58-500 Hz"})
                                rows.append(r)
    return rows

# ═══════════════════════════════════════════════════════════════════════════════
# 260 hollow bore + thru-bore (with commutation option)
# ═══════════════════════════════════════════════════════════════════════════════
def gen_260():
    rows=[]
    commutations=[("N","no commutation"),("C4","4-pole"),("C6","6-pole"),("C8","8-pole")]
    housings=[("B","blind hollow bore"),("T","front-clamp thru-bore"),("R","rear-clamp thru-bore")]
    temps=[("L","-40 ... +70 C","4.75 ... 28 V DC"),
           ("S","0 ... +70 C","4.75 ... 28 V DC"),
           ("H","0 ... +100 C","5 ... 16 V DC")]
    bores=[("01",6.35,"1/4\""),("02",9.525,"3/8\""),("10",12.7,"1/2\""),
           ("11",15.875,"5/8\""),("06",5.0,"5mm"),("04",6.0,"6mm"),
           ("14",8.0,"8mm"),("05",10.0,"10mm"),("12",12.0,"12mm"),("15",15.0,"15mm")]
    seals_thru=[("1","IP50 thru-bore"),("2","IP64 thru-bore")]
    seals_hollow=[("3","IP64 hollow bore"),("4","IP50 hollow bore")]
    mounts=[("SD","1.575\" BC flex mount"),("SF","1.811\" BC slotted flex")]
    conns=[("S","18in cable",True),("K00","18in cable + 8-pin M12",True),("SMK","8-pin body-mount M12",False)]
    for cm,cml in commutations:
        for hs,hl in housings:
            seals = seals_hollow if hs=="B" else seals_thru
            for tp,tl,tv in temps:
                if tp=="H" and cm!="N": continue   # H temp not standard with commutation
                for br,bore_mm,brl in bores:
                    for ch,chl in CHANNELS:
                        for oc,ol,canon,sv,rp,sigh,sigl,rf,load,is_ld in EPC_260_OUTS:
                            for sl,sll in seals:
                                for mt,ml in mounts:
                                    for cc,cl,is_cable in conns:
                                        if cm!="N" and not is_cable: continue  # commutation: cable only
                                        if cm!="N" and cc=="K00": continue     # M12 not with commutation
                                        if is_ld and cc=="SMK": continue       # HV not with 5-pin
                                        for cpr in CPR_260:
                                            r=_r(); pn=f"260-{cm[0]}{hs}{tp}{br}{ch}{str(cpr).zfill(5)}{oc}{sl}{mt[0]}{cc[0]}"
                                            _fill_base(r,"260","Model_260.pdf",pn,"260-{COMM}{HOUSING}{TEMP}{BORE}{CH}{CPR}{OUT}{SEAL}{MOUNT}{CONN}")
                                            _fill_elec(r,canon,tv,"< 100 mA typical",rp,sigh,sigl,rf,load,_freq(cpr))
                                            r.update({"param_a_key":cm,"param_a_value":cml,
                                                      "param_b_key":hs,"param_b_value":hl,
                                                      "param_c_key":oc,"param_c_value":ol,
                                                      "param_d_key":cc,"param_d_value":cl,
                                                      "param_e_key":str(cpr),"param_e_value":str(cpr),
                                                      "param_k_key":br,"param_k_value":f"{brl} ({bore_mm}mm)",
                                                      "param_l_key":tp,"param_l_value":tl,
                                                      "resolution_ppr":cpr,"max_speed_rpm":"7500 min-1",
                                                      "connection_type":cl,"shaft_diameter_mm":bore_mm,
                                                      "weight_kg":"approx. 0.099 kg","moment_of_inertia":"3.9e-4 oz-in-sec2",
                                                      "startup_torque":"{'IP50_thru':'0.035 Nm','IP64_thru':'0.18 Nm','IP50_hollow':'0.021 Nm','IP64_hollow':'0.14 Nm'}",
                                                      "protection_rating":sll,"operating_temp_range":tl,
                                                      "shock_resistance":"50g, 11 ms","vibration_resistance":"10g, 58-500 Hz"})
                                            rows.append(r)
    return rows

def main():
    p=argparse.ArgumentParser()
    p.add_argument("--output",default="data/epc_rules.csv")
    p.add_argument("--dry_run",action="store_true")
    args=p.parse_args()

    gens=[("858S",gen_858s),("802S",gen_802s),("755A_shaft",gen_755a_shaft),
          ("755A_hollow",gen_755a_hollow),("776",gen_776),("260",gen_260)]

    all_rows=[]; print("="*50)
    print(f"{'Family':<16}{'Rows':>10}"); print("="*50)
    for fam,fn in gens:
        rows=fn(); all_rows.extend(rows)
        print(f"{fam:<16}{len(rows):>10,}")
    print("="*50); print(f"{'TOTAL':<16}{len(all_rows):>10,}")
    print("="*50)
    if args.dry_run: print("\n[dry-run] Not writing."); return
    os.makedirs(os.path.dirname(args.output) or ".",exist_ok=True)
    with open(args.output,"w",newline="",encoding="utf-8") as f:
        w=csv.DictWriter(f,fieldnames=COLUMNS); w.writeheader(); w.writerows(all_rows)
    print(f"\nWritten: {args.output} ({len(all_rows):,} rows x {len(COLUMNS)} cols)")

if __name__=="__main__":
    main()
