"""
SAP Sample Data Generator for Databricks + SAP Course
Generates realistic SAP tables used throughout all modules.

Tables generated:
  BKPF  - Accounting Document Headers     (FI)
  BSEG  - Accounting Document Line Items  (FI)
  KNA1  - Customer Master Data            (SD)
  MARA  - Material Master Data            (MM)
  VBAK  - Sales Order Headers             (SD)
  VBAP  - Sales Order Line Items          (SD)
  LFA1  - Vendor Master Data              (MM)
  EKKO  - Purchase Order Headers          (MM)
"""
import csv
import random
from datetime import datetime, timedelta
import os

random.seed(42)
OUT = os.path.dirname(os.path.abspath(__file__))

def rand_date(y0=2020, y1=2024):
    s = datetime(y0, 1, 1)
    e = datetime(y1, 12, 31)
    return (s + timedelta(days=random.randint(0, (e - s).days))).strftime("%Y%m%d")

def rand_amt(lo=100, hi=500000):
    return round(random.uniform(lo, hi), 2)

def write_csv(name, rows):
    path = f"{OUT}/{name}.csv"
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=rows[0].keys())
        w.writeheader()
        w.writerows(rows)
    return len(rows)

BUKRS = ["1000","2000","3000"]
WAERK = ["COP","USD","EUR"]
BLART = ["RE","KR","SA","AB","ZP"]
LAND1 = ["CO","US","MX","BR","DE"]
MTART = ["FERT","ROH","HALB","HAWA","DIEN"]
MEINS = ["UN","KG","L","M","ST"]
MATKL = ["001","002","003","004","005"]
VKORG = ["1000","2000"]
VTWEG = ["10","20","30"]
KOART = ["K","D","S","A","M"]
BSART = ["NB","ZNB","UB"]

# BKPF
bkpf = []
for i in range(1, 5001):
    bkpf.append({
        "BUKRS": random.choice(BUKRS),
        "BELNR": str(i).zfill(10),
        "GJAHR": str(random.randint(2020, 2024)),
        "BLDAT": rand_date(), "BUDAT": rand_date(),
        "BLART": random.choice(BLART),
        "WAERK": random.choice(WAERK),
        "USNAM": f"USER{random.randint(1,50):03d}",
        "XBLNR": f"REF{random.randint(10000,99999)}",
        "BKTXT": random.choice(["Invoice payment","Expense report","Salary posting",
                                 "Asset acquisition","Vendor payment","Customer receipt"])
    })

# BSEG
bseg = []
for row in bkpf[:3000]:
    for buzei in range(1, random.randint(2, 5)):
        amt = rand_amt(50, 200000)
        bseg.append({
            "BUKRS": row["BUKRS"], "BELNR": row["BELNR"], "GJAHR": row["GJAHR"],
            "BUZEI": str(buzei).zfill(3),
            "KOART": random.choice(KOART),
            "KONTO": str(random.randint(100000, 999999)),
            "DMBTR": amt,
            "WRBTR": round(amt * random.uniform(0.9, 1.1), 2),
            "WAERS": row["WAERK"],
            "SGTXT": random.choice(["Line item desc","Cost allocation","Revenue rec",
                                     "Tax posting","Clearing entry"])
        })

# KNA1
kna1 = []
cities = ["Bogota","Medellin","Cali","Barranquilla","New York","Berlin","Miami","Sao Paulo"]
for i in range(1, 501):
    kna1.append({
        "KUNNR": str(i).zfill(10),
        "NAME1": f"Customer {i} S.A.S",
        "LAND1": random.choice(LAND1),
        "ORT01": random.choice(cities),
        "PSTLZ": str(random.randint(10000, 99999)),
        "STRAS": f"Calle {random.randint(1,100)} #{random.randint(1,99)}-{random.randint(1,99)}",
        "BRSCH": random.choice(["A","B","C","D","E"]),
        "KTOKD": random.choice(["0001","0002","KUNA"])
    })

# MARA
mara = []
for i in range(1, 301):
    mara.append({
        "MATNR": f"MAT{str(i).zfill(8)}",
        "MTART": random.choice(MTART),
        "MBRSH": random.choice(["A","B","C","D","M"]),
        "MATKL": random.choice(MATKL),
        "MEINS": random.choice(MEINS),
        "ERSDA": rand_date(2015, 2022),
        "LAEDA": rand_date(2022, 2024),
        "BRGEW": round(random.uniform(0.1, 500), 2),
        "NTGEW": round(random.uniform(0.1, 490), 2),
        "MAKTX": f"Material description {i}"
    })

# VBAK
vbak = []
for i in range(1, 2001):
    vbak.append({
        "VBELN": str(i).zfill(10),
        "ERDAT": rand_date(2020, 2024),
        "KUNNR": random.choice(kna1)["KUNNR"],
        "VKORG": random.choice(VKORG),
        "VTWEG": random.choice(VTWEG),
        "SPART": str(random.randint(1,5)).zfill(2),
        "NETWR": rand_amt(500, 1000000),
        "WAERK": random.choice(WAERK),
        "AUART": random.choice(["ZOR","OR","RE","CR"])
    })

# VBAP
vbap = []
for v in vbak[:1500]:
    for pos in range(1, random.randint(2, 6)):
        mat = random.choice(mara)
        qty = round(random.uniform(1, 1000), 3)
        price = round(random.uniform(10, 5000), 2)
        vbap.append({
            "VBELN": v["VBELN"],
            "POSNR": str(pos * 10).zfill(6),
            "MATNR": mat["MATNR"],
            "KWMENG": qty,
            "NETWR": round(qty * price, 2),
            "WAERK": v["WAERK"],
            "MEINS": mat["MEINS"],
            "WERKS": f"{random.randint(1,4):04d}",
            "LGORT": f"{random.randint(1,9):04d}"
        })

# LFA1
lfa1 = []
vcities = ["Bogota","Medellin","Sao Paulo","Mexico City","Miami","New York"]
for i in range(1, 201):
    lfa1.append({
        "LIFNR": str(i).zfill(10),
        "NAME1": f"Vendor {i} Ltda",
        "LAND1": random.choice(LAND1),
        "ORT01": random.choice(vcities),
        "PSTLZ": str(random.randint(10000,99999)),
        "STRAS": f"Avenida {random.randint(1,100)} No {random.randint(1,99)}",
        "KTOKK": random.choice(["0001","0002","LIEF"]),
        "BRSCH": random.choice(["A","B","C","D"])
    })

# EKKO
ekko = []
for i in range(1, 1001):
    ekko.append({
        "EBELN": str(i).zfill(10),
        "BUKRS": random.choice(BUKRS),
        "BSTYP": random.choice(["F","K"]),
        "BSART": random.choice(BSART),
        "LIFNR": random.choice(lfa1)["LIFNR"],
        "BEDAT": rand_date(2020, 2024),
        "NETWR": rand_amt(1000, 500000),
        "WAERS": random.choice(WAERK),
        "EKORG": random.choice(["1000","2000"]),
        "EKGRP": str(random.randint(1,9)).zfill(3)
    })

# Write all
tables = {"BKPF": bkpf,"BSEG": bseg,"KNA1": kna1,"MARA": mara,
          "VBAK": vbak,"VBAP": vbap,"LFA1": lfa1,"EKKO": ekko}

print("Generating SAP sample datasets...")
for name, rows in tables.items():
    n = write_csv(name, rows)
    print(f"  {name:<6}: {n:>6,} rows  ->  {name}.csv")
print("\nDone! All datasets ready for upload to Databricks DBFS.")
