import psycopg2, time

URL = (
    "postgresql://saptak:xjsWn0LLpiIRk0dBGAnn1g"
    "@itchy-gazelle-24547.j77.aws-us-east-1.cockroachlabs.cloud:26257"
    "/defaultdb?sslmode=verify-full"
)
TARGET = 834_228


def query(sql, retries=5):
    for i in range(retries):
        try:
            conn = psycopg2.connect(URL)
            conn.autocommit = True
            cur = conn.cursor()
            cur.execute(sql)
            rows = cur.fetchall()
            cur.close(); conn.close()
            return rows
        except psycopg2.errors.SerializationFailure:
            time.sleep(1.5)
        except Exception as e:
            print(f"  Query error: {e}")
            return []
    return []


total = query("SELECT COUNT(*) FROM encoders")[0][0]
print(f"=== CockroachDB status ===")
print(f"Rows loaded : {total:,} / {TARGET:,}")
print(f"Progress    : {total/TARGET*100:.1f}%")
print(f"Remaining   : {TARGET - total:,}")
print()

print("By manufacturer:")
rows = query(
    "SELECT manufacturer, COUNT(*) FROM encoders "
    "GROUP BY manufacturer ORDER BY COUNT(*) DESC"
)
for r in rows:
    mfr = str(r[0]) if r[0] else "(null)"
    print(f"  {mfr:<25s}: {r[1]:>8,}")

print()
print("By family (top families per manufacturer):")
rows = query(
    "SELECT manufacturer, product_family, COUNT(*) "
    "FROM encoders GROUP BY manufacturer, product_family "
    "ORDER BY manufacturer, COUNT(*) DESC"
)
cur_mfr = None
for r in rows:
    mfr = str(r[0]) if r[0] else "(null)"
    fam = str(r[1]) if r[1] else "(null)"
    if mfr != cur_mfr:
        cur_mfr = mfr
        print(f"  [{mfr}]")
    print(f"    {fam:<35s}: {r[2]:>8,}")
