"""Check the 13 documented SG re-shoots (LotA phase-02 PROGRESS, 'Documented interventions')
in a duplicate-candidate TSV. Usage: python reshoots.py <1.01.02-duplicate-candidates.tsv>
Planning result (stored LotA table, 2026-10-21): 13/13 found, 23 rows, all adjacent, all
same-side pairs (page-1/page-1 or page-2/page-2 across scans N and N+1)."""
import sys
import pandas as pd

RESHOOTS = [('3777', 246), ('3779', 65), ('3780', 138), ('3781', 352), ('3788', 246), ('3860', 91),
            ('3767', 65), ('3773', 276), ('3803', 187), ('3816', 386), ('3822', 390), ('3826', 200),
            ('3838', 235)]
ref = pd.read_csv(sys.argv[1], sep='\t', dtype={'inventory_number': str})
found = rows = 0
for inv, a in RESHOOTS:
    r = ref[(ref.inventory_number == inv)
            & (((ref.scan_number_a == a) & (ref.scan_number_b == a + 1))
               | ((ref.scan_number_a == a + 1) & (ref.scan_number_b == a)))]
    found += bool(len(r)) and bool(r.adjacent.all())
    rows += len(r)
    print(inv, a, len(r), bool(r.adjacent.all()), r.jaccard_exact.round(3).tolist())
print(f'found {found}/13, rows {rows}, table rows {len(ref)}, adjacent {int(ref.adjacent.sum())}')
