import sys
sys.path.insert(0, "reports/data/phase-13-register-index-volumes/step-19d")
import entry_eval as ee
split, rid = sys.argv[1], sys.argv[2]
sides, gold = ee.load_sides(split)
side = [s for s in sides if s["read_id"] == rid][0]
cache = ee.Cache()
regions_by, segs_by = cache.get(ee.CLONE[side["layer"]], side["inv"])
print(side)
for ri, r in enumerate(regions_by[(side["scan_id"], side["page"])]):
    segs = segs_by.get((side["scan_id"], side["page"], r["layout_region_id"]), [])
    print(f"-- region {ri} col {r['layout_column']} role {r['layout_role']} n {len(segs)}")
    for s in segs:
        lab = gold[rid].get(s["line_id"], "")
        print(f"   {s['layout_line_idx']:3d} {lab:1s} x{s['x']:5d} y{s['y']:5d} w{s['w']:5d} h{s['h']:3d} {s['line_type'][:9]:9s} {s['text'][:50]}")
