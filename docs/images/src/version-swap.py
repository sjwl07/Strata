# Generates docs/images/version-swap.svg. Run: python3 docs/images/src/version-swap.py
FONT = "-apple-system, BlinkMacSystemFont, 'Segoe UI', Helvetica, Arial, sans-serif"
out = []
add = out.append
def text(x, y, s, size=13, weight="normal", fill="#0f172a", anchor="middle", halo=False):
    h = ' stroke="#ffffff" stroke-width="4" paint-order="stroke"' if halo else ""
    add(f'<text x="{x}" y="{y}" font-size="{size}" font-weight="{weight}" fill="{fill}" text-anchor="{anchor}"{h}>{s}</text>')
def bar(x1, x2, y, fill, stroke, label, tcolor="#0f172a", dashed=False, weight="600"):
    d = ' stroke-dasharray="5 4"' if dashed else ""
    add(f'<rect x="{x1}" y="{y}" width="{x2 - x1}" height="36" rx="8" fill="{fill}" stroke="{stroke}" stroke-width="1.5"{d}/>')
    text((x1 + x2) / 2, y + 23, label, 12.5, weight, tcolor)

W, H = 1280, 360
add(f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" viewBox="0 0 {W} {H}" font-family="{FONT}">')
add('<defs><marker id="ar" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto">'
    '<path d="M0,0 L10,5 L0,10 z" fill="#64748b"/></marker>'
    '<marker id="ab" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto">'
    '<path d="M0,0 L10,5 L0,10 z" fill="#2563eb"/></marker></defs>')
add(f'<rect width="{W}" height="{H}" rx="18" fill="#ffffff"/>')

T, ARRIVE, SWAP, X0, X1 = 260, 470, 930, 170, 1230
rows = {"v1 (old)": 96, "stream": 156, "v2 (new)": 224}

# Event markers
for x, top, sub in [(T, "cutoff T", "snapshot v2 is taken"), (ARRIVE, "v2 published", "hours later"),
                    (SWAP, "atomic swap", "pointer flip")]:
    add(f'<line x1="{x}" y1="70" x2="{x}" y2="290" stroke="#94a3b8" stroke-width="1.5" stroke-dasharray="4 4"/>')
    text(x, 300 + 14, top, 12.5, "700", "#334155")
    text(x, 300 + 30, sub, 11.5, "normal", "#64748b")

# Window annotation
add(f'<path d="M{T},62 V54 H{SWAP} V62" fill="none" stroke="#2563eb" stroke-width="1.5"/>')
text((T + SWAP) / 2, 44, "Updates made after the cutoff are replayed onto v2 before it serves, so none are lost",
     13, "600", "#1d4ed8")

# Row labels
for name, y in rows.items():
    text(150, y + 23, name, 13, "700", "#334155", "end")

# Stream: updates arriving over time. Band = updates the replay re-reads.
sy = rows["stream"] + 18
add(f'<rect x="{T - 10}" y="{sy - 16}" width="{600 - T + 10}" height="32" rx="16" fill="#dbeafe" stroke="#93c5fd"/>')
x = X0 + 20
while x < X1:
    after = x > T
    add(f'<circle cx="{x}" cy="{sy}" r="6" fill="{"#2563eb" if after else "#ffffff"}" stroke="#2563eb" stroke-width="1.2"/>')
    x += 38
text(X0 + 40, sy + 32, "in snapshot", 10.5, "normal", "#64748b")

# v1
bar(X0, SWAP, rows["v1 (old)"], "#bbf7d0", "#16a34a", "v1 serving reads  ·  applying every stream update", "#14532d")
bar(SWAP, SWAP + 150, rows["v1 (old)"], "#f1f5f9", "#cbd5e1", "closed after last reader", "#64748b", True, "normal")

# v2 phases
bar(ARRIVE, 600, rows["v2 (new)"], "#fef3c7", "#d97706", "ingest SSTs", "#78350f")
bar(600, 760, rows["v2 (new)"], "#dbeafe", "#2563eb", "replay from T − skew", "#1e3a8a")
bar(760, SWAP, rows["v2 (new)"], "#eff6ff", "#60a5fa", "dual-write live updates", "#1e3a8a", True)
bar(SWAP, X1, rows["v2 (new)"], "#bbf7d0", "#16a34a", "v2 serving", "#14532d")

# Replay arrow: logged updates since the cutoff flow into v2
add(f'<path d="M430,{sy + 17} C470,{sy + 44} 600,{sy + 30} 668,{rows["v2 (new)"] - 2}" '
    'fill="none" stroke="#2563eb" stroke-width="1.8" stroke-dasharray="5 4" marker-end="url(#ab)"/>')
text(560, sy + 40, "re-read from Kafka", 11.5, "600", "#1d4ed8", halo=True)

# Time axis
add(f'<line x1="{X0}" y1="290" x2="{X1 + 20}" y2="290" stroke="#64748b" stroke-width="1.5" marker-end="url(#ar)"/>')
text(X1 + 20, 314, "time", 11.5, "normal", "#64748b", "end")
add('</svg>')
open(__import__('pathlib').Path(__file__).resolve().parent.parent / 'version-swap.svg', 'w').write("\n".join(out))
print("ok")
