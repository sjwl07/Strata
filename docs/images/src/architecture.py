# Generates docs/images/architecture.svg. Run: python3 docs/images/src/architecture.py
# Generates docs/images/architecture.svg
FONT = "-apple-system, BlinkMacSystemFont, 'Segoe UI', Helvetica, Arial, sans-serif"
out = []
def add(s): out.append(s)

def text(x, y, s, size=13, weight="normal", fill="#0f172a", anchor="middle", extra=""):
    add(f'<text x="{x}" y="{y}" font-size="{size}" font-weight="{weight}" fill="{fill}" text-anchor="{anchor}" {extra}>{s}</text>')

def box(x, y, w, h, title, sub=None, fill="#fff", stroke="#cbd5e1", tcolor="#0f172a", rx=10, sub2=None):
    add(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="{rx}" fill="{fill}" stroke="{stroke}" stroke-width="1.5"/>')
    cx = x + w / 2
    if sub is None:
        text(cx, y + h / 2 + 5, title, 14, "600", tcolor)
    elif sub2 is None:
        text(cx, y + h / 2 - 3, title, 14, "600", tcolor)
        text(cx, y + h / 2 + 15, sub, 11.5, "normal", "#475569")
    else:
        text(cx, y + h / 2 - 10, title, 14, "600", tcolor)
        text(cx, y + h / 2 + 8, sub, 11.5, "normal", "#475569")
        text(cx, y + h / 2 + 23, sub2, 11.5, "normal", "#475569")

def panel(x, y, w, h, label, fill, stroke, lcolor):
    add(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="16" fill="{fill}" stroke="{stroke}" stroke-width="1.5"/>')
    text(x + 20, y + 28, label, 12, "700", lcolor, "start", 'letter-spacing="1.2"')

def path(d, color, marker_end=True, marker_start=False, dashed=False):
    m = ""
    if marker_end:
        m += f' marker-end="url(#a-{color[1:]})"'
    if marker_start:
        m += f' marker-start="url(#a-{color[1:]})"'
    dash = ' stroke-dasharray="6 5"' if dashed else ""
    add(f'<path d="{d}" fill="none" stroke="{color}" stroke-width="2" stroke-linejoin="round"{dash}{m}/>')

def label(x, y, s, color, size=12, weight="600"):
    # text with a white halo so it stays readable over lines
    add(f'<text x="{x}" y="{y}" font-size="{size}" font-weight="{weight}" fill="{color}" text-anchor="middle" '
        f'stroke="#ffffff" stroke-width="4" paint-order="stroke">{s}</text>')

W, H = 1280, 620
colors = ["#64748b", "#16a34a", "#9333ea", "#d97706", "#2563eb"]
add(f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" viewBox="0 0 {W} {H}" font-family="{FONT}">')
add('<defs>')
for c in colors:
    add(f'<marker id="a-{c[1:]}" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" '
        f'orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" fill="{c}"/></marker>')
add('</defs>')
add(f'<rect x="0" y="0" width="{W}" height="{H}" rx="18" fill="#ffffff"/>')

# Column 1: inputs
panel(30, 30, 300, 300, "BATCH · PERIODIC", "#fffbeb", "#fbbf24", "#b45309")
box(60, 70, 240, 58, "Dataset snapshot", "e.g. a nightly Spark job's output", stroke="#f59e0b")
box(60, 158, 240, 58, "Push job", "partition · sort · build SSTs", stroke="#f59e0b")
box(60, 246, 240, 58, "Object store", "SST files + version manifest", stroke="#f59e0b")
path("M180,128 V156", "#d97706")
path("M180,216 V244", "#d97706")

panel(30, 360, 300, 230, "STREAM · CONTINUOUS", "#eff6ff", "#93c5fd", "#1d4ed8")
box(60, 400, 240, 58, "Producers", "services emitting updates", stroke="#3b82f6")
box(60, 500, 240, 58, "Update log · Kafka", "one partition per store partition", stroke="#3b82f6")
path("M180,458 V498", "#2563eb")

# Column 2: serving cluster
panel(440, 30, 440, 560, "SERVING CLUSTER", "#f0fdf4", "#86efac", "#15803d")
box(470, 66, 380, 66, "Controller", "membership · replica placement · push gate", stroke="#16a34a")
cards = [(470, "Node 1", ["p0", "p1", "p3"]), (604, "Node 2", ["p0", "p2", "p4"]), (738, "Node 3", ["p1", "p2", "p5"])]
for x0, name, parts in cards:
    y0 = 196
    add(f'<rect x="{x0}" y="{y0}" width="112" height="300" rx="12" fill="#ffffff" stroke="#16a34a" stroke-width="1.5"/>')
    text(x0 + 56, y0 + 28, name, 14, "700")
    text(x0 + 56, y0 + 54, "replicas", 10.5, "600", "#64748b", extra='letter-spacing="0.6"')
    for i, p in enumerate(parts):
        cx = x0 + 10 + i * 32
        add(f'<rect x="{cx}" y="{y0 + 63}" width="28" height="22" rx="6" fill="#f1f5f9" stroke="#cbd5e1"/>')
        text(cx + 14, y0 + 79, p, 11.5, "600", "#334155")
    text(x0 + 56, y0 + 116, "versions", 10.5, "600", "#64748b", extra='letter-spacing="0.6"')
    add(f'<rect x="{x0 + 10}" y="{y0 + 125}" width="92" height="28" rx="7" fill="#bbf7d0" stroke="#16a34a"/>')
    text(x0 + 56, y0 + 144, "v2 · serving", 11.5, "600", "#14532d")
    add(f'<rect x="{x0 + 10}" y="{y0 + 161}" width="92" height="28" rx="7" fill="#ffffff" stroke="#16a34a" stroke-dasharray="4 3"/>')
    text(x0 + 56, y0 + 180, "v3 · preparing", 11.5, "normal", "#166534")
    text(x0 + 56, y0 + 222, "engine", 10.5, "600", "#64748b", extra='letter-spacing="0.6"')
    text(x0 + 56, y0 + 244, "RocksDB", 13, "700", "#0f172a")
    text(x0 + 56, y0 + 263, "LWW merge", 11.5, "normal", "#475569")
    text(x0 + 56, y0 + 280, "replay · swap", 11.5, "normal", "#475569")
    path(f"M{x0 + 56},134 V194", "#16a34a", marker_start=True)
label(660, 168, "heartbeats", "#15803d", 11.5)
text(660, 530, "Each partition lives on RF nodes.", 12, "normal", "#166534")
text(660, 548, "Every replica consumes its own partitions of the log,", 12, "normal", "#166534")
text(660, 566, "so the log doubles as the replication stream.", 12, "normal", "#166534")

# Column 3: reads
panel(990, 30, 260, 400, "READ PATH", "#faf5ff", "#d8b4fe", "#7e22ce")
box(1015, 66, 210, 56, "Application", stroke="#a855f7")
add('<rect x="1015" y="190" width="210" height="210" rx="10" fill="#ffffff" stroke="#a855f7" stroke-width="1.5"/>')
text(1120, 220, "StrataClient", 15, "700")
for i, line in enumerate(["C3 replica ranking", "budgeted hedging", "cancels slower request", "failover", "scatter-gather batchGet"]):
    y = 252 + i * 28
    add(f'<circle cx="1036" cy="{y - 4}" r="3.5" fill="#a855f7"/>')
    text(1048, y, line, 12.5, "normal", "#334155", "start")
path("M1120,122 V188", "#9333ea")

# Legend
add('<rect x="990" y="450" width="260" height="140" rx="16" fill="#ffffff" stroke="#e2e8f0" stroke-width="1.5"/>')
text(1010, 476, "LEGEND", 12, "700", "#64748b", "start", 'letter-spacing="1.2"')
legend = [("#d97706", False, False, "batch data and pushes"), ("#2563eb", False, False, "stream updates"),
          ("#16a34a", True, False, "control: heartbeats"), ("#9333ea", True, False, "reads"),
          ("#9333ea", False, True, "routing metadata")]
for i, (c, both, dashed, name) in enumerate(legend):
    y = 498 + i * 20
    path(f"M1012,{y} H1050", c, marker_start=both, dashed=dashed)
    text(1062, y + 4, name, 12, "normal", "#334155", "start")

# Cross-column arrows: data flows into the cluster as a whole, not into one node
path("M300,187 H392 V99 H468", "#d97706")
label(346, 180, "StartPush(v)", "#b45309")
path("M300,275 H436", "#d97706")
label(368, 267, "SST files", "#b45309")
path("M300,529 H436", "#2563eb")
label(368, 521, "updates", "#1d4ed8")
path("M850,99 H935 V240 H1013", "#9333ea", dashed=True)
label(935, 160, "cluster view", "#7e22ce")
path("M1013,330 H884", "#9333ea", marker_start=True)
label(935, 322, "Get / BatchGet", "#7e22ce")
label(935, 350, "+ load feedback", "#7e22ce", 11, "normal")

add('</svg>')
open(__import__('pathlib').Path(__file__).resolve().parent.parent / 'architecture.svg', 'w').write("\n".join(out))
print("ok")
