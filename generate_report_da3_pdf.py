#!/usr/bin/env python3
"""
generate_report_da3_pdf.py — builds Digital_Assignment_3_Report.pdf from REPORT.md.

The PDF is generated from the Markdown (headings, paragraphs, lists, tables, code
blocks), so the two never drift apart. The evaluation plots of the serving model
(data/models/registry/vN/plots) are appended as figures.

Run:  make report      (or: venv/bin/python generate_report_da3_pdf.py)
"""

import html
import os
import re

import matplotlib
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import cm
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import (HRFlowable, Image, KeepTogether, ListFlowable, ListItem, PageBreak,
                                Paragraph, Preformatted, SimpleDocTemplate, Spacer, Table, TableStyle)

SRC = "REPORT.md"
OUT = "Digital_Assignment_3_Report.pdf"
REGISTRY = "data/models/registry"

# DejaVu (bundled with matplotlib) covers →, ≥, ✅ and the box-drawing characters in the diagram;
# the built-in PDF fonts do not and would render them as black boxes.
_TTF = os.path.join(os.path.dirname(matplotlib.__file__), "mpl-data", "fonts", "ttf")
for name, file in [("Sans", "DejaVuSans.ttf"), ("Sans-Bold", "DejaVuSans-Bold.ttf"),
                   ("Sans-Italic", "DejaVuSans-Oblique.ttf"), ("Sans-BoldItalic", "DejaVuSans-BoldOblique.ttf"),
                   ("Mono", "DejaVuSansMono.ttf")]:
    pdfmetrics.registerFont(TTFont(name, os.path.join(_TTF, file)))
pdfmetrics.registerFontFamily("Sans", normal="Sans", bold="Sans-Bold", italic="Sans-Italic",
                              boldItalic="Sans-BoldItalic")

NAVY = colors.HexColor("#1F3A5F")
GRID = colors.HexColor("#B8C2CC")
HEAD_BG = colors.HexColor("#E8EEF4")

ss = getSampleStyleSheet()
S = {
    "title": ParagraphStyle("t", parent=ss["Title"], fontName="Sans-Bold", fontSize=17, leading=21,
                            textColor=NAVY, spaceAfter=4),
    "subtitle": ParagraphStyle("st", parent=ss["Normal"], fontName="Sans-Bold", fontSize=12.5, leading=16,
                               alignment=TA_CENTER, textColor=NAVY, spaceAfter=10),
    "h1": ParagraphStyle("h1", parent=ss["Heading1"], fontName="Sans-Bold", fontSize=13.5, leading=17,
                         textColor=NAVY, spaceBefore=12, spaceAfter=6, keepWithNext=1),
    "h2": ParagraphStyle("h2", parent=ss["Heading2"], fontName="Sans-Bold", fontSize=11.5, leading=15,
                         textColor=NAVY, spaceBefore=8, spaceAfter=4, keepWithNext=1),
    "body": ParagraphStyle("b", parent=ss["Normal"], fontName="Sans", fontSize=9.6, leading=13.4, spaceAfter=5),
    "meta": ParagraphStyle("m", parent=ss["Normal"], fontName="Sans", fontSize=9.6, leading=13.2),
    "cell": ParagraphStyle("c", parent=ss["Normal"], fontName="Sans", fontSize=8.4, leading=10.6),
    "cellh": ParagraphStyle("ch", parent=ss["Normal"], fontName="Sans-Bold", fontSize=8.4, leading=10.6),
    "code": ParagraphStyle("code", parent=ss["Code"], fontName="Mono", fontSize=7.4, leading=9.2,
                           backColor=colors.HexColor("#F4F6F8"), borderPadding=5, spaceBefore=3, spaceAfter=7),
    "caption": ParagraphStyle("cap", parent=ss["Normal"], fontName="Sans-Italic", fontSize=8.6, leading=11,
                              alignment=TA_CENTER, spaceAfter=10),
}


def inline(text: str) -> str:
    """Markdown inline → reportlab mini-HTML: escape, **bold**, *italic*, `code`, <br>.

    Code spans are swapped for placeholders first, so bold/italic markers around them
    (e.g. **Full (`LITE=0`):**) still pair up.
    """
    text = text.replace("\\*", "\x00")                    # escaped asterisk
    codes: list[str] = []

    def stash(m):
        codes.append(f'<font name="Mono" size="8.6">{html.escape(m.group(1))}</font>')
        return f"\x01{len(codes) - 1}\x02"

    text = re.sub(r"`([^`]+)`", stash, text)
    text = html.escape(text).replace("&lt;br&gt;", "<br/>")
    text = re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", text)
    text = re.sub(r"(?<![\w*])\*(?!\s)(.+?)(?<!\s)\*(?![\w*])", r"<i>\1</i>", text)
    text = re.sub("\x01(\\d+)\x02", lambda m: codes[int(m.group(1))], text)
    return text.replace("\x00", "*")


def table(rows: list[list[str]], width: float) -> Table:
    ncol = len(rows[0])
    data = [[Paragraph(inline(c), S["cellh"] if i == 0 else S["cell"]) for c in r] for i, r in enumerate(rows)]
    # Give the widest-text columns more room
    # Width ∝ average cell length, but never narrower than the column's longest word
    lens = [sum(len(r[j]) if j < len(r) else 0 for r in rows) / len(rows) for j in range(ncol)]
    lens = [min(max(n, 8), 70) for n in lens]
    words = [max((len(w) for r in rows if j < len(r) for w in re.sub(r"[*`]", "", r[j]).split()), default=4)
             for j in range(ncol)]
    widths = [max(width * n / sum(lens), w * 5.6 + 10) for n, w in zip(lens, words)]
    widths = [w * width / sum(widths) for w in widths]
    t = Table(data, colWidths=widths, repeatRows=1, hAlign="LEFT")
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), HEAD_BG),
        ("GRID", (0, 0), (-1, -1), 0.4, GRID),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING", (0, 0), (-1, -1), 4), ("RIGHTPADDING", (0, 0), (-1, -1), 4),
        ("TOPPADDING", (0, 0), (-1, -1), 2.5), ("BOTTOMPADDING", (0, 0), (-1, -1), 2.5),
    ]))
    return t


def bullet_list(items: list[tuple[int, str]], ordered: bool) -> ListFlowable:
    """items: (indent level, text). Nested level-1 items attach to the previous level-0 item."""
    flow, cur = [], None
    for level, text in items:
        if level == 0:
            cur = [Paragraph(inline(text), S["body"])]
            flow.append(cur)
        else:
            sub = Paragraph(inline(text), S["body"])
            if cur is None:
                cur = [sub]
                flow.append(cur)
            elif len(cur) > 1 and isinstance(cur[-1], list):
                cur[-1].append(sub)
            else:
                cur.append([sub])
    li = []
    for entry in flow:
        parts = [entry[0]]
        for extra in entry[1:]:
            parts.append(ListFlowable([ListItem(p, leftIndent=12) for p in extra], bulletType="bullet",
                                      start="–", leftIndent=12, bulletFontName="Sans", bulletFontSize=8))
        li.append(ListItem(parts, leftIndent=14))
    kw = dict(bulletType="1", bulletFormat="%s.", bulletFontName="Sans", bulletFontSize=9) if ordered else \
        dict(bulletType="bullet", start="•", bulletFontName="Sans", bulletFontSize=9)
    return ListFlowable(li, leftIndent=14, **kw)


def build_story(md: str, width: float) -> list:
    lines = md.splitlines()
    story, i, para = [], 0, []

    def flush_para():
        if para:
            story.append(Paragraph(inline(" ".join(s.strip() for s in para)), S["body"]))
            para.clear()

    while i < len(lines):
        ln = lines[i]
        s = ln.strip()
        if s.startswith("```"):
            flush_para()
            j = i + 1
            while j < len(lines) and not lines[j].strip().startswith("```"):
                j += 1
            block = [Preformatted("\n".join(lines[i + 1:j]), S["code"])]
            # Pull a heading that directly precedes the block onto the same page
            if story and getattr(story[-1], "style", None) in (S["h1"], S["h2"]):
                block.insert(0, story.pop())
            story.append(KeepTogether(block))
            i = j + 1
            continue
        if s.startswith("|"):
            flush_para()
            rows = []
            while i < len(lines) and lines[i].strip().startswith("|"):
                cells = [c.strip() for c in lines[i].strip().strip("|").split("|")]
                if not all(re.fullmatch(r":?-{2,}:?", c) for c in cells):
                    rows.append(cells)
                i += 1
            story.append(table(rows, width))
            story.append(Spacer(1, 7))
            continue
        m_b = re.match(r"^(\s*)([-*]|\d+\.)\s+(.*)$", ln)
        if m_b and not s.startswith("**") and s != "---":
            flush_para()
            ordered = m_b.group(2).endswith(".")
            items = []
            while i < len(lines):
                m = re.match(r"^(\s*)([-*]|\d+\.)\s+(.*)$", lines[i])
                if m:
                    items.append((1 if len(m.group(1)) >= 2 else 0, m.group(3)))
                elif lines[i].startswith("    ") and items and lines[i].strip():
                    lvl, txt = items[-1]
                    items[-1] = (lvl, txt + " " + lines[i].strip())
                else:
                    break
                i += 1
            story.append(bullet_list(items, ordered))
            story.append(Spacer(1, 4))
            continue
        if s == "---":
            flush_para()
            story.append(HRFlowable(width="100%", thickness=0.6, color=GRID, spaceBefore=4, spaceAfter=6))
        elif s.startswith("# "):
            flush_para()
            story.append(Paragraph(inline(s[2:]), S["title"]))
        elif s.startswith("## ") and not story_has_h1(story):
            flush_para()
            story.append(Paragraph(inline(s[3:]), S["subtitle"]))
        elif s.startswith("## "):
            flush_para()
            story.append(Paragraph(inline(s[3:]), S["h1"]))
        elif s.startswith("### "):
            flush_para()
            story.append(Paragraph(inline(s[4:]), S["h2"]))
        elif not s:
            flush_para()
        elif ln.endswith("  "):                     # Markdown hard line break (header block)
            para.append(s)
            story.append(Paragraph(inline(" ".join(para)), S["meta"]))
            para.clear()
        else:
            para.append(s)
        i += 1
    flush_para()
    return story


def story_has_h1(story) -> bool:
    return any(getattr(f, "style", None) is S["subtitle"] for f in story)


def figures(width: float) -> list:
    try:
        version = int(open(os.path.join(REGISTRY, "latest.json")).read().split(":")[1].strip(" }\n"))
    except (OSError, ValueError, IndexError):
        return []
    plots = os.path.join(REGISTRY, f"v{version}", "plots")
    wanted = [
        ("confusion_matrix.png", "RandomForest confusion matrix on the held-out test split (row-normalised)."),
        ("confusion_matrix_live.png", "RandomForest confusion matrix on the live stream (flows never used in training)."),
        ("feature_importance.png", "RandomForest feature importance (top 20)."),
        ("anomaly_scores_live.png", "KMeans centroid-distance (anomaly score) distribution on live traffic, benign vs "
                                    "attack, with the alert threshold."),
    ]
    out, n = [], 0
    for file, caption in wanted:
        p = os.path.join(plots, file)
        if not os.path.exists(p):
            continue
        n += 1
        img = Image(p)
        scale = min(width * 0.78 / img.drawWidth, 9.5 * cm / img.drawHeight)
        img.drawWidth, img.drawHeight = img.drawWidth * scale, img.drawHeight * scale
        out.append(KeepTogether([img, Paragraph(f"Figure {n}. {caption}", S["caption"])]))
    if out:
        out.insert(0, Paragraph("Appendix: Evaluation Figures", S["h1"]))
        out.insert(0, PageBreak())
    return out


def footer(canvas, doc):
    canvas.saveState()
    canvas.setFont("Sans", 7.5)
    canvas.setFillColor(colors.grey)
    canvas.drawString(doc.leftMargin, 1.1 * cm, "BCSE402L Big Data Analytics · Digital Assignment 3")
    canvas.drawRightString(A4[0] - doc.rightMargin, 1.1 * cm, f"Page {doc.page}")
    canvas.restoreState()


def main():
    doc = SimpleDocTemplate(OUT, pagesize=A4, leftMargin=2 * cm, rightMargin=2 * cm, topMargin=1.8 * cm,
                            bottomMargin=1.8 * cm, title="BCSE402L Digital Assignment 3 Report",
                            author="Arjun Gupta", subject="Kappa-Architecture Network-Flow Anomaly Detection Platform")
    width = A4[0] - doc.leftMargin - doc.rightMargin
    story = build_story(open(SRC, encoding="utf-8").read(), width) + figures(width)
    doc.build(story, onFirstPage=footer, onLaterPages=footer)
    print(f"Wrote {OUT}")


if __name__ == "__main__":
    main()
