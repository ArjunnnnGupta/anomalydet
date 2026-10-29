#!/usr/bin/env python3
"""
generate_report_da3_pdf.py — builds Digital_Assignment_3_Report.pdf from REPORT.md.

Layout follows the VIT report format: a cover page (VIT logo, course, assignment, title,
team and guide), a border on every page, Times type, black-grid tables and numbered
figure captions. The cover fields come from the front matter at the top of REPORT.md;
the body is rendered from the Markdown (headings, paragraphs, lists, tables, images).
Figures are redrawn from the current lake by report_figures.py first, so the numbers
in the charts always match the pipeline.

Run:  make report      (or: venv/bin/python generate_report_da3_pdf.py)
"""

import html
import os
import re

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_JUSTIFY
from reportlab.lib.pagesizes import LETTER
from reportlab.lib.styles import ParagraphStyle
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import (Image, KeepTogether, ListFlowable, ListItem, PageBreak, Paragraph,
                                SimpleDocTemplate, Spacer, Table, TableStyle)

import report_figures

SRC = "REPORT.md"
OUT = "Digital_Assignment_3_Report.pdf"
LOGO = "assets/vit_logo.png"

# Times New Roman ships with macOS and, unlike the built-in PDF Times, has →, ≥ and –
_FONTS = "/System/Library/Fonts/Supplemental"
for name, file in [("Times", "Times New Roman.ttf"), ("Times-B", "Times New Roman Bold.ttf"),
                   ("Times-I", "Times New Roman Italic.ttf"), ("Times-BI", "Times New Roman Bold Italic.ttf")]:
    pdfmetrics.registerFont(TTFont(name, os.path.join(_FONTS, file)))
pdfmetrics.registerFontFamily("Times", normal="Times", bold="Times-B", italic="Times-I", boldItalic="Times-BI")

PAGE_W, PAGE_H = LETTER
BORDER = 30            # page border inset (pt)
MARGIN = 72            # text margin (pt)


def style(name, size, leading=None, font="Times", align=None, **kw):
    return ParagraphStyle(name, fontName=font, fontSize=size, leading=leading or size * 1.25,
                          alignment=align if align is not None else 0, **kw)


S = {
    "title": style("title", 17, 22, "Times-B", TA_CENTER, spaceAfter=14),
    "h1": style("h1", 13.5, 17, "Times-B", spaceBefore=14, spaceAfter=7, keepWithNext=1),
    "h2": style("h2", 12, 15, "Times-B", spaceBefore=10, spaceAfter=6, keepWithNext=1),
    "body": style("body", 12, 16.5, align=TA_JUSTIFY, spaceAfter=8),
    "item": style("item", 12, 16.5),
    "cell": style("cell", 10.5, 12.6),
    "cellc": style("cellc", 10.5, 12.6, align=TA_CENTER),
    "cellh": style("cellh", 10.5, 12.6, "Times-B", TA_CENTER),
    "caption": style("caption", 10, 12.5, "Times-I", TA_CENTER, spaceBefore=4, spaceAfter=12),
    # cover
    "c_course": style("cc", 23, 28, align=TA_CENTER),
    "c_code": style("ccode", 10.5, 13, align=TA_CENTER),
    "c_da": style("cda", 16, 20, align=TA_CENTER),
    "c_title": style("ct", 25, 33, align=TA_CENTER),
    "c_label": style("cl", 10.5, 22, "Times-B"),
    "c_text": style("ctx", 10.5, 22),
}


def inline(text: str) -> str:
    """Markdown inline → reportlab mini-HTML: escape, **bold**, *italic*, `code` (set as plain text)."""
    text = html.escape(text.replace("\\*", "\x00"))
    text = re.sub(r"`([^`]+)`", r"\1", text)
    text = re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", text)
    text = re.sub(r"(?<![\w*])\*(?!\s)(.+?)(?<!\s)\*(?![\w*])", r"<i>\1</i>", text)
    return text.replace("\x00", "*")


NUMERIC = re.compile(r"^[\d,.%/\s–\-×v@silver]*\d[\d,.%/\s–\-()a-z]*$")


def table(rows: list[list[str]], width: float) -> Table:
    ncol = len(rows[0])
    body = rows[1:]
    centred = [all(NUMERIC.match(r[j]) for r in body if j < len(r)) for j in range(ncol)]
    data = [[Paragraph(inline(c), S["cellh"]) for c in rows[0]]] + \
           [[Paragraph(inline(c), S["cellc"] if centred[j] else S["cell"]) for j, c in enumerate(r)] for r in body]
    # Two-column tables are narrower, as in the reference layout
    width = width * (0.86 if ncol == 2 else 1.0)
    # Width ∝ average cell length, but never narrower than the column's longest word
    lens = [min(max(sum(len(r[j]) for r in rows if j < len(r)) / len(rows), 16), 70) for j in range(ncol)]
    words = [max((len(w) for r in rows if j < len(r) for w in r[j].split()), default=4) for j in range(ncol)]
    widths = [max(width * n / sum(lens), w * 5.8 + 12) for n, w in zip(lens, words)]
    widths = [w * width / sum(widths) for w in widths]
    t = Table(data, colWidths=widths, repeatRows=1, hAlign="CENTER")
    t.setStyle(TableStyle([
        ("GRID", (0, 0), (-1, -1), 0.6, colors.black),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("LEFTPADDING", (0, 0), (-1, -1), 5), ("RIGHTPADDING", (0, 0), (-1, -1), 5),
        ("TOPPADDING", (0, 0), (-1, -1), 3), ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
    ]))
    return t


def bullet_list(items: list[str], ordered: bool) -> ListFlowable:
    li = [ListItem(Paragraph(inline(t), S["item"]), leftIndent=36, spaceAfter=3) for t in items]
    kw = dict(bulletType="1", bulletFormat="%s.", bulletFontSize=10.5) if ordered else \
        dict(bulletType="bullet", start="•", bulletFontSize=10)
    return ListFlowable(li, leftIndent=36, bulletFontName="Times", bulletDedent=24, **kw)


def figure(path: str, caption: str, n: int, width: float):
    img = Image(path)
    scale = min(width * 0.955 / img.drawWidth, 330 / img.drawHeight)
    img.drawWidth, img.drawHeight = img.drawWidth * scale, img.drawHeight * scale
    return KeepTogether([img, Paragraph(f"Figure {n}: {inline(caption)}", S["caption"])])


def front_matter(md: str) -> tuple[dict, str]:
    m = re.match(r"^---\n(.*?)\n---\n", md, re.S)
    if not m:
        return {}, md
    meta = dict(line.split(":", 1) for line in m.group(1).splitlines() if ":" in line)
    return {k.strip(): v.strip() for k, v in meta.items()}, md[m.end():]


def cover(meta: dict, width: float) -> list:
    logo = Image(LOGO)
    scale = 335 / logo.drawWidth
    logo.drawWidth, logo.drawHeight = logo.drawWidth * scale, logo.drawHeight * scale
    team = [p.strip() for p in meta.get("team", "").split(";") if p.strip()]
    guide = [p.strip() for p in meta.get("submitted", "").split(";") if p.strip()]
    rows = [[Paragraph("Team Members:", S["c_label"]), Paragraph("Submitted to:", S["c_label"])]]
    for i in range(max(len(team), len(guide))):
        rows.append([Paragraph(team[i] if i < len(team) else "", S["c_text"]),
                     Paragraph(guide[i] if i < len(guide) else "", S["c_text"])])
    people = Table(rows, colWidths=[285, width - 285 + 30], hAlign="LEFT")
    people.setStyle(TableStyle([("LEFTPADDING", (0, 0), (-1, -1), 0), ("TOPPADDING", (0, 0), (-1, -1), 0),
                                ("BOTTOMPADDING", (0, 0), (-1, -1), 0)]))
    return [Spacer(1, 6), logo, Spacer(1, 92),
            Paragraph(meta.get("course", ""), S["c_course"]), Spacer(1, 8),
            Paragraph(meta.get("code", ""), S["c_code"]), Spacer(1, 16),
            Paragraph(meta.get("assignment", ""), S["c_da"]), Spacer(1, 18),
            Paragraph(meta.get("title", ""), S["c_title"]), Spacer(1, 120),
            people, PageBreak()]


def build_story(md: str, width: float) -> list:
    lines = md.splitlines()
    story, para, i, nfig = [], [], 0, 0

    def flush():
        if para:
            story.append(Paragraph(inline(" ".join(para)), S["body"]))
            para.clear()

    while i < len(lines):
        s = lines[i].strip()
        img = re.match(r"^!\[(.*)\]\((.+)\)$", s)
        if img:
            flush()
            nfig += 1
            story.append(figure(img.group(2), img.group(1), nfig, width))
        elif s.startswith("|"):
            flush()
            rows = []
            while i < len(lines) and lines[i].strip().startswith("|"):
                cells = [c.strip() for c in lines[i].strip().strip("|").split("|")]
                if not all(re.fullmatch(r":?-{2,}:?", c) for c in cells):
                    rows.append(cells)
                i += 1
            story += [KeepTogether(table(rows, width)), Spacer(1, 12)]   # never split a table
            continue
        elif re.match(r"^([-*]|\d+\.)\s+", s):
            flush()
            ordered = s[0].isdigit()
            items = []
            while i < len(lines) and (m := re.match(r"^\s*([-*]|\d+\.)\s+(.*)$", lines[i])):
                items.append(m.group(2))
                i += 1
            story += [bullet_list(items, ordered), Spacer(1, 8)]
            continue
        elif s.startswith("# "):
            flush()
            story.append(Paragraph(inline(s[2:]), S["title"]))
        elif s.startswith("## "):
            flush()
            text = s[3:]
            # Sections 2, 5 and 6 start on a new page, as in the reference layout
            if re.match(r"^[256]\.", text):
                story.append(PageBreak())
            story.append(Paragraph(inline(text), S["h1"]))
        elif s.startswith("### "):
            flush()
            story.append(Paragraph(inline(s[4:]), S["h2"]))
        elif not s:
            flush()
        elif s.endswith("–") and not para:                # "Future Scope –" style lead-in
            story.append(Paragraph(f"<u>{inline(s)}</u>", S["body"]))
        else:
            para.append(s)
        i += 1
    flush()
    return story


def border(canvas, doc):
    canvas.saveState()
    canvas.setLineWidth(0.9)
    canvas.rect(BORDER, BORDER, PAGE_W - 2 * BORDER, PAGE_H - 2 * BORDER)
    canvas.restoreState()


def main():
    report_figures.build()
    meta, md = front_matter(open(SRC, encoding="utf-8").read())
    doc = SimpleDocTemplate(OUT, pagesize=LETTER, leftMargin=MARGIN, rightMargin=MARGIN, topMargin=66,
                            bottomMargin=46, title=meta.get("title", "Digital Assignment 3").title(),
                            author=", ".join(t.split("–")[0].strip() for t in meta.get("team", "").split(";")),
                            subject=f"{meta.get('course', '')} {meta.get('assignment', '')}")
    width = PAGE_W - 2 * MARGIN
    doc.build(cover(meta, width) + build_story(md, width), onFirstPage=border, onLaterPages=border)
    print(f"Wrote {OUT}")


if __name__ == "__main__":
    main()
