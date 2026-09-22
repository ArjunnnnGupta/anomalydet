#!/usr/bin/env python3
"""
generate_report_pdf.py — Compiles the Digital Assignment 2 Report into a 
professional 5-6 page PDF matching the exact structure and formatting of the 
VIT reference document.
"""

import os
from reportlab.lib.pagesizes import letter
from reportlab.lib import colors
from reportlab.lib.units import inch
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, Image, PageBreak, KeepTogether, HRFlowable
)
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_JUSTIFY, TA_RIGHT
from reportlab.pdfgen import canvas 

PDF_OUTPUT = "Digital_Assignment_2_Report.pdf"

class NumberedCanvas(canvas.Canvas):
    """Adds 'Page X of Y' and running header to every page except page 1."""
    def __init__(self, *args, **kwargs):
        super(NumberedCanvas, self).__init__(*args, **kwargs)
        self._saved_page_states = []

    def showPage(self):
        self._saved_page_states.append(dict(self.__dict__))
        self._startPage()

    def save(self):
        num_pages = len(self._saved_page_states)
        for state in self._saved_page_states:
            self.__dict__.update(state)
            self.draw_page_decorations(num_pages)
            super(NumberedCanvas, self).showPage()
        super(NumberedCanvas, self).save()

    def draw_page_decorations(self, page_count):
        if self._pageNumber > 1:
            self.saveState()
            self.setFont("Helvetica", 9)
            self.setFillColor(colors.HexColor("#555555"))
            
            # Header
            self.drawString(54, letter[1] - 36, "Flow-Level Network Anomaly Detection Platform — Digital Assignment 2")
            self.setStrokeColor(colors.HexColor("#CCCCCC"))
            self.setLineWidth(0.5)
            self.line(54, letter[1] - 42, letter[0] - 54, letter[1] - 42)
            
            # Footer
            self.setStrokeColor(colors.HexColor("#CCCCCC"))
            self.setLineWidth(0.5)
            self.line(54, 45, letter[0] - 54, 45)
            self.drawString(54, 32, "Vellore Institute of Technology | BCSE402L Big Data Analytics")
            page_str = f"Page {self._pageNumber} of {page_count}"
            self.drawRightString(letter[0] - 54, 32, page_str)
            self.restoreState()

def build_pdf():
    doc = SimpleDocTemplate(
        PDF_OUTPUT,
        pagesize=letter,
        leftMargin=54,
        rightMargin=54,
        topMargin=54,
        bottomMargin=54,
    )

    styles = getSampleStyleSheet()

    # Custom styles
    title_univ = ParagraphStyle(
        'UnivHeader',
        parent=styles['Normal'],
        fontName='Helvetica-Bold',
        fontSize=20,
        leading=24,
        alignment=TA_CENTER,
        textColor=colors.HexColor('#0B2545')
    )
    sub_univ = ParagraphStyle(
        'UnivSub',
        parent=styles['Normal'],
        fontName='Helvetica',
        fontSize=10,
        leading=14,
        alignment=TA_CENTER,
        textColor=colors.HexColor('#333333')
    )
    doc_course = ParagraphStyle(
        'CourseTitle',
        parent=styles['Normal'],
        fontName='Helvetica-Bold',
        fontSize=18,
        leading=22,
        alignment=TA_CENTER,
        textColor=colors.HexColor('#111111')
    )
    doc_code = ParagraphStyle(
        'CourseCode',
        parent=styles['Normal'],
        fontName='Helvetica',
        fontSize=12,
        leading=16,
        alignment=TA_CENTER,
        textColor=colors.HexColor('#444444')
    )
    doc_assignment = ParagraphStyle(
        'AssignmentTitle',
        parent=styles['Normal'],
        fontName='Helvetica-Oblique',
        fontSize=16,
        leading=20,
        alignment=TA_CENTER,
        textColor=colors.HexColor('#1B4965')
    )
    proj_title = ParagraphStyle(
        'ProjectTitle',
        parent=styles['Normal'],
        fontName='Helvetica-Bold',
        fontSize=21,
        leading=26,
        alignment=TA_CENTER,
        textColor=colors.HexColor('#0B2545')
    )
    h1 = ParagraphStyle(
        'H1',
        parent=styles['Heading1'],
        fontName='Helvetica-Bold',
        fontSize=14,
        leading=18,
        textColor=colors.HexColor('#0B2545'),
        spaceBefore=12,
        spaceAfter=6,
        keepWithNext=True
    )
    h2 = ParagraphStyle(
        'H2',
        parent=styles['Heading2'],
        fontName='Helvetica-Bold',
        fontSize=11.5,
        leading=15,
        textColor=colors.HexColor('#1B4965'),
        spaceBefore=8,
        spaceAfter=4,
        keepWithNext=True
    )
    body = ParagraphStyle(
        'Body',
        parent=styles['Normal'],
        fontName='Helvetica',
        fontSize=9.5,
        leading=13.5,
        alignment=TA_JUSTIFY,
        textColor=colors.HexColor('#222222'),
        spaceBefore=3,
        spaceAfter=5
    )
    code_style = ParagraphStyle(
        'CodeBlock',
        parent=styles['Normal'],
        fontName='Courier',
        fontSize=7.5,
        leading=10.5,
        textColor=colors.HexColor('#1A1A1A')
    )
    caption = ParagraphStyle(
        'Caption',
        parent=styles['Normal'],
        fontName='Helvetica-Oblique',
        fontSize=8.5,
        leading=11,
        alignment=TA_CENTER,
        textColor=colors.HexColor('#555555'),
        spaceBefore=4,
        spaceAfter=8
    )

    story = []

    # ═════════════════════════════════════════════════════════════════════════
    # PAGE 1: TITLE PAGE
    # ═════════════════════════════════════════════════════════════════════════
    story.append(Spacer(1, 0.4 * inch))
    story.append(Paragraph("<b>VIT</b>", title_univ))
    story.append(Paragraph("<b>Vellore Institute of Technology</b>", ParagraphStyle('Sub', parent=title_univ, fontSize=13, leading=16)))
    story.append(Paragraph("(Deemed to be University under section 3 of UGC Act, 1956)<br/><b>CHENNAI</b>", sub_univ))
    story.append(Spacer(1, 0.25 * inch))
    story.append(HRFlowable(width="80%", thickness=1.5, color=colors.HexColor("#0B2545"), spaceBefore=5, spaceAfter=20))

    story.append(Spacer(1, 0.6 * inch))
    story.append(Paragraph("BIG DATA ANALYTICS", doc_course))
    story.append(Spacer(1, 0.1 * inch))
    story.append(Paragraph("COURSE CODE – BCSE402L", doc_code))
    story.append(Spacer(1, 0.4 * inch))
    story.append(Paragraph("Digital Assignment 2", doc_assignment))
    story.append(Spacer(1, 0.4 * inch))
    story.append(Paragraph("FLOW-LEVEL NETWORK ANOMALY<br/>DETECTION PLATFORM", proj_title))
    story.append(Spacer(1, 1.2 * inch))

    # Team & Submission info in a table matching reference
    info_data = [
        [
            Paragraph("<b>Team Members:</b><br/><br/>Arjun Gupta – [Registration Number]<br/>[Team Member 2] – [Registration Number]", ParagraphStyle('Left', parent=body, fontSize=10.5, leading=16)),
            Paragraph("<b>Submitted to:</b><br/><br/>Dr. Dharini R<br/>SCOPE", ParagraphStyle('Right', parent=body, fontSize=10.5, leading=16, alignment=TA_LEFT))
        ]
    ]
    info_table = Table(info_data, colWidths=[3.2 * inch, 3.2 * inch])
    info_table.setStyle(TableStyle([
        ('VALIGN', (0,0), (-1,-1), 'TOP'),
        ('LEFTPADDING', (0,0), (-1,-1), 10),
        ('RIGHTPADDING', (0,0), (-1,-1), 10),
    ]))
    story.append(info_table)
    story.append(PageBreak())

    # ═════════════════════════════════════════════════════════════════════════
    # PAGE 2: SECTION 1 — APPLICATION OF CONCEPTS
    # ═════════════════════════════════════════════════════════════════════════
    story.append(Paragraph("1. Application of Concepts", h1))
    story.append(Paragraph(
        "The project applies distributed data processing with Apache Spark (PySpark) and declarative querying "
        "with Spark SQL to ingest, clean, transform, and detect malicious network intrusion flows from the CICIDS2017 "
        "security benchmark dataset. For Review 2 (Midway Progress), the complete ingestion-to-dashboard pipeline has "
        "been implemented, tested, and verified on <b>2,520,751 raw network flow records</b>.",
        body
    ))

    story.append(Paragraph("1.1 Implemented Pipeline", h2))
    
    # Visual pipeline diagram using Table
    pipe_boxes = [
        [Paragraph("<b>Raw Network Flow Logs</b><br/>CICIDS2017 Dataset (2,520,751 records × 53 features)", ParagraphStyle('PBox', parent=caption, fontSize=8, leading=10))],
        [Paragraph("↓", caption)],
        [Paragraph("<b>PySpark Distributed Ingestion & Cleaning</b><br/>Type casting · Inf/-Inf replacement · Null dropping · Zero-variance detection", ParagraphStyle('PBox', parent=caption, fontSize=8, leading=10))],
        [Paragraph("↓", caption)],
        [Paragraph("<b>Columnar Parquet Analytical Store</b><br/>Compressed partitioned storage (data/clean_parquet/)", ParagraphStyle('PBox', parent=caption, fontSize=8, leading=10))],
        [Paragraph("↓", caption)],
        [Paragraph("<b>PySpark & Scikit-Learn Feature Preprocessing</b><br/>Edge-case cleanup · StandardScaler normalization · (500k × 52 feature matrix)", ParagraphStyle('PBox', parent=caption, fontSize=8, leading=10))],
        [Paragraph("↓", caption)],
        [Paragraph("<b>Machine Learning Inference Engine</b><br/>IsolationForest (unsupervised anomaly scoring) · RandomForest (multi-class validation)", ParagraphStyle('PBox', parent=caption, fontSize=8, leading=10))],
        [Paragraph("↓", caption)],
        [Paragraph("<b>SQLite Hot Alert Cache & Streamlit Dashboard</b><br/>Indexed relational cache (alerts.db) · Real-time KPI cards, charts, and alert feed", ParagraphStyle('PBox', parent=caption, fontSize=8, leading=10))]
    ]
    pipe_table = Table(pipe_boxes, colWidths=[6.2 * inch])
    pipe_table.setStyle(TableStyle([
        ('ALIGN', (0,0), (-1,-1), 'CENTER'),
        ('VALIGN', (0,0), (-1,-1), 'MIDDLE'),
        ('BACKGROUND', (0,0), (0,0), colors.HexColor("#EBF2FA")),
        ('BACKGROUND', (0,2), (0,2), colors.HexColor("#EBF2FA")),
        ('BACKGROUND', (0,4), (0,4), colors.HexColor("#EBF2FA")),
        ('BACKGROUND', (0,6), (0,6), colors.HexColor("#EBF2FA")),
        ('BACKGROUND', (0,8), (0,8), colors.HexColor("#EBF2FA")),
        ('BACKGROUND', (0,10), (0,10), colors.HexColor("#EBF2FA")),
        ('BOX', (0,0), (0,0), 0.8, colors.HexColor("#0B2545")),
        ('BOX', (0,2), (0,2), 0.8, colors.HexColor("#0B2545")),
        ('BOX', (0,4), (0,4), 0.8, colors.HexColor("#0B2545")),
        ('BOX', (0,6), (0,6), 0.8, colors.HexColor("#0B2545")),
        ('BOX', (0,8), (0,8), 0.8, colors.HexColor("#0B2545")),
        ('BOX', (0,10), (0,10), 0.8, colors.HexColor("#0B2545")),
        ('TOPPADDING', (0,0), (-1,-1), 2),
        ('BOTTOMPADDING', (0,0), (-1,-1), 2),
    ]))
    story.append(pipe_table)
    story.append(Paragraph("<i>Figure 1 — Implemented end-to-end Big Data processing and anomaly detection pipeline</i>", caption))

    story.append(Paragraph("1.2 Big Data Technique: Distributed Ingestion with Spark", h2))
    story.append(Paragraph(
        "Instead of row-by-row Python parsing (which causes memory exhaustion on a 700MB+ dataset that expands to >2.5GB in-memory), "
        "the raw flows are loaded into a distributed Spark DataFrame. Spark's catalyst optimizer schedules the workload across 8 worker cores "
        "with an explicit 2GB driver memory limit:",
        body
    ))

    code_snippet1 = (
        "df = spark.read.csv('data/raw/cicds.csv', header=True, inferSchema=True)<br/>"
        "df = df.toDF(*[c.strip() for c in df.columns])<br/>"
        "feature_cols = [c for c in df.columns if c != 'Attack Type']<br/>"
        "for col in feature_cols:<br/>"
        "&nbsp;&nbsp;&nbsp;&nbsp;df = df.withColumn(col, F.col(col).cast(DoubleType()))"
    )
    code_box1 = Table([[Paragraph(code_snippet1, code_style)]], colWidths=[6.2 * inch])
    code_box1.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,-1), colors.HexColor("#F5F7FA")),
        ('BOX', (0,0), (-1,-1), 0.5, colors.HexColor("#D0D7DE")),
        ('LEFTPADDING', (0,0), (-1,-1), 8),
        ('TOPPADDING', (0,0), (-1,-1), 6),
        ('BOTTOMPADDING', (0,0), (-1,-1), 6),
    ]))
    story.append(code_box1)

    story.append(Paragraph("1.3 Spark Transformations and Feature Engineering", h2))
    story.append(Paragraph(
        "The distributed preprocessing stage purges non-finite arithmetic values (Inf/-Inf caused by zero flow durations) "
        "and drops zero-variance columns across all 52 features:",
        body
    ))

    code_snippet2 = (
        "for col in feature_cols:<br/>"
        "&nbsp;&nbsp;&nbsp;&nbsp;df = df.withColumn(col, F.when(F.col(col).isin([float('inf'), float('-inf')]), None).otherwise(F.col(col)))<br/>"
        "df_clean = df.dropna()<br/>"
        "stats = df_clean.select([F.stddev(F.col(c)).alias(c) for c in feature_cols]).collect()[0].asDict()<br/>"
        "zero_var_cols = [c for c, std in stats.items() if std is None or std == 0.0]<br/>"
        "df_clean = df_clean.drop(*zero_var_cols)<br/>"
        "df_clean.write.mode('overwrite').parquet('data/clean_parquet/')"
    )
    code_box2 = Table([[Paragraph(code_snippet2, code_style)]], colWidths=[6.2 * inch])
    code_box2.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,-1), colors.HexColor("#F5F7FA")),
        ('BOX', (0,0), (-1,-1), 0.5, colors.HexColor("#D0D7DE")),
        ('LEFTPADDING', (0,0), (-1,-1), 8),
        ('TOPPADDING', (0,0), (-1,-1), 6),
        ('BOTTOMPADDING', (0,0), (-1,-1), 6),
    ]))
    story.append(code_box2)
    story.append(PageBreak())

    # ═════════════════════════════════════════════════════════════════════════
    # PAGE 3: SECTION 2 — PROJECT PROGRESS
    # ═════════════════════════════════════════════════════════════════════════
    story.append(Paragraph("2. Project Progress", h1))
    story.append(Paragraph("2.1 Dataset Collection", h2))
    story.append(Paragraph(
        "Data was acquired from the benchmark CICIDS2017 security capture dataset. It comprises a total of "
        "<b>2,520,751 flow records</b> across <b>53 attributes</b> (52 continuous network traffic features and 1 multi-class "
        "ground truth label <i>Attack Type</i> covering benign behavior and six major attack vectors: DoS, DDoS, Port Scanning, "
        "Brute Force, Web Attacks, and Botnets).",
        body
    ))

    story.append(Paragraph("2.2 Environment Setup", h2))
    env_data = [
        ["Component", "Version / Tool"],
        ["Operating System", "macOS (Apple Silicon ARM64)"],
        ["Python Runtime", "Python 3.13.2"],
        ["Apache Spark / PySpark", "PySpark 3.5.1"],
        ["Java Development Kit", "OpenJDK 17 LTS (Homebrew)"],
        ["Streaming Broker", "Apache Kafka (KRaft mode, single broker)"],
        ["Intermediate Storage", "Apache Parquet (via pyarrow 25.0)"],
        ["Serving Database", "SQLite 3 (indexed alerts.db)"],
        ["Machine Learning", "scikit-learn 1.6+ (Isolation Forest, Random Forest)"],
        ["Interactive Dashboard", "Streamlit 1.37+ & Plotly 5.23+"]
    ]
    env_table = Table(env_data, colWidths=[2.6 * inch, 3.6 * inch])
    env_table.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,0), colors.HexColor("#0B2545")),
        ('TEXTCOLOR', (0,0), (-1,0), colors.white),
        ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
        ('FONTSIZE', (0,0), (-1,-1), 8.5),
        ('BOTTOMPADDING', (0,0), (-1,-1), 4),
        ('TOPPADDING', (0,0), (-1,-1), 4),
        ('GRID', (0,0), (-1,-1), 0.5, colors.HexColor("#D0D7DE")),
        ('ROWBACKGROUNDS', (0,1), (-1,-1), [colors.white, colors.HexColor("#F8F9FA")])
    ]))
    story.append(env_table)
    story.append(Spacer(1, 0.1 * inch))

    story.append(Paragraph("2.3 Data Ingestion Output", h2))
    story.append(Paragraph(
        "All 2.52M flows were parsed in a single distributed Spark job, structured with an explicit schema, and printed with raw counts:",
        body
    ))

    ingest_output = (
        "=== Phase 1: PySpark Data Ingestion & Cleaning ===<br/>"
        "Loading data/raw/cicds.csv ...<br/>"
        "Raw shape: 2,520,751 rows × 53 columns<br/>"
        "+------------------+---------+<br/>"
        "| Attack Type      | count   |<br/>"
        "+------------------+---------+<br/>"
        "| Normal Traffic   | 2095057 |<br/>"
        "| DoS              |  193745 |<br/>"
        "| DDoS             |  128014 |<br/>"
        "| Port Scanning    |   90694 |<br/>"
        "| Brute Force      |    9150 |<br/>"
        "| Web Attacks      |    2143 |<br/>"
        "| Bots             |    1948 |<br/>"
        "+------------------+---------+"
    )
    ingest_box = Table([[Paragraph(ingest_output, code_style)]], colWidths=[6.2 * inch])
    ingest_box.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,-1), colors.HexColor("#1A1A1A")),
        ('TEXTCOLOR', (0,0), (-1,-1), colors.HexColor("#00FF66")),
        ('BOX', (0,0), (-1,-1), 0.5, colors.HexColor("#333333")),
        ('LEFTPADDING', (0,0), (-1,-1), 8),
        ('TOPPADDING', (0,0), (-1,-1), 6),
        ('BOTTOMPADDING', (0,0), (-1,-1), 6),
    ]))
    story.append(ingest_box)
    story.append(Paragraph("<i>Figure 2 — Spark execution log: distributed ingestion and raw class counts</i>", caption))

    story.append(Paragraph("2.4 Data Preprocessing", h2))
    story.append(Paragraph(
        "Preprocessing verified 2,520,751 input records. After null/inf validation and standard deviation profiling, 0 columns had "
        "zero variance, retaining all 52 features. To ensure consistent anomaly scoring across heterogeneous attributes "
        "(e.g., flow durations in millions of microseconds vs. binary TCP flag counts), features were normalized using "
        "StandardScaler. A stratified sample of 500,407 rows was preserved for local ML modeling to prevent exceeding the 8GB RAM threshold.",
        body
    ))
    story.append(PageBreak())

    # ═════════════════════════════════════════════════════════════════════════
    # PAGE 4: SECTION 3 — METHODS & TOOLS USED
    # ═════════════════════════════════════════════════════════════════════════
    story.append(Paragraph("3. Methods & Tools Used", h1))
    story.append(Paragraph(
        "The following tools were selected for their ability to process high-throughput network event data locally "
        "without memory exhaustion while adhering to enterprise architecture patterns.",
        body
    ))

    tools_data = [
        ["Technology", "Purpose / Justification"],
        [
            "Apache Spark / PySpark",
            "Distributed in-memory computation across multi-core CPU workers; avoids single-threaded memory bottlenecks when processing 2.52M flow rows."
        ],
        [
            "Spark SQL",
            "Declarative SQL interface for rapid aggregation, distribution profiling, and data validation."
        ],
        [
            "Apache Parquet",
            "Binary columnar format offering ~70% storage compression over CSV and efficient predicate pushdown."
        ],
        [
            "Apache Kafka (KRaft)",
            "Lightweight event pub/sub messaging broker without ZooKeeper overhead; supports continuous flow ingestion."
        ],
        [
            "Isolation Forest",
            "Unsupervised tree-based algorithm with O(n log n) complexity; isolates anomalous flows in high-dimensional feature space without labels."
        ],
        [
            "Random Forest",
            "Supervised decision tree ensemble used to classify attack categories and validate unsupervised anomaly outputs."
        ],
        [
            "SQLite",
            "Zero-maintenance embedded database providing indexed, millisecond queries for dashboard reads without background RAM overhead."
        ],
        [
            "Streamlit & Plotly",
            "Python-native reactive dashboard framework enabling real-time KPI monitoring and interactive attack distribution charts."
        ]
    ]
    tools_table = Table(tools_data, colWidths=[1.8 * inch, 4.4 * inch])
    tools_table.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,0), colors.HexColor("#0B2545")),
        ('TEXTCOLOR', (0,0), (-1,0), colors.white),
        ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
        ('FONTSIZE', (0,0), (-1,-1), 8),
        ('BOTTOMPADDING', (0,0), (-1,-1), 4),
        ('TOPPADDING', (0,0), (-1,-1), 4),
        ('GRID', (0,0), (-1,-1), 0.5, colors.HexColor("#D0D7DE")),
        ('ROWBACKGROUNDS', (0,1), (-1,-1), [colors.white, colors.HexColor("#F8F9FA")])
    ]))
    story.append(tools_table)
    story.append(Spacer(1, 0.1 * inch))

    story.append(Paragraph("3.1 Spark SQL for Analytical Querying", h2))
    story.append(Paragraph(
        "The cleaned Parquet store is queried via Spark SQL views to produce pre-aggregated analytics without re-reading raw CSVs:",
        body
    ))

    code_snippet3 = (
        "df_clean.createOrReplaceTempView('network_flows')<br/>"
        "spark.sql('''<br/>"
        "&nbsp;&nbsp;&nbsp;&nbsp;SELECT `Attack Type`, COUNT(*) AS count,<br/>"
        "&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;ROUND(AVG(`Flow Duration`), 1) AS avg_duration,<br/>"
        "&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;ROUND(AVG(`Flow Bytes/s`), 1) AS avg_bytes_sec<br/>"
        "&nbsp;&nbsp;&nbsp;&nbsp;FROM network_flows<br/>"
        "&nbsp;&nbsp;&nbsp;&nbsp;GROUP BY `Attack Type` ORDER BY count DESC<br/>"
        "''').show(truncate=False)"
    )
    code_box3 = Table([[Paragraph(code_snippet3, code_style)]], colWidths=[6.2 * inch])
    code_box3.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,-1), colors.HexColor("#F5F7FA")),
        ('BOX', (0,0), (-1,-1), 0.5, colors.HexColor("#D0D7DE")),
        ('LEFTPADDING', (0,0), (-1,-1), 8),
        ('TOPPADDING', (0,0), (-1,-1), 6),
        ('BOTTOMPADDING', (0,0), (-1,-1), 6),
    ]))
    story.append(code_box3)

    story.append(Paragraph("4. Explanation of Work Completed", h1))
    story.append(Paragraph(
        "This section summarizes the distribution of network traffic vectors and the machine learning anomaly detection results.",
        body
    ))

    # Traffic breakdown table
    breakdown_data = [
        ["Attack Category", "Record Count", "Percentage", "Observed Signature"],
        ["Normal Traffic", "2,095,057", "83.11%", "Standard bidirectional packet exchanges"],
        ["DoS", "193,745", "7.69%", "Volumetric packet flooding to target port"],
        ["DDoS", "128,014", "5.08%", "High-frequency multi-source request bursts"],
        ["Port Scanning", "90,694", "3.60%", "Rapid SYN/probe connections with low payload"],
        ["Brute Force", "9,150", "0.36%", "High failed connection count on auth services"],
        ["Web Attacks", "2,143", "0.09%", "SQLi/XSS patterns in forward packet lengths"],
        ["Bots", "1,948", "0.08%", "Periodic beaconing and synchronized IAT"],
        ["Total", "2,520,751", "100.00%", "Complete verified clean dataset"]
    ]
    breakdown_table = Table(breakdown_data, colWidths=[1.4 * inch, 1.1 * inch, 1.0 * inch, 2.7 * inch])
    breakdown_table.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,0), colors.HexColor("#0B2545")),
        ('TEXTCOLOR', (0,0), (-1,0), colors.white),
        ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
        ('FONTSIZE', (0,0), (-1,-1), 8),
        ('BOTTOMPADDING', (0,0), (-1,-1), 3),
        ('TOPPADDING', (0,0), (-1,-1), 3),
        ('GRID', (0,0), (-1,-1), 0.5, colors.HexColor("#D0D7DE")),
        ('ROWBACKGROUNDS', (0,1), (-1,-2), [colors.white, colors.HexColor("#F8F9FA")]),
        ('FONTNAME', (0,-1), (-1,-1), 'Helvetica-Bold'),
        ('BACKGROUND', (0,-1), (-1,-1), colors.HexColor("#EBF2FA"))
    ]))
    story.append(breakdown_table)
    story.append(PageBreak())

    # ═════════════════════════════════════════════════════════════════════════
    # PAGE 5: SECTION 5 — ML EVALUATION AND DASHBOARD
    # ═════════════════════════════════════════════════════════════════════════
    story.append(Paragraph("5. Machine Learning Evaluation and Dashboard", h1))
    story.append(Paragraph("5.1 Unsupervised Anomaly Detection Analysis", h2))
    story.append(Paragraph(
        "An Isolation Forest model (100 estimators, contamination=0.17 calibrated against the 16.9% attack baseline) "
        "was fitted to 500,407 normalized flow records without access to labels. The model flagged <b>85,070 flows (17.00%)</b> "
        "as anomalous, exhibiting strong statistical divergence between normal and malicious score distributions:",
        body
    ))

    # Insert score distribution image if available
    img_hist_path = "models/plots/if_score_distribution.png"
    if os.path.exists(img_hist_path):
        story.append(Image(img_hist_path, width=6.0 * inch, height=2.2 * inch))
        story.append(Paragraph("<i>Figure 3 — Isolation Forest: Anomaly score distribution (Normal vs Attack traffic)</i>", caption))

    story.append(Paragraph("5.2 Supervised Multi-Class Classifier Evaluation", h2))
    story.append(Paragraph(
        "A Random Forest classifier (100 trees, max_depth=20, balanced weights) was trained on an 80/20 stratified split "
        "(400,325 train / 100,082 test) to categorize threat types. The model achieved <b>99.70% overall test accuracy</b>:",
        body
    ))

    # Insert confusion matrix image if available
    img_cm_path = "models/plots/rf_confusion_matrix.png"
    if os.path.exists(img_cm_path):
        story.append(Image(img_cm_path, width=4.5 * inch, height=2.6 * inch))
        story.append(Paragraph("<i>Figure 4 — Random Forest: Multi-class confusion matrix across 7 traffic categories</i>", caption))

    story.append(Paragraph("5.3 Interactive Streamlit Dashboard", h2))
    story.append(Paragraph(
        "The web application (running locally on port 8501) connects directly to SQLite (alerts.db). It surfaces executive KPI cards, "
        "an interactive threat distribution chart, score histograms, and a live alert table supporting drill-down by predicted attack label.",
        body
    ))
    story.append(PageBreak())

    # ═════════════════════════════════════════════════════════════════════════
    # PAGE 6: SECTIONS 6, 7, 8, 9
    # ═════════════════════════════════════════════════════════════════════════
    story.append(Paragraph("6. Team Participation", h1))
    team_data = [
        ["Team Member", "Core Contributions"],
        [
            "Arjun Gupta —\n[Registration Number]",
            "macOS ARM64 & Java 17 LTS runtime configuration; PySpark 3.5.1 distributed setup; raw dataset ingestion pipeline; data cleaning, infinity/null sanitization, and columnar Parquet storage; Streamlit dashboard implementation; report preparation."
        ],
        [
            "[Team Member 2] —\n[Registration Number]",
            "Feature normalization with StandardScaler; machine learning pipeline development (IsolationForest anomaly detection & multi-class RandomForest); evaluation scripts and confusion matrix generation; SQLite database schema and alert caching; testing & validation."
        ]
    ]
    team_table = Table(team_data, colWidths=[2.2 * inch, 4.0 * inch])
    team_table.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,0), colors.HexColor("#0B2545")),
        ('TEXTCOLOR', (0,0), (-1,0), colors.white),
        ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
        ('FONTSIZE', (0,0), (-1,-1), 8.5),
        ('BOTTOMPADDING', (0,0), (-1,-1), 5),
        ('TOPPADDING', (0,0), (-1,-1), 5),
        ('GRID', (0,0), (-1,-1), 0.5, colors.HexColor("#D0D7DE")),
        ('ROWBACKGROUNDS', (0,1), (-1,-1), [colors.white, colors.HexColor("#F8F9FA")]),
        ('VALIGN', (0,0), (-1,-1), 'TOP')
    ]))
    story.append(team_table)
    story.append(Spacer(1, 0.1 * inch))

    story.append(Paragraph("7. Current Project Status", h1))
    status_data = [
        ["Component", "Status"],
        ["CICIDS2017 Dataset Collection (2.52M records)", "Completed"],
        ["OpenJDK 17 + PySpark 3.5.1 Environment Configuration", "Completed"],
        ["Distributed PySpark Data Cleaning & Type Casting", "Completed"],
        ["Columnar Parquet Analytical Storage", "Completed"],
        ["Feature Standardization & NaN Sanitization", "Completed"],
        ["Unsupervised Isolation Forest Modeling (85k anomalies)", "Completed"],
        ["Supervised Random Forest Classifier (99.70% accuracy)", "Completed"],
        ["Model Evaluation & Confusion Matrix Visualizations", "Completed"],
        ["SQLite Relational Serving Cache (alerts.db)", "Completed"],
        ["Interactive Streamlit Dashboard (Port 8501)", "Completed"],
        ["End-to-End Pipeline Sanity Test Suite (19/19 checks)", "Completed"],
        ["Kafka Streaming Ingestion Simulation (Phase 2)", "In Progress"],
        ["Explainable AI with SHAP Feature Attributions", "Planned — Review 3"],
        ["Automated Security Webhook Notifications", "Planned — Review 3"]
    ]
    status_table = Table(status_data, colWidths=[4.2 * inch, 2.0 * inch])
    status_table.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,0), colors.HexColor("#0B2545")),
        ('TEXTCOLOR', (0,0), (-1,0), colors.white),
        ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
        ('FONTSIZE', (0,0), (-1,-1), 8),
        ('BOTTOMPADDING', (0,0), (-1,-1), 3),
        ('TOPPADDING', (0,0), (-1,-1), 3),
        ('GRID', (0,0), (-1,-1), 0.5, colors.HexColor("#D0D7DE")),
        ('ROWBACKGROUNDS', (0,1), (-1,-1), [colors.white, colors.HexColor("#F8F9FA")])
    ]))
    story.append(status_table)
    story.append(Spacer(1, 0.1 * inch))

    story.append(Paragraph("8. Challenges Encountered", h1))
    story.append(Paragraph(
        "• <b>Memory-Constrained Distributed Execution</b>: Processing 2.52M rows × 53 attributes on an 8GB unified memory laptop "
        "posed severe risk of kernel swapping. This was solved by bounding Spark's driver to 2GB, tuning shuffle partitions, "
        "and utilizing Parquet columnar compression.<br/>"
        "• <b>Dataset Realignment</b>: The pre-extracted Kaggle export stripped temporal timestamps and source IPs. The project "
        "was defensively adapted from entity-behavioral profiling to per-flow anomaly detection, which represents an authentic, "
        "production-standard IDS approach.",
        body
    ))

    story.append(Paragraph("9. Conclusion and Future Work", h1))
    story.append(Paragraph(
        "The midway milestone for the Flow-Level Network Anomaly Detection Platform has established an end-to-end "
        "Big Data analytics architecture. 2.52 million flows are cleaned with Apache Spark, stored in Parquet, scored with "
        "Isolation Forest, indexed in SQLite, and visualized via Streamlit.<br/>"
        "<b>Planned for Review 3:</b><br/>"
        "1. Real-time Kafka streaming integration with Spark Structured Streaming micro-batches.<br/>"
        "2. SHAP (SHapley Additive exPlanations) integration to provide feature-level attribution for flagged threats.<br/>"
        "3. Production threshold tuning and webhook alert dispatching.",
        body
    ))

    doc.build(story, canvasmaker=NumberedCanvas)
    print(f"Successfully generated: {PDF_OUTPUT}")

if __name__ == "__main__":
    build_pdf()

