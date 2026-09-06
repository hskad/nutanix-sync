import os
from pptx import Presentation
from pptx.util import Inches, Pt
from pptx.enum.text import PP_ALIGN
from pptx.dml.color import RGBColor
from pptx.enum.shapes import MSO_SHAPE

def create_presentation():
    prs = Presentation()
    prs.slide_width = Inches(13.333)
    prs.slide_height = Inches(7.5)  # 16:9 Widescreen

    # Color Palette: Minimalist Dark Editorial (2 Neutrals + 1 Sharp Accent)
    BG_COLOR = RGBColor(12, 13, 16)         # Deep Ink / Off-Black
    PANEL_BG = RGBColor(19, 22, 28)         # Subtle Surface
    TEXT_WHITE = RGBColor(245, 246, 248)     # Crisp White
    TEXT_MUTED = RGBColor(139, 146, 158)     # Architectural Grey
    ACCENT_SHARP = RGBColor(255, 59, 0)      # Signal Vermillion
    BORDER_COLOR = RGBColor(34, 38, 48)      # 1px Hairline

    def apply_background(slide):
        background = slide.shapes.add_shape(MSO_SHAPE.RECTANGLE, 0, 0, prs.slide_width, prs.slide_height)
        background.fill.solid()
        background.fill.fore_color.rgb = BG_COLOR
        background.line.fill.background()
        return background

    def add_header(slide, tag_text, title_text):
        # Category Tag
        tag_box = slide.shapes.add_textbox(Inches(0.8), Inches(0.5), Inches(11.7), Inches(0.4))
        tf_tag = tag_box.text_frame
        tf_tag.word_wrap = True
        p_tag = tf_tag.paragraphs[0]
        p_tag.text = tag_text.upper()
        p_tag.font.size = Pt(11)
        p_tag.font.bold = True
        p_tag.font.color.rgb = ACCENT_SHARP
        p_tag.font.name = "Arial"

        # Main Title
        title_box = slide.shapes.add_textbox(Inches(0.8), Inches(0.85), Inches(11.7), Inches(0.8))
        tf_title = title_box.text_frame
        tf_title.word_wrap = True
        p_title = tf_title.paragraphs[0]
        p_title.text = title_text
        p_title.font.size = Pt(28)
        p_title.font.bold = True
        p_title.font.color.rgb = TEXT_WHITE
        p_title.font.name = "Arial"

        # Divider line
        line = slide.shapes.add_shape(MSO_SHAPE.RECTANGLE, Inches(0.8), Inches(1.75), Inches(11.733), Inches(0.02))
        line.fill.solid()
        line.fill.fore_color.rgb = BORDER_COLOR
        line.line.fill.background()

    # ==========================================
    # SLIDE 1: TITLE SLIDE
    # ==========================================
    slide_layout = prs.slide_layouts[6]  # Blank
    s1 = prs.slides.add_slide(slide_layout)
    apply_background(s1)

    # Folio Tag
    box = s1.shapes.add_textbox(Inches(1.2), Inches(1.8), Inches(10), Inches(0.4))
    tf = box.text_frame
    p = tf.paragraphs[0]
    p.text = "SYS.SPEC // 02 — DISTRIBUTED RECONCILIATION"
    p.font.size = Pt(12)
    p.font.bold = True
    p.font.color.rgb = ACCENT_SHARP
    p.font.name = "Arial"

    # Main Title
    box = s1.shapes.add_textbox(Inches(1.2), Inches(2.2), Inches(11), Inches(1.5))
    tf = box.text_frame
    p = tf.paragraphs[0]
    p.text = "Nutanix—Sync"
    p.font.size = Pt(56)
    p.font.bold = True
    p.font.color.rgb = TEXT_WHITE
    p.font.name = "Georgia"

    # Subtitle
    box = s1.shapes.add_textbox(Inches(1.2), Inches(3.7), Inches(10.5), Inches(1.0))
    tf = box.text_frame
    p = tf.paragraphs[0]
    p.text = "Distributed Cluster File Synchronization Fabric"
    p.font.size = Pt(22)
    p.font.color.rgb = TEXT_WHITE
    p.font.name = "Arial"

    p2 = tf.add_paragraph()
    p2.text = "Scaling from 2-Machine Baseline to 100+ Nodes via Merkle Swarms & Epidemic Gossip"
    p2.font.size = Pt(14)
    p2.font.color.rgb = TEXT_MUTED
    p2.font.name = "Arial"

    # Footer Metadata
    box = s1.shapes.add_textbox(Inches(1.2), Inches(5.8), Inches(10), Inches(0.6))
    tf = box.text_frame
    p = tf.paragraphs[0]
    p.text = "Nutanix Hackathon 2026  •  IIT Guwahati  •  Topic 2: Multi-Machine Cluster File Sync"
    p.font.size = Pt(11)
    p.font.color.rgb = TEXT_MUTED
    p.font.name = "Arial"

    # ==========================================
    # SLIDE 2: THE PROBLEM
    # ==========================================
    s2 = prs.slides.add_slide(slide_layout)
    apply_background(s2)
    add_header(s2, "The Challenge at Enterprise Scale", "Why Naive Cluster Syncing Breaks at 100+ Machines")

    problems = [
        ("01 / Egress Saturation", "Modifying 100 bytes in a 100MB database or log file re-transmits the entire 100MB over the wire, wasting 99.9% network bandwidth."),
        ("02 / O(N²) Broadcast Storms", "Centralized master-replica sync turns publisher nodes into single points of failure (SPOF) and collapses cluster network switch capacity."),
        ("03 / Silent Data Corruption", "Asynchronous network partitions result in concurrent writes. Standard sync tools perform destructive Last-Writer-Wins overwrites.")
    ]

    for i, (head, desc) in enumerate(problems):
        x = Inches(0.8 + i * 4.0)
        # Background card
        card = s2.shapes.add_shape(MSO_SHAPE.RECTANGLE, x, Inches(2.2), Inches(3.7), Inches(4.2))
        card.fill.solid()
        card.fill.fore_color.rgb = PANEL_BG
        card.line.color.rgb = BORDER_COLOR

        tb = s2.shapes.add_textbox(x + Inches(0.3), Inches(2.5), Inches(3.1), Inches(3.6))
        tf = tb.text_frame
        tf.word_wrap = True
        
        p = tf.paragraphs[0]
        p.text = head
        p.font.size = Pt(16)
        p.font.bold = True
        p.font.color.rgb = ACCENT_SHARP
        p.font.name = "Arial"

        p_desc = tf.add_paragraph()
        p_desc.text = f"\n{desc}"
        p_desc.font.size = Pt(13)
        p_desc.font.color.rgb = TEXT_MUTED
        p_desc.font.name = "Arial"

    # ==========================================
    # SLIDE 3: ARCHITECTURE & 4 PILLARS
    # ==========================================
    s3 = prs.slides.add_slide(slide_layout)
    apply_background(s3)
    add_header(s3, "Architecture & Engineering Pillars", "Four Core Innovations Powering Nutanix-Sync")

    pillars = [
        ("1. Content Chunking & Merkle Trees", "Files sliced into 64KB chunks. Hierarchical directory hash trees isolate changed bytes in O(log M) time."),
        ("2. SWIM Epidemic Gossip Protocol", "Decentralized membership discovery & failure detector. Reaches cluster-wide consistency in O(log N) rounds."),
        ("3. P2P Swarm Chunk Distribution", "BitTorrent-inspired swarm distribution pulls chunks concurrently across peers holding blocks, eliminating single-publisher bottlenecks."),
        ("4. Vector Clock Causality Engine", "Tracks causal partial ordering. Flags split-brain conflicts and preserves non-destructive branches with zero data loss.")
    ]

    for i, (head, desc) in enumerate(pillars):
        row = i // 2
        col = i % 2
        x = Inches(0.8 + col * 6.0)
        y = Inches(2.2 + row * 2.3)

        card = s3.shapes.add_shape(MSO_SHAPE.RECTANGLE, x, y, Inches(5.7), Inches(2.0))
        card.fill.solid()
        card.fill.fore_color.rgb = PANEL_BG
        card.line.color.rgb = BORDER_COLOR

        tb = s3.shapes.add_textbox(x + Inches(0.3), y + Inches(0.25), Inches(5.1), Inches(1.5))
        tf = tb.text_frame
        tf.word_wrap = True

        p = tf.paragraphs[0]
        p.text = head
        p.font.size = Pt(15)
        p.font.bold = True
        p.font.color.rgb = TEXT_WHITE
        p.font.name = "Arial"

        p_desc = tf.add_paragraph()
        p_desc.text = desc
        p_desc.font.size = Pt(12)
        p_desc.font.color.rgb = TEXT_MUTED
        p_desc.font.name = "Arial"

    # ==========================================
    # SLIDE 4: BENCHMARK SCORECARD
    # ==========================================
    s4 = prs.slides.add_slide(slide_layout)
    apply_background(s4)
    add_header(s4, "Physical Performance Verification", "Quantitative Benchmark: 97.9% Bandwidth Reduction")

    # Table
    rows = 6
    cols = 3
    left = Inches(0.8)
    top = Inches(2.2)
    width = Inches(8.5)
    height = Inches(4.2)

    table_shape = s4.shapes.add_table(rows, cols, left, top, width, height)
    table = table_shape.table
    table.columns[0].width = Inches(3.2)
    table.columns[1].width = Inches(2.5)
    table.columns[2].width = Inches(2.8)

    data = [
        ("Benchmark Metric", "Naive Full Sync", "Nutanix-Sync Delta Sync"),
        ("Total File Size", "3.00 MB", "3.00 MB"),
        ("Modified Payload", "—", "58 Bytes"),
        ("Wire Traffic Egress", "128.00 KB", "64.00 KB (1 chunk)"),
        ("Bandwidth Saved", "0 B (0.0%)", "2.94 MB (97.92% saved)"),
        ("Data Integrity Verification", "Passed", "Bit-for-Bit SHA-256 Match")
    ]

    for r_idx, row_data in enumerate(data):
        for c_idx, cell_value in enumerate(row_data):
            cell = table.cell(r_idx, c_idx)
            cell.text = cell_value
            cell.fill.solid()
            cell.fill.fore_color.rgb = PANEL_BG if r_idx > 0 else BORDER_COLOR
            p = cell.text_frame.paragraphs[0]
            p.font.size = Pt(12) if r_idx > 0 else Pt(13)
            p.font.bold = (r_idx == 0 or c_idx == 2)
            p.font.color.rgb = ACCENT_SHARP if (r_idx > 0 and c_idx == 2) else TEXT_WHITE
            p.font.name = "Arial"

    # Right callout panel
    callout = s4.shapes.add_shape(MSO_SHAPE.RECTANGLE, Inches(9.6), Inches(2.2), Inches(2.9), Inches(4.2))
    callout.fill.solid()
    callout.fill.fore_color.rgb = PANEL_BG
    callout.line.color.rgb = ACCENT_SHARP

    tb = s4.shapes.add_textbox(Inches(9.8), Inches(2.4), Inches(2.5), Inches(3.8))
    tf = tb.text_frame
    tf.word_wrap = True
    p = tf.paragraphs[0]
    p.text = "TAKEAWAY"
    p.font.size = Pt(12)
    p.font.bold = True
    p.font.color.rgb = ACCENT_SHARP

    p2 = tf.add_paragraph()
    p2.text = "\nBy decomposing files into 64KB content blocks, modifying a line inside a large file transfers ONLY the single modified chunk.\n\nResults scale to 100MB+ enterprise datasets with >99% wire reduction."
    p2.font.size = Pt(12)
    p2.font.color.rgb = TEXT_WHITE

    # ==========================================
    # SLIDE 5: DUAL VERIFICATION STRATEGY
    # ==========================================
    s5 = prs.slides.add_slide(slide_layout)
    apply_background(s5)
    add_header(s5, "Demonstration & Verification Strategy", "Ground-Truth Physical Sync + Cluster Telemetry Console")

    demos = [
        ("Part 1: Physical 2-Node Live Sync", 
         "• Real OS filesystem watcher (`watchdog`) monitoring disk.\n• Sub-50ms propagation between physical folders (`folder_a` → `folder_b`).\n• Interactive Terminal TUI displaying live gossip peers, chunks, and vector clocks in real time.\n• Standalone benchmark script (`benchmark_delta.py`) measuring disk I/O."),
        ("Part 2: Scalable Cluster Telemetry (50+ Nodes)", 
         "• Interactive architectural schematic canvas showing decentralized gossip mesh.\n• Real-time Merkle consistency verification across 50 simulated cluster nodes.\n• Non-destructive split-brain conflict isolation under network partition.\n• Chaos fault injection: severed node recovery and catch-up sync.")
    ]

    for i, (title, content) in enumerate(demos):
        x = Inches(0.8 + i * 6.0)
        card = s5.shapes.add_shape(MSO_SHAPE.RECTANGLE, x, Inches(2.2), Inches(5.7), Inches(4.2))
        card.fill.solid()
        card.fill.fore_color.rgb = PANEL_BG
        card.line.color.rgb = BORDER_COLOR

        tb = s5.shapes.add_textbox(x + Inches(0.3), Inches(2.4), Inches(5.1), Inches(3.8))
        tf = tb.text_frame
        tf.word_wrap = True

        p = tf.paragraphs[0]
        p.text = title
        p.font.size = Pt(16)
        p.font.bold = True
        p.font.color.rgb = ACCENT_SHARP if i == 0 else TEXT_WHITE
        p.font.name = "Arial"

        p_desc = tf.add_paragraph()
        p_desc.text = f"\n{content}"
        p_desc.font.size = Pt(13)
        p_desc.font.color.rgb = TEXT_WHITE
        p_desc.font.name = "Arial"

    # ==========================================
    # SLIDE 6: CONCLUSION & ENTERPRISE IMPACT
    # ==========================================
    s6 = prs.slides.add_slide(slide_layout)
    apply_background(s6)
    add_header(s6, "Summary & Enterprise Impact", "Hyperconverged File Resilience with Enterprise Performance")

    points = [
        ("Alignment with Nutanix AOS & Files", "Solves the core distributed storage challenge: bandwidth-efficient edge-to-core replication and multi-datacenter data consistency without single points of failure."),
        ("Verified & Fully Tested", "7 automated test suites execute in <7 seconds, confirming unit chunking, Vector Clock partial ordering, and 20-node cluster convergence."),
        ("Open-Source Codebase", "Completely open-source and reproducible with full documentation, tests, and CLI daemons at https://github.com/hskad/nutanix-sync.")
    ]

    for i, (head, desc) in enumerate(points):
        y = Inches(2.2 + i * 1.4)
        card = s6.shapes.add_shape(MSO_SHAPE.RECTANGLE, Inches(0.8), y, Inches(11.733), Inches(1.2))
        card.fill.solid()
        card.fill.fore_color.rgb = PANEL_BG
        card.line.color.rgb = BORDER_COLOR

        tb = s6.shapes.add_textbox(Inches(1.1), y + Inches(0.15), Inches(11.1), Inches(0.9))
        tf = tb.text_frame
        tf.word_wrap = True

        p = tf.paragraphs[0]
        p.text = head
        p.font.size = Pt(14)
        p.font.bold = True
        p.font.color.rgb = ACCENT_SHARP
        p.font.name = "Arial"

        p_desc = tf.add_paragraph()
        p_desc.text = desc
        p_desc.font.size = Pt(12)
        p_desc.font.color.rgb = TEXT_WHITE
        p_desc.font.name = "Arial"

    output_path = os.path.abspath("Nutanix_Sync_Presentation.pptx")
    prs.save(output_path)
    print(f"Presentation saved successfully to: {output_path}")

if __name__ == "__main__":
    create_presentation()
