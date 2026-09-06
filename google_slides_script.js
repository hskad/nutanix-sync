/**
 * Google Apps Script to auto-generate the Nutanix-Sync Presentation directly in Google Slides.
 * 
 * Instructions:
 * 1. Open a new Google Slides deck at: https://slides.new
 * 2. Click on: Extensions -> Apps Script
 * 3. Delete any code in the editor, paste this entire script, and click "Run" (run createNutanixDeck).
 * 4. All 6 professional widescreen slides will be instantly generated in your Google Slides deck!
 */

function createNutanixDeck() {
  const deck = SlidesApp.getActivePresentation();
  
  // Set 16:9 widescreen page size
  deck.setPageDimensions(960, 540); // 16:9 points

  // Colors
  const BG_COLOR = '#0C0D10';
  const PANEL_BG = '#13161C';
  const TEXT_WHITE = '#F5F6F8';
  const TEXT_MUTED = '#8B929E';
  const ACCENT = '#FF3B00';

  // Helper: Style Slide Background
  function prepSlide(slide) {
    slide.getBackground().setSolidFill(BG_COLOR);
  }

  // -------------------------------------------------------------
  // SLIDE 1: Title Slide
  // -------------------------------------------------------------
  const s1 = deck.getSlides()[0] || deck.appendSlide();
  prepSlide(s1);

  const tag1 = s1.insertTextBox("SYS.SPEC // 02 — DISTRIBUTED RECONCILIATION", 60, 110, 800, 30);
  tag1.getText().getTextStyle().setFontFamily("Arial").setFontSize(11).setBold(true).setForegroundColor(ACCENT);

  const title1 = s1.insertTextBox("Nutanix—Sync", 60, 140, 800, 90);
  title1.getText().getTextStyle().setFontFamily("Georgia").setFontSize(54).setBold(true).setForegroundColor(TEXT_WHITE);

  const sub1 = s1.insertTextBox("Distributed Cluster File Synchronization Fabric\nScaling from 2-Machine Baseline to 100+ Nodes via Merkle Swarms & Epidemic Gossip", 60, 240, 800, 70);
  sub1.getText().getTextStyle().setFontFamily("Arial").setFontSize(16).setForegroundColor(TEXT_MUTED);

  const foot1 = s1.insertTextBox("Nutanix Hackathon 2026  •  IIT Guwahati  •  Topic 2: Multi-Machine Cluster File Sync", 60, 440, 800, 30);
  foot1.getText().getTextStyle().setFontFamily("Arial").setFontSize(11).setForegroundColor(TEXT_MUTED);

  // -------------------------------------------------------------
  // SLIDE 2: The Problem
  // -------------------------------------------------------------
  const s2 = deck.appendSlide();
  prepSlide(s2);

  const s2_tag = s2.insertTextBox("THE CHALLENGE AT ENTERPRISE SCALE", 60, 30, 800, 25);
  s2_tag.getText().getTextStyle().setFontFamily("Arial").setFontSize(10).setBold(true).setForegroundColor(ACCENT);

  const s2_title = s2.insertTextBox("Why Naive Cluster Syncing Breaks at 100+ Machines", 60, 50, 800, 50);
  s2_title.getText().getTextStyle().setFontFamily("Arial").setFontSize(22).setBold(true).setForegroundColor(TEXT_WHITE);

  const problems = [
    { head: "01 / Egress Saturation", desc: "Modifying 100 bytes in a 100MB file usually re-transmits the entire 100MB over the wire, wasting 99.9% network bandwidth." },
    { head: "02 / O(N²) Broadcast Storms", desc: "Centralized master-replica sync turns publishers into single points of failure (SPOF) and collapses cluster switch capacity." },
    { head: "03 / Silent Data Loss", desc: "Asynchronous network partitions cause concurrent writes. Standard sync tools perform destructive Last-Writer-Wins overwrites." }
  ];

  problems.forEach((p, i) => {
    const card = s2.insertShape(SlidesApp.ShapeType.RECTANGLE, 60 + i * 280, 120, 260, 340);
    card.getFill().setSolidFill(PANEL_BG);
    card.getBorder().getLineFill().setSolidFill('#222630');

    const tb = s2.insertTextBox(p.head + "\n\n" + p.desc, 75 + i * 280, 140, 230, 300);
    const text = tb.getText();
    text.getTextStyle().setFontFamily("Arial").setForegroundColor(TEXT_MUTED).setFontSize(12);
    text.getParagraphs()[0].getRange().getTextStyle().setFontSize(14).setBold(true).setForegroundColor(ACCENT);
  });

  // -------------------------------------------------------------
  // SLIDE 3: Architecture & 4 Pillars
  // -------------------------------------------------------------
  const s3 = deck.appendSlide();
  prepSlide(s3);

  const s3_tag = s3.insertTextBox("ARCHITECTURE & ENGINEERING PILLARS", 60, 30, 800, 25);
  s3_tag.getText().getTextStyle().setFontFamily("Arial").setFontSize(10).setBold(true).setForegroundColor(ACCENT);

  const s3_title = s3.insertTextBox("Four Core Innovations Powering Nutanix-Sync", 60, 50, 800, 50);
  s3_title.getText().getTextStyle().setFontFamily("Arial").setFontSize(22).setBold(true).setForegroundColor(TEXT_WHITE);

  const pillars = [
    { title: "1. Content Chunking & Merkle Trees", desc: "Files are sliced into 64KB blocks. Hierarchical directory hash trees isolate changed bytes in O(log M) time." },
    { title: "2. SWIM Epidemic Gossip Protocol", desc: "Decentralized membership discovery & failure detector. Reaches cluster-wide consistency in O(log N) rounds." },
    { title: "3. P2P Swarm Chunk Distribution", desc: "BitTorrent-inspired swarm distribution pulls chunks concurrently across peers holding blocks, eliminating single-publisher bottlenecks." },
    { title: "4. Vector Clock Causality Engine", desc: "Tracks causal partial ordering. Flags split-brain conflicts and preserves non-destructive branches with zero data loss." }
  ];

  pillars.forEach((pil, i) => {
    const row = Math.floor(i / 2);
    const col = i % 2;
    const card = s3.insertShape(SlidesApp.ShapeType.RECTANGLE, 60 + col * 430, 120 + row * 170, 410, 150);
    card.getFill().setSolidFill(PANEL_BG);
    card.getBorder().getLineFill().setSolidFill('#222630');

    const tb = s3.insertTextBox(pil.title + "\n\n" + pil.desc, 75 + col * 430, 130 + row * 170, 380, 130);
    const text = tb.getText();
    text.getTextStyle().setFontFamily("Arial").setForegroundColor(TEXT_MUTED).setFontSize(11);
    text.getParagraphs()[0].getRange().getTextStyle().setFontSize(13).setBold(true).setForegroundColor(TEXT_WHITE);
  });

  // -------------------------------------------------------------
  // SLIDE 4: Benchmark Scorecard
  // -------------------------------------------------------------
  const s4 = deck.appendSlide();
  prepSlide(s4);

  const s4_tag = s4.insertTextBox("PHYSICAL DISK PERFORMANCE VERIFICATION", 60, 30, 800, 25);
  s4_tag.getText().getTextStyle().setFontFamily("Arial").setFontSize(10).setBold(true).setForegroundColor(ACCENT);

  const s4_title = s4.insertTextBox("Quantitative Benchmark: 97.9% Bandwidth Reduction", 60, 50, 800, 50);
  s4_title.getText().getTextStyle().setFontFamily("Arial").setFontSize(22).setBold(true).setForegroundColor(TEXT_WHITE);

  const table = s4.insertTable(6, 3, 60, 120, 560, 320);
  const data = [
    ["Benchmark Metric", "Naive Full Sync", "Nutanix Delta Sync"],
    ["Total File Size", "3.00 MB", "3.00 MB"],
    ["Modified Payload", "—", "58 Bytes"],
    ["Wire Traffic Egress", "128.00 KB", "64.00 KB (1 chunk)"],
    ["Bandwidth Saved", "0 B (0.0%)", "2.94 MB (97.92% saved)"],
    ["Data Integrity Verification", "Passed", "Bit-for-Bit SHA-256 Match"]
  ];

  for (let r = 0; r < 6; r++) {
    for (let c = 0; c < 3; c++) {
      const cell = table.getCell(r, c);
      cell.getText().setText(data[r][c]);
      cell.getFill().setSolidFill(r === 0 ? '#222630' : PANEL_BG);
      const style = cell.getText().getTextStyle();
      style.setFontFamily("Arial").setFontSize(11);
      if (r === 0) style.setBold(true).setForegroundColor(TEXT_WHITE);
      else if (c === 2) style.setBold(true).setForegroundColor(ACCENT);
      else style.setForegroundColor(TEXT_MUTED);
    }
  }

  const callout = s4.insertShape(SlidesApp.ShapeType.RECTANGLE, 640, 120, 260, 320);
  callout.getFill().setSolidFill(PANEL_BG);
  callout.getBorder().getLineFill().setSolidFill(ACCENT);
  const cb = s4.insertTextBox("TAKEAWAY\n\nBy decomposing files into 64KB content blocks, modifying a line inside a large file transfers ONLY the single modified chunk.\n\nResults scale to 100MB+ enterprise datasets with >99% wire reduction.", 655, 140, 230, 280);
  cb.getText().getTextStyle().setFontFamily("Arial").setForegroundColor(TEXT_WHITE).setFontSize(12);
  cb.getText().getParagraphs()[0].getRange().getTextStyle().setFontSize(13).setBold(true).setForegroundColor(ACCENT);

  // -------------------------------------------------------------
  // SLIDE 5: Dual Verification Strategy
  // -------------------------------------------------------------
  const s5 = deck.appendSlide();
  prepSlide(s5);

  const s5_tag = s5.insertTextBox("DEMONSTRATION & VERIFICATION STRATEGY", 60, 30, 800, 25);
  s5_tag.getText().getTextStyle().setFontFamily("Arial").setFontSize(10).setBold(true).setForegroundColor(ACCENT);

  const s5_title = s5.insertTextBox("Ground-Truth Physical Sync + Cluster Telemetry Console", 60, 50, 800, 50);
  s5_title.getText().getTextStyle().setFontFamily("Arial").setFontSize(22).setBold(true).setForegroundColor(TEXT_WHITE);

  const demos = [
    { title: "Part 1: Physical 2-Node Live Sync", desc: "• Real OS filesystem watcher (watchdog) monitoring disk.\n• Sub-50ms propagation between physical folders (folder_a -> folder_b).\n• Interactive Terminal TUI displaying live gossip peers, chunks, and vector clocks.\n• Standalone benchmark script (benchmark_delta.py) measuring disk I/O." },
    { title: "Part 2: Scalable Cluster Telemetry (50+ Nodes)", desc: "• Interactive architectural schematic canvas showing decentralized gossip mesh.\n• Real-time Merkle consistency verification across 50 simulated cluster nodes.\n• Non-destructive split-brain conflict isolation under network partition.\n• Chaos fault injection: severed node recovery and catch-up sync." }
  ];

  demos.forEach((d, i) => {
    const card = s5.insertShape(SlidesApp.ShapeType.RECTANGLE, 60 + i * 430, 120, 410, 340);
    card.getFill().setSolidFill(PANEL_BG);
    card.getBorder().getLineFill().setSolidFill('#222630');

    const tb = s5.insertTextBox(d.title + "\n\n" + d.desc, 75 + i * 430, 140, 380, 300);
    const text = tb.getText();
    text.getTextStyle().setFontFamily("Arial").setForegroundColor(TEXT_WHITE).setFontSize(12);
    text.getParagraphs()[0].getRange().getTextStyle().setFontSize(15).setBold(true).setForegroundColor(i === 0 ? ACCENT : TEXT_WHITE);
  });

  // -------------------------------------------------------------
  // SLIDE 6: Conclusion & Enterprise Impact
  // -------------------------------------------------------------
  const s6 = deck.appendSlide();
  prepSlide(s6);

  const s6_tag = s6.insertTextBox("SUMMARY & ENTERPRISE IMPACT", 60, 30, 800, 25);
  s6_tag.getText().getTextStyle().setFontFamily("Arial").setFontSize(10).setBold(true).setForegroundColor(ACCENT);

  const s6_title = s6.insertTextBox("Hyperconverged File Resilience with Enterprise Performance", 60, 50, 800, 50);
  s6_title.getText().getTextStyle().setFontFamily("Arial").setFontSize(22).setBold(true).setForegroundColor(TEXT_WHITE);

  const impacts = [
    { title: "Alignment with Nutanix AOS & Files", desc: "Solves the core distributed storage challenge: bandwidth-efficient edge-to-core replication and multi-datacenter data consistency without single points of failure." },
    { title: "Verified & Fully Tested", desc: "7 automated test suites execute in <7 seconds, confirming unit chunking, Vector Clock partial ordering, and 20-node cluster convergence." },
    { title: "Open-Source Codebase", desc: "Completely open-source and reproducible with full documentation, tests, and CLI daemons at https://github.com/hskad/nutanix-sync." }
  ];

  impacts.forEach((imp, i) => {
    const card = s6.insertShape(SlidesApp.ShapeType.RECTANGLE, 60, 120 + i * 115, 840, 95);
    card.getFill().setSolidFill(PANEL_BG);
    card.getBorder().getLineFill().setSolidFill('#222630');

    const tb = s6.insertTextBox(imp.title + "\n" + imp.desc, 80, 130 + i * 115, 800, 75);
    const text = tb.getText();
    text.getTextStyle().setFontFamily("Arial").setForegroundColor(TEXT_WHITE).setFontSize(11);
    text.getParagraphs()[0].getRange().getTextStyle().setFontSize(13).setBold(true).setForegroundColor(ACCENT);
  });
}
