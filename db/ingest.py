#!/usr/bin/env python3
"""
Ingest all curriculum data files into SQLite database.
Drummond Math Solutions · RMS Standards Alignment Tool
"""

import sqlite3
import pandas as pd
import os
import re
import json

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA = os.path.join(BASE, "data")
DB_PATH = os.path.join(BASE, "db", "curriculum.db")
SCHEMA_PATH = os.path.join(BASE, "db", "schema.sql")

def get_data_file(pattern):
    """Find a data file matching pattern. If multiple matches, prefer exact match."""
    matches = []
    for f in os.listdir(DATA):
        if pattern.lower() in f.lower():
            matches.append(os.path.join(DATA, f))
    if len(matches) == 1:
        return matches[0]
    if len(matches) > 1:
        # Return best match (shortest filename or exact pattern)
        return min(matches, key=lambda x: len(os.path.basename(x)))
    return None

def get_data_file_by_keywords(*keywords):
    """Find file that contains ALL keywords (case-insensitive)."""
    for f in os.listdir(DATA):
        fl = f.lower()
        if all(k.lower() in fl for k in keywords):
            return os.path.join(DATA, f)
    return None

def clean_text(val):
    if pd.isna(val):
        return None
    return str(val).strip()

def split_codes(code_str):
    if not code_str or pd.isna(code_str):
        return []
    codes = re.split(r'[,;\n]+', str(code_str))
    return [c.strip() for c in codes if c.strip()]

def init_db():
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
    conn = sqlite3.connect(DB_PATH)
    with open(SCHEMA_PATH) as f:
        conn.executescript(f.read())
    return conn

# ── 1. Standards & Topic Clusters from K12_Standards_DOK_Rigor_v4.xlsx ──

def ingest_standards_and_clusters(conn):
    f = get_data_file("DOK_Rigor_v4")
    if not f:
        print("WARNING: DOK_Rigor_v4 file not found")
        return

    # Cross-Reference Map → standards
    xref = pd.read_excel(f, sheet_name="Cross-Reference Map")
    frameworks = {
        'CCSS-M Codes': 'CCSS-M',
        'MN 2007 Codes': 'MN-2007',
        'MN 2022 Codes': 'MN-2022',
        'TEKS (TX) Codes': 'TEKS',
        'FL B.E.S.T. Codes': 'FL-BEST',
        'VA SOL Codes': 'VA-SOL'
    }

    standards_map = {}  # (framework, code) → id
    std_id = 0

    for _, row in xref.iterrows():
        grade = clean_text(row.get('Grade'))
        cluster_name = clean_text(row.get('Topic Cluster'))
        if not cluster_name:
            continue

        for col, fw in frameworks.items():
            codes = split_codes(row.get(col))
            for code in codes:
                key = (fw, code)
                if key not in standards_map:
                    std_id += 1
                    standards_map[key] = std_id
                    conn.execute(
                        "INSERT OR IGNORE INTO standards (id, framework, code, grade, domain) VALUES (?,?,?,?,?)",
                        (std_id, fw, code, grade, cluster_name)
                    )

    # TEKS Database
    try:
        teks = pd.read_excel(f, sheet_name="TEKS Database")
        for _, row in teks.iterrows():
            code = clean_text(row.get('TEKS Code'))
            if not code:
                continue
            key = ('TEKS', code)
            if key not in standards_map:
                std_id += 1
                standards_map[key] = std_id
            else:
                sid = standards_map[key]
                conn.execute("UPDATE standards SET description=?, topic=? WHERE id=?",
                             (clean_text(row.get('Description')), clean_text(row.get('Topic')), sid))
                continue
            standards_map[key] = std_id
            conn.execute(
                "INSERT OR IGNORE INTO standards (id, framework, code, grade, domain, description, topic) VALUES (?,?,?,?,?,?,?)",
                (std_id, 'TEKS', code, clean_text(row.get('Grade')),
                 clean_text(row.get('Domain')), clean_text(row.get('Description')),
                 clean_text(row.get('Topic')))
            )
    except Exception as e:
        print(f"  TEKS parse: {e}")

    # MN-22 Database
    try:
        mn22 = pd.read_excel(f, sheet_name="MN-22 Database")
        for _, row in mn22.iterrows():
            code = clean_text(row.get('MN-22 Code'))
            if not code:
                continue
            key = ('MN-2022', code)
            if key not in standards_map:
                std_id += 1
                standards_map[key] = std_id
                conn.execute(
                    "INSERT OR IGNORE INTO standards (id, framework, code, grade, domain, description, topic) VALUES (?,?,?,?,?,?,?)",
                    (std_id, 'MN-2022', code, clean_text(row.get('Grade')),
                     clean_text(row.get('Strand')), clean_text(row.get('Description')),
                     clean_text(row.get('Topic')))
                )
            else:
                sid = standards_map[key]
                conn.execute("UPDATE standards SET description=?, topic=?, domain=? WHERE id=?",
                             (clean_text(row.get('Description')), clean_text(row.get('Topic')),
                              clean_text(row.get('Strand')), sid))
    except Exception as e:
        print(f"  MN-22 parse: {e}")

    # Benchmark Descriptors + Rigor → topic_clusters
    bench = pd.read_excel(f, sheet_name="Benchmark Descriptors")
    rigor_df = None
    try:
        rigor_df = pd.read_excel(f, sheet_name="Rigor & DOK Analysis")
    except:
        pass

    vocab_df = None
    try:
        vocab_df = pd.read_excel(f, sheet_name="Vocabulary Master")
    except:
        pass

    cluster_map = {}  # (grade, name) → cluster_id
    cluster_id = 0

    for _, row in bench.iterrows():
        grade = clean_text(row.get('Grade'))
        name = clean_text(row.get('Topic Cluster'))
        if not name:
            continue

        cluster_id += 1
        dok = clean_text(row.get('DOK'))

        # Try to get rigor data
        dok_floor = dok
        dok_ceiling = dok
        rigor_class = None
        if rigor_df is not None:
            match = rigor_df[(rigor_df['Grade'].astype(str).str.strip() == str(grade)) &
                             (rigor_df['Topic Cluster'].astype(str).str.strip() == name)]
            if len(match) > 0:
                r = match.iloc[0]
                dok_floor = clean_text(r.get('DOK Floor')) or dok
                dok_ceiling = clean_text(r.get('DOK Ceiling')) or dok
                rigor_class = clean_text(r.get('Rigor Classification'))

        tier2 = None
        tier3 = None
        if vocab_df is not None:
            vmatch = vocab_df[(vocab_df['Grade'].astype(str).str.strip() == str(grade)) &
                              (vocab_df['Topic Cluster'].astype(str).str.strip() == name)]
            if len(vmatch) > 0:
                v = vmatch.iloc[0]
                tier2 = clean_text(v.get('Tier 2 (Academic)'))
                tier3 = clean_text(v.get('Tier 3 (Math-Specific)'))

        conn.execute(
            """INSERT INTO topic_clusters
               (id, grade, name, dok_floor, dok_ceiling, rigor,
                level4_desc, level3_desc, level2_desc, level1_desc,
                tier2_vocab, tier3_vocab)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
            (cluster_id, grade, name, dok_floor, dok_ceiling, rigor_class,
             clean_text(row.get('Level 4: Advanced')),
             clean_text(row.get('Level 3: Proficient')),
             clean_text(row.get('Level 2: Developing')),
             clean_text(row.get('Level 1: Beginning')),
             tier2, tier3)
        )
        cluster_map[(str(grade), name)] = cluster_id

        # Link standards to clusters
        for col, fw in frameworks.items():
            xref_row = xref[(xref['Grade'].astype(str).str.strip() == str(grade)) &
                            (xref['Topic Cluster'].astype(str).str.strip() == name)]
            if len(xref_row) > 0:
                codes = split_codes(xref_row.iloc[0].get(col))
                for code in codes:
                    key = (fw, code)
                    if key in standards_map:
                        conn.execute("INSERT OR IGNORE INTO cluster_standards VALUES (?,?)",
                                     (cluster_id, standards_map[key]))

    # Engine Navigation → cluster_links
    try:
        nav = pd.read_excel(f, sheet_name="Engine Navigation")
        for _, row in nav.iterrows():
            grade = str(clean_text(row.get('Grade')))
            name = clean_text(row.get('Topic Cluster'))
            if not name:
                continue
            cid = cluster_map.get((grade, name))
            if not cid:
                continue

            # Parse prerequisite
            prereq = clean_text(row.get('Prerequisite (Retreat To)'))
            if prereq and prereq != '—' and prereq != '–':
                # Try to find the cluster
                for key, pid in cluster_map.items():
                    if prereq.lower() in key[1].lower() or key[1].lower() in prereq.lower():
                        conn.execute("INSERT OR IGNORE INTO cluster_links VALUES (?,?,?)",
                                     (cid, pid, 'prerequisite'))
                        break

            # Parse successor
            succ = clean_text(row.get('Successor (Advance To)'))
            if succ and succ != '—' and succ != '–':
                # Parse "Grade X: Topic Name" format
                m = re.match(r'Grade\s+(\d+):\s*(.*)', succ)
                if m:
                    s_grade, s_name = m.group(1), m.group(2).strip()
                    sid = cluster_map.get((s_grade, s_name))
                    if sid:
                        conn.execute("INSERT OR IGNORE INTO cluster_links VALUES (?,?,?)",
                                     (cid, sid, 'successor'))

            # Parse lateral connections
            lateral = clean_text(row.get('Lateral Connection'))
            if lateral and lateral != '—':
                for lat_name in re.split(r'[;,]', lateral):
                    lat_name = lat_name.strip()
                    lid = cluster_map.get((grade, lat_name))
                    if lid:
                        conn.execute("INSERT OR IGNORE INTO cluster_links VALUES (?,?,?)",
                                     (cid, lid, 'lateral'))
    except Exception as e:
        print(f"  Navigation parse: {e}")

    conn.commit()
    print(f"  Standards: {std_id} | Clusters: {cluster_id}")
    return standards_map, cluster_map

# ── 1b. Official MN 2022 Standards (with full descriptions + code crosswalk) ──

def ingest_official_mn2022(conn, standards_map):
    """
    Load the official MN 2022 spreadsheet (375 standards).
    Build a crosswalk between 3 code formats:
      - Spreadsheet: 6.1.1.01 (padded 2-digit benchmark)
      - CPM correlations / PDFs: 6.1.1.1 (single-digit benchmark)
      - DOK file (MN-22 Database): 6.DP.1.1 (strand-based)

    Strategy: For each official standard, ensure it exists in the DB with
    the numeric code (6.1.1.1 format, matching the correlations file),
    and store the padded code and strand-based code as alternates.
    """
    f = get_data_file_by_keywords("MN_2022_Official") or get_data_file_by_keywords("Academic Standards", "Math Spreadsheet")
    if not f:
        print("  WARNING: Official MN 2022 spreadsheet not found")
        return

    df = pd.read_excel(f, sheet_name='2022 MN Math Standards K-12')
    # Remove end-of-sheet row
    df = df[df['Grade'] != 'end of worksheet ']

    # Grade mapping for the spreadsheet
    grade_map = {'K': 'K', '9–11': '9', '9-11': '9'}

    new_count = 0
    updated_count = 0

    for _, row in df.iterrows():
        padded_code = clean_text(row.get('Code'))
        if not padded_code:
            continue

        grade = str(row.get('Grade', '')).strip()
        grade = grade_map.get(grade, grade)
        strand = clean_text(row.get('Strand'))
        anchor = clean_text(row.get('Anchor Standard'))
        benchmark = clean_text(row.get('Benchmark'))

        # Convert padded code (6.1.1.01) to unpadded (6.1.1.1)
        # Format: G.S.A.BB → strip leading zeros from each part
        parts = padded_code.split('.')
        unpadded_parts = []
        for p in parts:
            try:
                unpadded_parts.append(str(int(p)))
            except ValueError:
                unpadded_parts.append(p)
        unpadded_code = '.'.join(unpadded_parts)

        # Check if this standard already exists (unpadded format from correlations)
        key = ('MN-2022', unpadded_code)
        if key in standards_map:
            # Update with full description
            sid = standards_map[key]
            conn.execute(
                "UPDATE standards SET description=?, domain=?, grade=? WHERE id=? AND (description IS NULL OR description='')",
                (benchmark, strand, grade, sid)
            )
            updated_count += 1
        else:
            # Also check padded format
            key_padded = ('MN-2022', padded_code)
            if key_padded in standards_map:
                sid = standards_map[key_padded]
                conn.execute(
                    "UPDATE standards SET description=?, domain=?, grade=? WHERE id=? AND (description IS NULL OR description='')",
                    (benchmark, strand, grade, sid)
                )
                updated_count += 1
            else:
                # Create new standard with unpadded code (matches correlations)
                new_id = max(standards_map.values()) + 1 if standards_map else 1
                standards_map[key] = new_id
                # Also register padded version
                standards_map[key_padded] = new_id
                conn.execute(
                    "INSERT OR IGNORE INTO standards (id, framework, code, grade, domain, description, topic) VALUES (?,?,?,?,?,?,?)",
                    (new_id, 'MN-2022', unpadded_code, grade, strand, benchmark, anchor)
                )
                new_count += 1

    conn.commit()
    print(f"  Official MN-2022: {new_count} new, {updated_count} updated (total in spreadsheet: {len(df)})")

    # Remove strand-based MN-2022 codes (6.DP.1.1, 6.GM.1.1, etc.) that are
    # duplicates of the official numeric codes (6.1.1.1, 6.2.3.1, etc.).
    # The strand-based codes came from the DOK file's MN-22 Database but
    # the official MN spreadsheet uses numeric codes with full descriptions.
    removed = conn.execute("""
        DELETE FROM standards WHERE framework='MN-2022' AND code GLOB '*[A-Z]*'
    """).rowcount
    conn.commit()
    print(f"  Removed {removed} duplicate strand-based MN-2022 codes (kept numeric codes with descriptions)")

    # Also clean up cluster_standards referencing deleted standards
    conn.execute("DELETE FROM cluster_standards WHERE standard_id NOT IN (SELECT id FROM standards)")
    conn.commit()

    # Skip the old cluster-linking code below
    return

    # Add numeric MN-2022 codes to the same clusters as strand-based ones.
    # Both represent the same MN-2022 standards with different coding formats.
    # Strategy: within each grade, match by position (strand codes and numeric codes
    # are in the same order within each strand grouping).
    # More robust: add ALL numeric MN-2022 for a grade into ALL clusters for that grade.
    # This is slightly over-broad but ensures propagation works.

    numeric_stds = conn.execute(
        "SELECT id, code, grade FROM standards WHERE framework='MN-2022' AND code NOT GLOB '*[A-Z]*'"
    ).fetchall()

    # Get clusters by grade
    clusters_by_grade = {}
    for row in conn.execute("SELECT id, grade FROM topic_clusters"):
        g = str(row[0])
        grade = row[1]
        if grade not in clusters_by_grade:
            clusters_by_grade[grade] = []
        clusters_by_grade[grade].append(row[0])

    # For each numeric standard, find clusters in same grade that have strand-based MN-2022 codes
    added_to_clusters = 0
    for std in numeric_stds:
        sid, code, grade = std
        # Find clusters for this grade
        grade_clusters = clusters_by_grade.get(grade, [])
        for cid in grade_clusters:
            # Check if this cluster has any MN-2022 strand-based codes
            has_mn22 = conn.execute("""
                SELECT COUNT(*) FROM cluster_standards cs
                JOIN standards s ON cs.standard_id = s.id
                WHERE cs.cluster_id=? AND s.framework='MN-2022' AND s.code GLOB '*[A-Z]*'
            """, (cid,)).fetchone()[0]
            if has_mn22 > 0:
                # Match by domain: numeric code's strand number should match cluster's strand
                # E.g., 7.1.x.x = Data strand, 7.2.x.x = Spatial, 7.3.x.x = Patterns
                # Get the cluster's domain to check if it matches
                cluster_domain = conn.execute(
                    "SELECT name FROM topic_clusters WHERE id=?", (cid,)
                ).fetchone()[0]

                # Get a strand-based code from this cluster to figure out the strand number
                sample = conn.execute("""
                    SELECT s.code FROM cluster_standards cs
                    JOIN standards s ON cs.standard_id = s.id
                    WHERE cs.cluster_id=? AND s.framework='MN-2022' AND s.code GLOB '*[A-Z]*'
                    LIMIT 1
                """, (cid,)).fetchone()

                if sample:
                    # Extract strand number from strand-based code (e.g., 7.GM.2.1 → strand part)
                    # And from numeric code (e.g., 7.2.3.1 → second number = strand)
                    # Strand mapping: 1=DP(Data), 2=GM(Spatial/Geometry), 3=PR(Patterns)
                    strand_code = sample[0]  # e.g., "7.GM.2.1"
                    parts = strand_code.split('.')
                    strand_letter = parts[1] if len(parts) > 1 else ''

                    num_parts = code.split('.')
                    num_strand = num_parts[1] if len(num_parts) > 1 else ''

                    # Map strand letters to numbers
                    strand_num_map = {'DP': '1', 'GM': '2', 'PR': '3',
                                      'N': '2', 'A': '3',  # varies by grade
                                      'Data': '1', 'Spatial': '2', 'Patterns': '3'}

                    # Simpler approach: just check if the numeric strand matches
                    # Grade 6+: strand 1=Data, 2=Spatial/Geometry, 3=Patterns
                    match = False
                    if strand_letter in ('DP',) and num_strand == '1':
                        match = True
                    elif strand_letter in ('GM',) and num_strand == '2':
                        match = True
                    elif strand_letter in ('N', 'A') and num_strand in ('2', '3'):
                        # N and A are in Patterns and Relationships for grades 6-8
                        match = True
                    elif strand_letter in ('PR',) and num_strand == '3':
                        match = True
                    else:
                        # Fallback: add to cluster if same grade (slightly over-broad but works)
                        match = True

                    if match:
                        try:
                            conn.execute("INSERT OR IGNORE INTO cluster_standards VALUES (?,?)",
                                         (cid, sid))
                            added_to_clusters += 1
                        except:
                            pass

    conn.commit()
    print(f"  Added {added_to_clusters} numeric MN-2022 codes to clusters")

# ── 2. CPM Courses ──

def ingest_cpm_courses(conn):
    courses = [
        ('CC1', 'Core Connections Course 1', 'core_connections', '6'),
        ('CC2', 'Core Connections Course 2', 'core_connections', '7'),
        ('CC3', 'Core Connections Course 3', 'core_connections', '8'),
        ('CCA', 'CC Algebra', 'traditional', '9'),
        ('CCG', 'CC Geometry', 'traditional', '10'),
        ('CCA2', 'CC Algebra 2', 'traditional', '11'),
        ('INT1', 'Integrated Math 1', 'integrated', '9'),
        ('INT2', 'Integrated Math 2', 'integrated', '10'),
        ('INT3', 'Integrated Math 3', 'integrated', '11'),
        ('PC3', 'Precalculus 3rd Ed.', 'upper', '11-12'),
        ('CALC', 'Calculus 3rd Ed.', 'upper', '11-12'),
        ('STATS', 'Statistics', 'upper', '11-12'),
    ]
    for c in courses:
        conn.execute("INSERT OR IGNORE INTO cpm_courses VALUES (?,?,?,?)", c)
    conn.commit()

# ── 3. CPM Modules from Lesson Guides ──

def ingest_cc13_modules(conn):
    f = get_data_file_by_keywords("CC1", "Remotely")
    if not f:
        f = get_data_file_by_keywords("CC1-3")
    if not f:
        print("  WARNING: CC1-3 guide not found")
        return

    module_id = 0

    # Get actual sheet names dynamically
    xls = pd.ExcelFile(f)
    course_sheets = {}
    for s in xls.sheet_names:
        if 'CC1' in s and 'Individual' in s:
            course_sheets[s] = 'CC1'
        elif 'CC2' in s and 'Individual' in s:
            course_sheets[s] = 'CC2'
        elif 'CC3' in s and 'Individual' in s:
            course_sheets[s] = 'CC3'

    if not course_sheets:
        print(f"  WARNING: No Individual Lesson Level sheets found in {xls.sheet_names}")
        return

    for sheet_name, course_id in course_sheets.items():
        try:
            df = pd.read_excel(f, sheet_name=sheet_name, header=None)
        except:
            print(f"  Sheet '{sheet_name}' not found")
            continue

        # Find the header row (contains 'Chapter' or 'Lesson')
        header_row = None
        for i, row in df.iterrows():
            vals = [str(v).lower() for v in row.values if pd.notna(v)]
            if any('chapter' in v for v in vals) and any('lesson' in v for v in vals):
                header_row = i
                break

        if header_row is None:
            # Try using row 0 as header context
            header_row = 0

        current_chapter = None
        current_section = None

        for i in range(header_row + 1, len(df)):
            row = df.iloc[i]
            vals = [clean_text(v) for v in row.values]

            # Skip empty rows and chapter overview rows
            if all(v is None for v in vals):
                continue

            # Column layout: Chapter, Section, Core Concepts, Lesson, Learning Intent, CCSS, Core Problems, Reduced Year, Remediation, Notes
            chapter_val = vals[0] if len(vals) > 0 else None
            section_val = vals[1] if len(vals) > 1 else None
            concepts_val = vals[2] if len(vals) > 2 else None
            lesson_val = vals[3] if len(vals) > 3 else None
            intent_val = vals[4] if len(vals) > 4 else None
            ccss_val = vals[5] if len(vals) > 5 else None
            core_probs = vals[6] if len(vals) > 6 else None
            reduced_probs = vals[7] if len(vals) > 7 else None
            notes_val = vals[9] if len(vals) > 9 else (vals[8] if len(vals) > 8 else None)

            # Track chapter
            if chapter_val and re.match(r'^\d+$', str(chapter_val).strip()):
                current_chapter = int(chapter_val)
            if section_val and re.match(r'^\d+\.\d+', str(section_val).strip()):
                current_section = section_val

            # Only insert if we have a lesson number
            if lesson_val and re.match(r'^\d+\.\d+\.\d+', str(lesson_val).strip()):
                module_id += 1
                conn.execute(
                    """INSERT INTO cpm_modules
                       (id, course_id, chapter, section, lesson, core_concepts,
                        core_problems, reduced_year_problems, notes)
                       VALUES (?,?,?,?,?,?,?,?,?)""",
                    (module_id, course_id, current_chapter, current_section,
                     lesson_val, concepts_val or intent_val,
                     core_probs, reduced_probs,
                     notes_val)
                )

                # Store CCSS alignment
                if ccss_val:
                    for code in split_codes(ccss_val):
                        conn.execute(
                            "INSERT OR IGNORE INTO cpm_standard_alignments VALUES (?,?,?)",
                            (module_id,
                             _find_standard_id(conn, 'CCSS-M', code),
                             'ccss_lesson_guide')
                        )

        print(f"  {course_id}: loaded through module_id {module_id}")

    conn.commit()
    return module_id

def ingest_cca_ccg_cca2_modules(conn, start_id=0):
    f = get_data_file_by_keywords("CCA", "CCG", "Remotely")
    if not f:
        f = get_data_file_by_keywords("CCA", "CCG")
    if not f:
        print("  WARNING: CCA/CCG/CCA2 guide not found")
        return start_id

    module_id = start_id
    course_sheets = {'CCA': 'CCA', 'CCG': 'CCG', 'CCA2': 'CCA2'}

    for sheet_name, course_id in course_sheets.items():
        try:
            df = pd.read_excel(f, sheet_name=sheet_name, header=None)
        except:
            print(f"  Sheet '{sheet_name}' not found")
            continue

        # Find header row
        header_row = None
        for i, row in df.iterrows():
            vals = [str(v).lower() for v in row.values if pd.notna(v)]
            if any('chapter' in v for v in vals) and any('section' in v for v in vals):
                header_row = i
                break
        if header_row is None:
            header_row = 0

        current_chapter = None

        for i in range(header_row + 1, len(df)):
            row = df.iloc[i]
            vals = [clean_text(v) for v in row.values]

            if all(v is None for v in vals):
                continue

            chapter_val = vals[0] if len(vals) > 0 else None
            section_val = vals[1] if len(vals) > 1 else None

            # These sheets have section-level data (no individual lessons)
            # Columns: Chapter, Section, (Lesson), Core Concepts, TF problems, Team problems, IS problems, Remediation, Sample outline
            concepts_val = vals[3] if len(vals) > 3 else None
            tf_probs = vals[4] if len(vals) > 4 else None
            team_probs = vals[5] if len(vals) > 5 else None
            is_probs = vals[6] if len(vals) > 6 else None
            notes_val = vals[8] if len(vals) > 8 else (vals[7] if len(vals) > 7 else None)

            # Track chapter
            if chapter_val:
                ch = re.match(r'^(\d+)', str(chapter_val).strip())
                if ch:
                    current_chapter = int(ch.group(1))

            if section_val and re.match(r'^\d+\.\d+', str(section_val).strip()):
                module_id += 1
                # Combine all problem types
                all_probs = "; ".join(filter(None, [
                    f"TF: {tf_probs}" if tf_probs else None,
                    f"Team: {team_probs}" if team_probs else None,
                    f"IS: {is_probs}" if is_probs else None,
                ]))

                conn.execute(
                    """INSERT INTO cpm_modules
                       (id, course_id, chapter, section, lesson, core_concepts,
                        core_problems, reduced_year_problems, notes)
                       VALUES (?,?,?,?,?,?,?,?,?)""",
                    (module_id, course_id, current_chapter, section_val,
                     None, concepts_val,
                     all_probs, None, notes_val)
                )

        print(f"  {course_id}: loaded through module_id {module_id}")

    conn.commit()
    return module_id

def ingest_int13_modules(conn, start_id=0):
    f = get_data_file_by_keywords("INT1-3", "Remotely")
    if not f:
        print("  WARNING: INT1-3 guide not found")
        return start_id

    module_id = start_id
    course_sheets = {'INT1': 'INT1', 'INT2': 'INT2', 'INT3': 'INT3'}

    for sheet_name, course_id in course_sheets.items():
        try:
            df = pd.read_excel(f, sheet_name=sheet_name, header=None)
        except:
            print(f"  Sheet '{sheet_name}' not found")
            continue

        # Find header row
        header_row = None
        for i, row in df.iterrows():
            vals = [str(v).lower() for v in row.values if pd.notna(v)]
            if any('chapter' in v for v in vals) and any('section' in v for v in vals):
                header_row = i
                break
        if header_row is None:
            header_row = 1  # typical for these files

        current_chapter = None

        for i in range(header_row + 1, len(df)):
            row = df.iloc[i]
            vals = [clean_text(v) for v in row.values]

            if all(v is None for v in vals):
                continue

            chapter_val = vals[0] if len(vals) > 0 else None
            section_val = vals[1] if len(vals) > 1 else None
            concepts_val = vals[2] if len(vals) > 2 else None
            tf_probs = vals[3] if len(vals) > 3 else None
            team_probs = vals[4] if len(vals) > 4 else None
            is_probs = vals[5] if len(vals) > 5 else None
            remediation = vals[6] if len(vals) > 6 else None
            notes_val = vals[7] if len(vals) > 7 else None

            if chapter_val:
                ch = re.match(r'^(\d+)', str(chapter_val).strip())
                if ch:
                    current_chapter = int(ch.group(1))

            if section_val and re.match(r'^\d+\.\d+', str(section_val).strip()):
                module_id += 1
                all_probs = "; ".join(filter(None, [
                    f"TF: {tf_probs}" if tf_probs else None,
                    f"Team: {team_probs}" if team_probs else None,
                    f"IS: {is_probs}" if is_probs else None,
                ]))
                conn.execute(
                    """INSERT INTO cpm_modules
                       (id, course_id, chapter, section, lesson, core_concepts,
                        core_problems, reduced_year_problems, notes)
                       VALUES (?,?,?,?,?,?,?,?,?)""",
                    (module_id, course_id, current_chapter, section_val,
                     None, concepts_val, all_probs, None,
                     (remediation or '') + (' | ' + notes_val if notes_val else ''))
                )

        print(f"  {course_id}: loaded through module_id {module_id}")

    conn.commit()
    return module_id

def ingest_pc3_calc_stats_modules(conn, start_id=0):
    f = get_data_file_by_keywords("PC3", "Remotely")
    if not f:
        f = get_data_file_by_keywords("DRAFT", "Remotely")
    if not f:
        print("  WARNING: PC3/Calc/Stats guide not found")
        return start_id

    module_id = start_id
    course_sheets = {
        'Statistics': 'STATS',
        'Precalc': 'PC3',
        'Calculus': 'CALC',
    }

    for sheet_name, course_id in course_sheets.items():
        try:
            df = pd.read_excel(f, sheet_name=sheet_name, header=None)
        except:
            print(f"  Sheet '{sheet_name}' not found")
            continue

        # Find header row
        header_row = None
        for i, row in df.iterrows():
            vals = [str(v).lower() for v in row.values if pd.notna(v)]
            if any('chapter' in v for v in vals) and any('section' in v for v in vals):
                header_row = i
                break
        if header_row is None:
            header_row = 1

        current_chapter = None

        for i in range(header_row + 1, len(df)):
            row = df.iloc[i]
            vals = [clean_text(v) for v in row.values]

            if all(v is None for v in vals):
                continue

            chapter_val = vals[0] if len(vals) > 0 else None
            section_val = vals[1] if len(vals) > 1 else None
            concepts_val = vals[2] if len(vals) > 2 else None
            tf_probs = vals[3] if len(vals) > 3 else None
            team_probs = vals[4] if len(vals) > 4 else None
            is_probs = vals[5] if len(vals) > 5 else None
            remediation = vals[6] if len(vals) > 6 else None
            notes_val = vals[7] if len(vals) > 7 else None

            if chapter_val:
                ch = re.match(r'^(\d+)', str(chapter_val).strip())
                if ch:
                    current_chapter = int(ch.group(1))

            # These files have lesson-level sections (1.1.1, 1.1.2, etc.)
            if section_val and re.match(r'^\d+\.', str(section_val).strip()):
                module_id += 1
                all_probs = "; ".join(filter(None, [
                    f"TF: {tf_probs}" if tf_probs else None,
                    f"Team: {team_probs}" if team_probs else None,
                    f"IS: {is_probs}" if is_probs else None,
                ]))

                # Determine if this is a lesson (x.x.x) or section (x.x)
                sec_str = str(section_val).strip()
                parts = sec_str.split('.')
                lesson_val = sec_str if len(parts) >= 3 else None
                section_only = f"{parts[0]}.{parts[1]}" if len(parts) >= 2 else sec_str

                conn.execute(
                    """INSERT INTO cpm_modules
                       (id, course_id, chapter, section, lesson, core_concepts,
                        core_problems, reduced_year_problems, notes)
                       VALUES (?,?,?,?,?,?,?,?,?)""",
                    (module_id, course_id, current_chapter, section_only,
                     lesson_val, concepts_val, all_probs, None,
                     (remediation or '') + (' | ' + notes_val if notes_val else ''))
                )

        print(f"  {course_id}: loaded through module_id {module_id}")

    conn.commit()
    return module_id

# ── 4. MN 2022 Correlations ──

def _get_or_create_module(conn, course_id, lesson_num, next_id_holder):
    """Find or create a CPM module for a lesson reference. Returns module_id."""
    parts = lesson_num.split('.')
    chapter = int(parts[0]) if parts else None
    section = f"{parts[0]}.{parts[1]}" if len(parts) >= 2 else None
    lesson = lesson_num if len(parts) >= 3 else None

    # Try exact lesson match
    if lesson:
        row = conn.execute(
            "SELECT id FROM cpm_modules WHERE course_id=? AND lesson=?",
            (course_id, lesson)
        ).fetchone()
        if row:
            return row[0]

    # Try section match
    if section:
        row = conn.execute(
            "SELECT id FROM cpm_modules WHERE course_id=? AND section=?",
            (course_id, section)
        ).fetchone()
        if row:
            return row[0]

    # Create new module
    next_id_holder[0] += 1
    mid = next_id_holder[0]
    conn.execute(
        "INSERT INTO cpm_modules (id, course_id, chapter, section, lesson) VALUES (?,?,?,?,?)",
        (mid, course_id, chapter, section, lesson)
    )
    return mid

def _ensure_standard(conn, standards_map, mn_code, desc=None, grade=None):
    """Find or create an MN-2022 standard. Returns standard_id."""
    std_key = ('MN-2022', mn_code)
    if std_key in standards_map:
        return standards_map[std_key]

    new_id = max(standards_map.values()) + 1 if standards_map else 1
    standards_map[std_key] = new_id
    if not grade:
        grade = mn_code.split('.')[0]
    conn.execute(
        "INSERT OR IGNORE INTO standards (id, framework, code, grade, description) VALUES (?,?,?,?,?)",
        (new_id, 'MN-2022', mn_code, grade, desc)
    )
    return new_id

def ingest_mn_correlations(conn, standards_map):
    f = get_data_file_by_keywords("Correlations", "MN")
    if not f:
        print("  WARNING: MN Correlations file not found")
        return

    # Get current max module id
    max_mod = conn.execute("SELECT COALESCE(MAX(id), 0) FROM cpm_modules").fetchone()[0]
    next_id = [max_mod]

    xls = pd.ExcelFile(f)

    for sheet in xls.sheet_names:
        df = pd.read_excel(f, sheet_name=sheet, header=None)
        sheet_lower = sheet.lower()

        # Determine if this is a single-course or multi-course sheet
        is_hs = 'grade 9' in sheet_lower or 'cca' in sheet_lower

        # Determine course mapping
        if 'grade 6' in sheet_lower or 'cc1' in sheet_lower:
            course_cols = {'CC1': None}  # will find column below
        elif 'grade 7' in sheet_lower or 'cc2' in sheet_lower:
            course_cols = {'CC2': None}
        elif 'grade 8' in sheet_lower or ('cc3' in sheet_lower or 'cc8' in sheet_lower):
            course_cols = {'CC3': None}
        elif is_hs:
            course_cols = {'CCA': None, 'CCG': None, 'CCA2': None}
        else:
            print(f"  Skipping sheet: {sheet}")
            continue

        print(f"  Processing: {sheet}")

        # Find header row and column positions
        header_row = None
        mn_col = None

        for i in range(min(10, len(df))):
            for j in range(len(df.columns)):
                val = str(df.iloc[i, j]).strip().lower() if pd.notna(df.iloc[i, j]) else ''
                if val == 'mn 2022':
                    header_row = i
                    mn_col = j
                    break
            if header_row is not None:
                break

        if mn_col is None:
            print(f"    Could not find header")
            continue

        # Find lesson columns by header text
        for j in range(len(df.columns)):
            hdr = str(df.iloc[header_row, j]).strip().lower() if pd.notna(df.iloc[header_row, j]) else ''
            if is_hs:
                if 'cca2' in hdr:
                    course_cols['CCA2'] = j
                elif 'ccg' in hdr:
                    course_cols['CCG'] = j
                elif 'cca' in hdr and 'cca2' not in hdr:
                    course_cols['CCA'] = j
            else:
                if 'core connection' in hdr or 'minnesota' in hdr:
                    # Single course sheet — this is the lesson column
                    for cid in course_cols:
                        course_cols[cid] = j

        # Filter out courses with no column found
        active_courses = {c: col for c, col in course_cols.items() if col is not None}
        if not active_courses:
            print(f"    No lesson columns found")
            continue

        print(f"    MN col: {mn_col}, Courses: {active_courses}")

        alignment_count = 0
        for i in range(header_row + 1, len(df)):
            mn_code = clean_text(df.iloc[i, mn_col])
            if not mn_code or not re.match(r'^\d+\.\d+', mn_code):
                continue

            # Get description from benchmark column (col after MN code)
            desc = clean_text(df.iloc[i, mn_col + 1]) if mn_col + 1 < len(df.columns) else None
            if desc and len(desc) < 20:
                desc = None

            std_id = _ensure_standard(conn, standards_map, mn_code, desc)

            # Process each course's lesson column
            for course_id, col_idx in active_courses.items():
                lessons_str = clean_text(df.iloc[i, col_idx])
                if not lessons_str or lessons_str.lower().startswith('updated'):
                    continue

                is_supplement = 'SS' in lessons_str or 'supplement' in lessons_str.lower()
                source = 'mn_correlation_supplement' if is_supplement else 'mn_correlation'

                # Extract all lesson numbers
                all_lessons = re.findall(r'(\d+\.\d+\.?\d*)', lessons_str)
                for lesson_num in all_lessons:
                    mod_id = _get_or_create_module(conn, course_id, lesson_num, next_id)
                    conn.execute(
                        "INSERT OR IGNORE INTO cpm_standard_alignments VALUES (?,?,?)",
                        (mod_id, std_id, source)
                    )
                    alignment_count += 1

        print(f"    Alignments created: {alignment_count}")

    conn.commit()

def _find_standard_id(conn, framework, code):
    row = conn.execute(
        "SELECT id FROM standards WHERE framework=? AND code=?",
        (framework, code)
    ).fetchone()
    if row:
        return row[0]
    # Insert new standard
    conn.execute(
        "INSERT INTO standards (framework, code) VALUES (?,?)",
        (framework, code)
    )
    return conn.execute("SELECT last_insert_rowid()").fetchone()[0]

# ── 5b. CCSS alignments from CPM PDF correlation documents ──

def ingest_pdf_ccss_alignments(conn):
    try:
        import fitz
    except ImportError:
        print("  WARNING: pymupdf not installed, skipping PDF ingestion")
        return

    def extract_text(filename):
        for fn in os.listdir(DATA):
            if filename.lower() in fn.lower():
                doc = fitz.open(os.path.join(DATA, fn))
                text = ""
                for p in range(len(doc)):
                    text += doc[p].get_text() + "\n"
                doc.close()
                return text
        return None

    total_new = 0

    # ── CC2: CCSS standard → lesson (from CC2_standards_alignment.pdf) ──
    text = extract_text("CC2_standards_alignment")
    if text:
        lines = text.split('\n')
        current_std = None
        cc2_count = 0
        for line in lines:
            line = line.strip()
            std_match = re.match(r'^(7\.\w+\.\d+[a-z]?)\.?\s', line)
            if std_match:
                current_std = std_match.group(1)
            elif current_std and re.match(r'^[\d]+\.[\d]+\.[\d]+', line):
                lessons = re.findall(r'(\d+\.\d+\.\d+)', line)
                for lesson in lessons:
                    std_id = _find_standard_id(conn, 'CCSS-M', current_std)
                    mod = conn.execute(
                        "SELECT id FROM cpm_modules WHERE course_id='CC2' AND lesson=?",
                        (lesson,)
                    ).fetchone()
                    if mod:
                        conn.execute(
                            "INSERT OR IGNORE INTO cpm_standard_alignments VALUES (?,?,?)",
                            (mod[0], std_id, 'ccss_pdf_correlation')
                        )
                        cc2_count += 1
        print(f"  CC2 PDF: {cc2_count} CCSS alignments")
        total_new += cc2_count

    # ── CC3: lesson → CCSS (from CC3 Text to CCSS.pdf) ──
    text = extract_text("CC3 Text to CCSS")
    if text:
        lines = text.split('\n')
        current_lesson = None
        cc3_count = 0
        for line in lines:
            line = line.strip()
            lesson_match = re.match(r'^(\d+\.\d+\.\d+)$', line)
            if lesson_match:
                current_lesson = lesson_match.group(1)
            elif current_lesson:
                ccss_codes = re.findall(r'(8\.\w+\.\d+[a-z]?)', line)
                if ccss_codes:
                    for code in ccss_codes:
                        std_id = _find_standard_id(conn, 'CCSS-M', code)
                        mod = conn.execute(
                            "SELECT id FROM cpm_modules WHERE course_id='CC3' AND lesson=?",
                            (current_lesson,)
                        ).fetchone()
                        if mod:
                            conn.execute(
                                "INSERT OR IGNORE INTO cpm_standard_alignments VALUES (?,?,?)",
                                (mod[0], std_id, 'ccss_pdf_correlation')
                            )
                            cc3_count += 1
                    current_lesson = None
        print(f"  CC3 lesson→CCSS PDF: {cc3_count} alignments")
        total_new += cc3_count

    # ── CC3: CCSS → lesson (from Correlation CC3 to CCSS.pdf) ──
    text = extract_text("Correlation CC3 to CCSS Grade 8")
    if text:
        lines = text.split('\n')
        current_std = None
        cc3b_count = 0
        for line in lines:
            line = line.strip()
            std_match = re.match(r'^(8\.\w+\.\d+[a-z]?)\.?\s', line)
            if std_match:
                current_std = std_match.group(1)
            elif current_std and re.match(r'^[\d]+\.[\d]+\.[\d]+', line):
                lessons = re.findall(r'(\d+\.\d+\.\d+)', line)
                for lesson in lessons:
                    std_id = _find_standard_id(conn, 'CCSS-M', current_std)
                    mod = conn.execute(
                        "SELECT id FROM cpm_modules WHERE course_id='CC3' AND lesson=?",
                        (lesson,)
                    ).fetchone()
                    if mod:
                        conn.execute(
                            "INSERT OR IGNORE INTO cpm_standard_alignments VALUES (?,?,?)",
                            (mod[0], std_id, 'ccss_pdf_correlation')
                        )
                        cc3b_count += 1
        print(f"  CC3 CCSS→lesson PDF: {cc3b_count} alignments")
        total_new += cc3b_count

    conn.commit()
    print(f"  Total new from PDFs: {total_new}")

# ── 6. Propagate alignments across frameworks ──

def propagate_alignments(conn):
    """
    The MN correlations file uses numeric benchmark codes (6.1.1.1) while the
    DOK file uses strand-based codes (6.DP.1.1) for MN-2022. Both are MN-2022
    but different formats.

    Strategy: when a CPM module is aligned to a standard in one framework,
    propagate that alignment to ALL other standards in the SAME topic cluster.
    This bridges CCSS-M alignments → MN-2022, MN-2007, TEKS, etc.
    """

    # For each CPM module that has at least one alignment,
    # find all clusters that contain any of its aligned standards,
    # then create alignments to ALL standards in those clusters.

    modules_with_alignments = conn.execute(
        "SELECT DISTINCT module_id FROM cpm_standard_alignments"
    ).fetchall()

    new_alignments = 0
    for (mod_id,) in modules_with_alignments:
        # Get all standard IDs this module is aligned to
        aligned_std_ids = [r[0] for r in conn.execute(
            "SELECT DISTINCT standard_id FROM cpm_standard_alignments WHERE module_id=?",
            (mod_id,)
        ).fetchall()]

        # Find all clusters containing any of these standards
        cluster_ids = set()
        for std_id in aligned_std_ids:
            for (cid,) in conn.execute(
                "SELECT cluster_id FROM cluster_standards WHERE standard_id=?",
                (std_id,)
            ).fetchall():
                cluster_ids.add(cid)

        # Get ALL standards in those clusters
        for cid in cluster_ids:
            sibling_stds = conn.execute(
                "SELECT standard_id FROM cluster_standards WHERE cluster_id=?",
                (cid,)
            ).fetchall()

            for (sib_id,) in sibling_stds:
                if sib_id not in aligned_std_ids:
                    try:
                        conn.execute(
                            "INSERT OR IGNORE INTO cpm_standard_alignments VALUES (?,?,?)",
                            (mod_id, sib_id, 'inferred_via_cluster')
                        )
                        new_alignments += 1
                    except:
                        pass

    conn.commit()
    print(f"  Propagated {new_alignments} new alignments via cluster cross-references")

# ── Main ──

def main():
    print("=== CPM Curriculum Standards Alignment Database ===")
    print(f"Database: {DB_PATH}")
    print()

    conn = init_db()

    print("1. Ingesting standards & topic clusters...")
    result = ingest_standards_and_clusters(conn)
    standards_map = result[0] if result else {}

    print("\n1b. Loading official MN 2022 standards from spreadsheet...")
    ingest_official_mn2022(conn, standards_map)

    print("\n2. Ingesting CPM courses...")
    ingest_cpm_courses(conn)

    print("\n3. Ingesting CC1-3 modules...")
    last_id = ingest_cc13_modules(conn) or 0

    print("\n4. Ingesting CCA/CCG/CCA2 modules...")
    last_id = ingest_cca_ccg_cca2_modules(conn, last_id)

    print("\n4b. Ingesting INT1-3 modules...")
    last_id = ingest_int13_modules(conn, last_id)

    print("\n4c. Ingesting PC3/Calc/Stats modules...")
    last_id = ingest_pc3_calc_stats_modules(conn, last_id)

    print("\n5. Ingesting MN 2022 correlations...")
    ingest_mn_correlations(conn, standards_map)

    print("\n5b. Ingesting CCSS alignments from PDF correlation docs...")
    ingest_pdf_ccss_alignments(conn)

    # NOTE: Cluster-based propagation disabled — it was too broad, creating
    # false alignments (one lesson mapped to 50+ standards). The direct
    # CPM correlations + CCSS lesson guide data is the reliable source.
    # print("\n6. Propagating alignments across frameworks via clusters...")
    # propagate_alignments(conn)
    print("\n6. Skipping cluster propagation (using direct alignments only)")

    # Summary
    print("\n=== Database Summary ===")
    for table in ['standards', 'topic_clusters', 'cluster_standards', 'cluster_links',
                  'cpm_courses', 'cpm_modules', 'cpm_standard_alignments']:
        count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"  {table}: {count} rows")

    # Show alignment coverage
    print("\n=== Alignment Coverage ===")
    for course in ['CC1', 'CC2', 'CC3', 'CCA', 'CCG', 'CCA2']:
        total = conn.execute("SELECT COUNT(*) FROM cpm_modules WHERE course_id=?", (course,)).fetchone()[0]
        aligned = conn.execute("""
            SELECT COUNT(DISTINCT m.id) FROM cpm_modules m
            JOIN cpm_standard_alignments a ON m.id = a.module_id
            WHERE m.course_id=?
        """, (course,)).fetchone()[0]
        print(f"  {course}: {aligned}/{total} modules have standard alignments")

    conn.close()
    print(f"\nDone! Database saved to {DB_PATH}")

if __name__ == '__main__':
    main()
