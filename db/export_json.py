#!/usr/bin/env python3
"""Export SQLite database to JSON for the HTML viewer."""

import sqlite3
import json
import os

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE, "db", "curriculum.db")
OUT_PATH = os.path.join(BASE, "viewer", "data.json")

def export():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    data = {}

    # Standards
    rows = conn.execute("SELECT * FROM standards ORDER BY framework, grade, code").fetchall()
    data['standards'] = [dict(r) for r in rows]

    # Topic clusters
    rows = conn.execute("SELECT * FROM topic_clusters ORDER BY grade, name").fetchall()
    data['topic_clusters'] = [dict(r) for r in rows]

    # Cluster-standard links
    rows = conn.execute("SELECT * FROM cluster_standards").fetchall()
    data['cluster_standards'] = [dict(r) for r in rows]

    # Cluster links (prerequisites/successors)
    rows = conn.execute("SELECT * FROM cluster_links").fetchall()
    data['cluster_links'] = [dict(r) for r in rows]

    # CPM courses
    rows = conn.execute("SELECT * FROM cpm_courses ORDER BY id").fetchall()
    data['cpm_courses'] = [dict(r) for r in rows]

    # CPM modules
    rows = conn.execute("SELECT * FROM cpm_modules ORDER BY course_id, chapter, section, lesson").fetchall()
    data['cpm_modules'] = [dict(r) for r in rows]

    # Alignments
    rows = conn.execute("SELECT * FROM cpm_standard_alignments").fetchall()
    data['cpm_standard_alignments'] = [dict(r) for r in rows]

    # Precomputed: coverage summary per course per framework
    coverage = {}
    for course_row in conn.execute("SELECT id FROM cpm_courses").fetchall():
        cid = course_row['id']
        coverage[cid] = {}
        for fw in ['CCSS-M', 'MN-2007', 'MN-2022', 'TEKS', 'FL-BEST', 'VA-SOL']:
            total = conn.execute(
                "SELECT COUNT(DISTINCT s.id) FROM standards s WHERE s.framework=?", (fw,)
            ).fetchone()[0]

            covered = conn.execute("""
                SELECT COUNT(DISTINCT s.id) FROM standards s
                JOIN cpm_standard_alignments a ON s.id = a.standard_id
                JOIN cpm_modules m ON a.module_id = m.id
                WHERE s.framework=? AND m.course_id=?
            """, (fw, cid)).fetchone()[0]

            coverage[cid][fw] = {'total': total, 'covered': covered}

    data['coverage_summary'] = coverage

    # Precomputed: for each standard, which modules cover it
    std_to_modules = {}
    for row in conn.execute("""
        SELECT a.standard_id, a.module_id, m.course_id, a.source
        FROM cpm_standard_alignments a
        JOIN cpm_modules m ON a.module_id = m.id
    """).fetchall():
        sid = row['standard_id']
        if sid not in std_to_modules:
            std_to_modules[sid] = []
        std_to_modules[sid].append({
            'module_id': row['module_id'],
            'course_id': row['course_id'],
            'source': row['source']
        })
    data['standard_to_modules'] = std_to_modules

    # Precomputed: for each module, which standards it covers
    mod_to_standards = {}
    for row in conn.execute("""
        SELECT a.module_id, a.standard_id, s.framework, s.code, a.source
        FROM cpm_standard_alignments a
        JOIN standards s ON a.standard_id = s.id
    """).fetchall():
        mid = row['module_id']
        if mid not in mod_to_standards:
            mod_to_standards[mid] = []
        mod_to_standards[mid].append({
            'standard_id': row['standard_id'],
            'framework': row['framework'],
            'code': row['code'],
            'source': row['source']
        })
    data['module_to_standards'] = mod_to_standards

    conn.close()

    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, 'w') as f:
        json.dump(data, f, indent=None, separators=(',', ':'))

    size = os.path.getsize(OUT_PATH)
    print(f"Exported to {OUT_PATH} ({size:,} bytes)")
    print(f"  Standards: {len(data['standards'])}")
    print(f"  Modules: {len(data['cpm_modules'])}")
    print(f"  Alignments: {len(data['cpm_standard_alignments'])}")

if __name__ == '__main__':
    export()
