-- CPM Curriculum Standards Alignment Database
-- Drummond Math Solutions · RMS Standards Alignment Tool

CREATE TABLE IF NOT EXISTS standards (
  id INTEGER PRIMARY KEY,
  framework TEXT NOT NULL,
  code TEXT NOT NULL,
  grade TEXT,
  domain TEXT,
  description TEXT,
  topic TEXT,
  UNIQUE(framework, code)
);

CREATE TABLE IF NOT EXISTS topic_clusters (
  id INTEGER PRIMARY KEY,
  grade TEXT,
  name TEXT,
  dok_floor TEXT,
  dok_ceiling TEXT,
  rigor TEXT,
  level4_desc TEXT,
  level3_desc TEXT,
  level2_desc TEXT,
  level1_desc TEXT,
  tier2_vocab TEXT,
  tier3_vocab TEXT
);

CREATE TABLE IF NOT EXISTS cluster_standards (
  cluster_id INTEGER REFERENCES topic_clusters(id),
  standard_id INTEGER REFERENCES standards(id),
  PRIMARY KEY (cluster_id, standard_id)
);

CREATE TABLE IF NOT EXISTS cluster_links (
  cluster_id INTEGER REFERENCES topic_clusters(id),
  linked_cluster_id INTEGER REFERENCES topic_clusters(id),
  link_type TEXT,
  PRIMARY KEY (cluster_id, linked_cluster_id, link_type)
);

CREATE TABLE IF NOT EXISTS cpm_courses (
  id TEXT PRIMARY KEY,
  name TEXT,
  pathway TEXT,
  grade_range TEXT
);

CREATE TABLE IF NOT EXISTS cpm_modules (
  id INTEGER PRIMARY KEY,
  course_id TEXT REFERENCES cpm_courses(id),
  chapter INTEGER,
  section TEXT,
  lesson TEXT,
  core_concepts TEXT,
  core_problems TEXT,
  reduced_year_problems TEXT,
  notes TEXT
);

CREATE TABLE IF NOT EXISTS cpm_standard_alignments (
  module_id INTEGER REFERENCES cpm_modules(id),
  standard_id INTEGER REFERENCES standards(id),
  source TEXT,
  PRIMARY KEY (module_id, standard_id)
);

CREATE TABLE IF NOT EXISTS teaching_log (
  module_id INTEGER REFERENCES cpm_modules(id) PRIMARY KEY,
  taught INTEGER DEFAULT 0,
  date_taught TEXT,
  notes TEXT
);

CREATE INDEX IF NOT EXISTS idx_standards_framework ON standards(framework);
CREATE INDEX IF NOT EXISTS idx_standards_grade ON standards(grade);
CREATE INDEX IF NOT EXISTS idx_cpm_modules_course ON cpm_modules(course_id);
CREATE INDEX IF NOT EXISTS idx_alignments_module ON cpm_standard_alignments(module_id);
CREATE INDEX IF NOT EXISTS idx_alignments_standard ON cpm_standard_alignments(standard_id);
