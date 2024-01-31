import sqlite3

conn = sqlite3.connect('students_cancellation_warehouse.db')

cursor = conn.cursor()

cursor.execute('''
CREATE TABLE students_dim (
  student_id INTEGER,
  name TEXT,
  date_of_birth DATE,
  age INTEGER,
  sex TEXT,
  email TEXT,
  street TEXT,
  city TEXT,
  state TEXT,
  postcode INTEGER,
  time_spent_week_hrs NUMERIC,
  num_course_taken INTEGER
);
''')

cursor.execute('''
CREATE TABLE jobs_dim (
  record_id INTEGER PRIMARY KEY AUTOINCREMENT,
  job_id INTEGER,
  category TEXT,
  salary INTEGER,
  row_effective_date DATE,
  row_expiration_date DATE,
  current_row_indicator BOOLEAN
);
''')

cursor.execute('''
CREATE TABLE paths_dim (
  record_id INTEGER PRIMARY KEY AUTOINCREMENT,
  path_id INTEGER,
  path_name TEXT,
  hours_to_complete INTEGER,
  row_effective_date DATE,
  row_expiration_date DATE,
  current_row_indicator BOOLEAN
);
''')

cursor.execute('''
CREATE TABLE cancellations_fact (
  student_id INTEGER,
  job_id INTEGER,
  career_path_id INTEGER,
  PRIMARY KEY (job_id, career_path_id, student_id),
  FOREIGN KEY (job_id) REFERENCES jobs_dim (record_id),
  FOREIGN KEY (career_path_id) REFERENCES paths_dim (record_id),
  FOREIGN KEY (student_id) REFERENCES students_dim (record_id)
);
''')

conn.commit()
conn.close()