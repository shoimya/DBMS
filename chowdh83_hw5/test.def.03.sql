1: CREATE TABLE students (name TEXT DEFAULT 'MESSI', grade REAL DEFAULT 10.0);
1: INSERT INTO students (name, grade) VALUES ('James', 3.2);
1: INSERT INTO students (grade, name) VALUES (5.0, 'Yaxin');
1: INSERT INTO students (grade) VALUES (3.7);
1: INSERT INTO students DEFAULT VALUES;
1: SELECT grade FROM students ORDER BY grade DESC, name DESC;