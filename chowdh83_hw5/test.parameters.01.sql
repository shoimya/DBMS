1: CREATE TABLE students (name TEXT, grade REAL DEFAULT 0.0);
Parameters: [('James', 3.5), ('Yaxin', 2.5), ('Messi', 10.0)]
1: INSERT INTO students VALUES (?, ?);
1: INSERT INTO students (grade) VALUES (3.7);
Parameters: [('De Paul',11.0), ('Enzo',12.0), ('Molina',13.0)] 
1: INSERT INTO students VALUES (?,?);
1: SELECT * FROM students ORDER BY grade DESC;