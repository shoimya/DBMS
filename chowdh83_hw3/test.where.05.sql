CREATE TABLE student (name TEXT, grade REAL, piazza INTEGER);
INSERT INTO student VALUES ('James', 4.0, 1);
INSERT INTO student VALUES ('Yaxin', NULL, 2);
INSERT INTO student VALUES ('Li', 3.2, 2);
SELECT * FROM student WHERE student.grade > 3.5 ORDER BY student.piazza, grade;
SELECT * FROM student WHERE student.grade < 3.5 ORDER BY student.piazza, grade;
