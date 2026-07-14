-- Number of students
SELECT COUNT(*) AS total_students
FROM student_features;

-- Average assessment score
SELECT AVG(avg_score)
FROM student_features;

-- Students with failed assessments
SELECT *
FROM student_features
WHERE failed_assessments > 0;

-- Top performing students
SELECT id_student, avg_score
FROM student_features
ORDER BY avg_score DESC
LIMIT 10;

-- Average submission delay
SELECT AVG(avg_submission_delay)
FROM student_features;
