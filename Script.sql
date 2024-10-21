show databases;
create database otus_task3;

CREATE EXTERNAL TABLE otus_task3.titanic(
PassengerId INT,
 Survived INT,
 Pclass INT,
 Name STRING,
 Sex STRING,
 Age INT,
 SibSp INT,
 Parch INT,
 Ticket STRING,
 Fare DOUBLE,
 Cabin STRING,
 Embarked STRING)
 ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
      WITH SERDEPROPERTIES (
       "separatorChar" = ","
      ) 
      STORED AS TEXTFILE
      LOCATION '/user/hive/warehouse/titanic'
      tblproperties("skip.header.line.count"="1"
      );

--1) Подсчет кол-ва пассажиров каждого пола севших на титаник в каждом порту
CREATE TABLE IF NOT EXISTS otus_task3.sex_by_embarked AS 
SELECT 
ti.embarked,
SUM(IF(ti.sex = "male", 1,0)) AS male_count,
SUM(IF(ti.sex = "female",1,0)) AS female_count,
COUNT(ti.embarked) AS embarked_count
FROM otus_task3.titanic AS ti
GROUP BY ti.embarked;

--2) Кол-во выживших пасажиров в каждом классе билетов
CREATE TABLE IF NOT EXISTS otus_task3.survived_by_pclass AS 
SELECT 
ti.pclass,
SUM(ti.survived) AS survived,
COUNT(ti.passengerid) AS pass_count
FROM otus_task3.titanic AS ti
GROUP BY ti.pclass;

--3) Статистика выживших в каждой возрастной группе
CREATE TABLE IF NOT EXISTS otus_task3.survived_by_age AS 
SELECT 
ti.survived,
SUM(IF(ti.age <= 25, 1,0)) AS yang_age,
SUM(IF(ti.age > 25 AND ti.age <= 45, 1,0)) AS midle_age,
SUM(IF(ti.age > 45, 1,0)) AS old_age
FROM otus_task3.titanic AS ti
GROUP BY ti.survived;

--4) Статистика выживших пассажиров севших на титаник из каждого порта по классам билетов
CREATE TABLE IF NOT EXISTS otus_task3.survived_by_embarked_for_pclass AS 
SELECT 
ti.embarked,
SUM(IF(ti.pclass = 1, 1, 0)) AS class_1,
SUM(IF(ti.pclass = 2, 1,0)) AS class_2,
SUM(IF(ti.pclass = 3, 1,0)) AS class_3
FROM otus_task3.titanic AS ti
WHERE ti.survived = 1
GROUP BY ti.embarked
HAVING ti.embarked != ''
ORDER BY ti.embarked ASC;

--5) Средняя стоимость билетов по классам выживших пассажиров севших в каждом порту (4 join 5)
CREATE TABLE IF NOT EXISTS otus_task3.avg_fare_for_survived_by_embarked AS 
SELECT 
ti_1.embarked,
ti_1.fare_sum_1/ti_2.class_1 AS class_1_avg_fare,
ti_1.fare_sum_2/ti_2.class_2 AS class_2_avg_fare,
ti_1.fare_sum_3/ti_2.class_3 AS class_3_avg_fare
FROM 
(SELECT 
ti.embarked,
SUM(IF(ti.pclass = 1,ti.fare,0)) AS fare_sum_1,
SUM(IF(ti.pclass = 2,ti.fare,0)) AS fare_sum_2,
SUM(IF(ti.pclass = 3,ti.fare,0)) AS fare_sum_3
FROM otus_task3.titanic AS ti
WHERE ti.survived = 1
GROUP BY ti.embarked
HAVING ti.embarked != ''
ORDER BY ti.embarked ASC) as ti_1
LEFT JOIN 
(SELECT 
ti.embarked,
SUM(IF(ti.pclass = 1, 1, 0)) AS class_1,
SUM(IF(ti.pclass = 2, 1,0)) AS class_2,
SUM(IF(ti.pclass = 3, 1,0)) AS class_3
FROM otus_task3.titanic AS ti
WHERE ti.survived = 1
GROUP BY ti.embarked
HAVING ti.embarked != ''
ORDER BY ti.embarked ASC
) as ti_2 
ON ti_1.embarked = ti_2.embarked;
