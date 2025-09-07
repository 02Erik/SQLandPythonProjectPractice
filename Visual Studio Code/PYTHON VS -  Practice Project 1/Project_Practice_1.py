import sqlite3 as sq
import pandas as pd

#Creating the connection to the database and importing the csv file to the database

con = sq.connect("Chicago_SchoolAnalysis.db")

cur = con.cursor()

df = pd.read_csv('https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-DB0201EN-SkillsNetwork/labs/FinalModule_Coursera_V5/data/ChicagoPublicSchools.csv')

df.to_sql("ChicagoPublicSchools", con, if_exists='replace', index=False)

#Getting the name of the tables within the database
query = 'SELECT name FROM sqlite_master WHERE type="table";'
result = cur.execute(query).fetchall()
print("Getting the name of the tables within the database")
print(result)

#Getting the number of rows within the table
query2 = "PRAGMA table_info('ChicagoPublicSchools'); "
result2 = cur.execute(query2).fetchall()
print("Getting the number of rows within the table")
print(len(result2))

#Obtaining the list of columns and their respective data type
query3 = "PRAGMA table_info('ChicagoPublicSchools')"
result3 = cur.execute(query3).fetchall()
print("Obtaining the list of columns and their respective data type")
for col in result3:
    print(f"Name: {col[1]}, Type: {col[2]}")

#Obtaining the number of elementary schools in Chicago
query4 = 'SELECT COUNT(*) FROM ChicagoPublicSchools WHERE "Elementary, Middle, or High School" = \'ES\';'
result4 = cur.execute(query4).fetchall()
print("Obtaining the number of elementary schools in Chicago")
print(result4)

#Obtaining the highest Safety Score

query5 = 'SELECT MAX(SAFETY_SCORE) AS MAX_SAFETY_SCORE FROM ChicagoPublicSchools;'
result5 = cur.execute(query5).fetchall()
print("Obtaining the highest Safety Score")
print(result5)

#Obtaining the School with the highest safety score
query6 = 'SELECT NAME_OF_SCHOOL, MAX(SAFETY_SCORE) AS MAX_SAFETY_SCORE FROM ChicagoPublicSchools;'
result6 = cur.execute(query6).fetchall()
print("Obtaining the School with the highest safety score")
print(result6)

#Obtaining the top 10 schools with the highest "Average Student Attendance"
query7 = 'SELECT NAME_OF_SCHOOL, AVERAGE_STUDENT_ATTENDANCE FROM ChicagoPublicSchools ORDER BY AVERAGE_STUDENT_ATTENDANCE DESC LIMIT 10;'
result7 = cur.execute(query7).fetchall() 
print("Obtaining the top 10 schools with the highest Average Student Attendance")
for position in range(len(result7)):
    print(f"Position {position + 1}: {result7[position]}")

#Obtaining the list of 5 Schools with the lowest Average Student Attendance sorted in ascending order based on attendance
query8 = 'SELECT NAME_OF_SCHOOL, AVERAGE_STUDENT_ATTENDANCE FROM ChicagoPublicSchools ORDER BY AVERAGE_STUDENT_ATTENDANCE LIMIT 5;'
result8 = cur.execute(query8).fetchall()
print("Obtaining the list of 5 Schools with the lowest Average Student Attendance sorted in ascending order based on attendance")
for position2 in range(len(result8)):
    print(f"Position {position2 + 1}: {result8[position2]}")

#Removing the '%' sign from the above result set for Average Student Attendance column
query9 = 'SELECT NAME_OF_SCHOOL, REPLACE(AVERAGE_STUDENT_ATTENDANCE, "%", "") FROM ChicagoPublicSchools ORDER BY AVERAGE_STUDENT_ATTENDANCE LIMIT 5;'
result9 = cur.execute(query9).fetchall()
print("Removing the percent sign from the above result set for Average Student Attendance column")
for position3 in range(len(result9)):
    print(f"Position {position3 + 1}: {result9[position3]}")

#Obtaining the Schools who have the Average Student Attendance lower than 70%
query10 = 'SELECT NAME_OF_SCHOOL, AVERAGE_STUDENT_ATTENDANCE FROM ChicagoPublicSchools WHERE AVERAGE_STUDENT_ATTENDANCE < 70 ORDER BY AVERAGE_STUDENT_ATTENDANCE'
result10 = cur.execute(query10).fetchall()
print("Obtaining the Schools who have the Average Student Attendance lower than 70%")
for position4 in range(len(result10)):
    print(f"Position {position4 + 1}: {result10[position4]}")

#Obtaining the total college enrollment for each community area
query11 = 'SELECT COMMUNITY_AREA_NAME, SUM(COLLEGE_ENROLLMENT) AS TOTAL_COLLEGE_ENROLLMENT FROM ChicagoPublicSchools GROUP BY COMMUNITY_AREA_NAME LIMIT 10;'
result11 = cur.execute(query11).fetchall()
print("Obtaining the total college enrollment for each community area")
for position5 in range(len(result11)):
    print(f"Position {position5 + 1}: {result11[position5]}")

#Obtaining the 5 Community Areas with the least total College Enrollment sorted in ascending order
query12 = 'SELECT COMMUNITY_AREA_NAME, SUM(COLLEGE_ENROLLMENT) AS TOTAL_COLLEGE_ENROLLMENT FROM ChicagoPublicSchools GROUP BY COMMUNITY_AREA_NAME ORDER BY TOTAL_COLLEGE_ENROLLMENT LIMIT 5;'
result12 = cur.execute(query12).fetchall()
print("Obtaining the 5 Community Areas with the least total College Enrollment sorted in ascending order")
for position6 in range(len(result12)):
    print(f"Position {position6 + 1}: {result12[position6]}")

#Obtaining the 5 schools with lowest safety score.
query13 = "SELECT NAME_OF_SCHOOL, SAFETY_SCORE FROM ChicagoPublicSchools WHERE SAFETY_SCORE != 'Null' ORDER BY SAFETY_SCORE LIMIT 5;"
result13 = cur.execute(query13).fetchall()
print("Obtaining the 5 schools with lowest safety score")
for position7 in range(len(result13)):
    print(f"Position {position7 + 1}: {result13[position7]}")

#Obtaining the hardship index for the community area of the school which has College Enrollment of 4368 - In this area we will be working with another dataset
df2 = pd.read_csv("https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-DB0201EN-SkillsNetwork/labs/FinalModule_Coursera_V5/data/ChicagoCensusData.csv")
df2.to_sql("CENSUS_DATA", con, if_exists='replace', index=False,method="multi")

query14 = 'select hardship_index from CENSUS_DATA CD, ChicagoPublicSchools CPS where CD.community_area_number = CPS.community_area_number and college_enrollment = 4368'
result14 = cur.execute(query14).fetchall()
print("Obtaining the hardship index for the community area of the school which has College Enrollment of 4368")
print(result14)

#Obtaining the hardship index for the community area which has the highest value for College Enrollment
query15 = 'SELECT community_area_number, community_area_name, hardship_index from CENSUS_DATA where community_area_number in (select community_area_number from ChicagoPublicSchools order by college_enrollment desc limit 1)'
result15 = cur.execute(query15).fetchall()
print("Obtaining the hardship index for the community area which has the highest value for College Enrollment")
print(result15)
print(con.close())