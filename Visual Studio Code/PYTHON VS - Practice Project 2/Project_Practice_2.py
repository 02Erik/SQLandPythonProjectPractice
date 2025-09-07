import sqlite3 as sq
import pandas as pd

#Creating the connection to the database and importing the csv file to the database
con = sq.connect("FinalDB.db")
cur = con.cursor()

#Loading the CSV Files to the Database using pandas
df_censusdata = pd.read_csv('https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-DB0201EN-SkillsNetwork/labs/FinalModule_Coursera_V5/data/ChicagoCensusData.csv?utm_medium=Exinfluencer&utm_source=Exinfluencer&utm_content=000026UJ&utm_term=10006555&utm_id=NA-SkillsNetwork-Channel-SkillsNetworkCoursesIBMDeveloperSkillsNetworkDB0201ENSkillsNetwork20127838-2021-01-01')
df_censusdata.to_sql("CHICAGO_CENSUS_DATA", con, if_exists='replace', index=False)

df_publicschools = pd.read_csv('https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-DB0201EN-SkillsNetwork/labs/FinalModule_Coursera_V5/data/ChicagoPublicSchools.csv?utm_medium=Exinfluencer&utm_source=Exinfluencer&utm_content=000026UJ&utm_term=10006555&utm_id=NA-SkillsNetwork-Channel-SkillsNetworkCoursesIBMDeveloperSkillsNetworkDB0201ENSkillsNetwork20127838-2021-01-01')
df_publicschools.to_sql("CHICAGO_PUBLIC_SCHOOLS", con, if_exists='replace', index=False)

df_crimedata = pd.read_csv('https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-DB0201EN-SkillsNetwork/labs/FinalModule_Coursera_V5/data/ChicagoCrimeData.csv?utm_medium=Exinfluencer&utm_source=Exinfluencer&utm_content=000026UJ&utm_term=10006555&utm_id=NA-SkillsNetwork-Channel-SkillsNetworkCoursesIBMDeveloperSkillsNetworkDB0201ENSkillsNetwork20127838-2021-01-01')
df_crimedata.to_sql("CHICAGO_CRIME_DATA", con, if_exists='replace', index=False)

#Obtanining the total number of crimes recorded in the CRIME table
query1 = 'SELECT COUNT(*) FROM CHICAGO_CRIME_DATA;'
result1 = cur.execute(query1).fetchall()    
print("Total number of crimes recorded in the CRIME table")
print(result1)

#Obtaining a list of community area names and numbers with per capita income less than 11000 from CENSUS DATA TABLE
query2 = 'SELECT COMMUNITY_AREA_NUMBER, COMMUNITY_AREA_NAME, PER_CAPITA_INCOME FROM CHICAGO_CENSUS_DATA WHERE PER_CAPITA_INCOME < 11000;'
result2 = cur.execute(query2).fetchall()
print("List of community area names and numbers with per capita income less than 11000 from CENSUS DATA TABLE")
for position in range(len(result2)):
    print(f'{result2[position]}')

#Listing all case numbers for crimes involving minors?(children are not considered minors for the purposes of crime analysis)
#his analysis was made using and comparing the community area number in the tables CHICAGO_CRIME_DATA, CHICAGO_CENSUS_DATA and using the PERCENT_AGED_UNDER_18_OR_OVER_64 from the table CHICAGO_CENSUS_DATA to see the percent of aged under 18 and at the same time the longitude and latitude of the tables CHICAGO_PUBLIC_SCHOOLS and CHICAGO_CRIME_DATA were compared to see crimes in areas near to Schools related to involve minors.
query3 = 'select CRIME.CASE_NUMBER, CRIME.community_area_number, SCHOOLS.LONGITUDE, SCHOOLS.LATITUDE, CENSUS.PERCENT_AGED_UNDER_18_OR_OVER_64 from CHICAGO_CRIME_DATA AS CRIME, CHICAGO_CENSUS_DATA AS CENSUS, CHICAGO_PUBLIC_SCHOOLS AS SCHOOLS WHERE CRIME.community_area_number =  CENSUS.community_area_number and PERCENT_AGED_UNDER_18_OR_OVER_64 > 30 and  ABS(SCHOOLS.LONGITUDE - CRIME.LONGITUDE) < 0.001 AND ABS(SCHOOLS.LATITUDE - CRIME.LATITUDE) < 0.001;'
result3 = cur.execute(query3).fetchall()
print("Listing all case numbers for crimes involving minors")
for position2 in range(len(result3)):
    print(f'{result3[position2]}')

#Listing all kidnapping crimes involving a child
query4 = "SELECT * FROM CHICAGO_CRIME_DATA WHERE PRIMARY_TYPE = 'KIDNAPPING' and DESCRIPTION LIKE '%CHILD%';"
result4 = cur.execute(query4).fetchall()
print("Listing all kidnapping crimes involving a child")
for position3 in range(len(result4)):
    print(f'{result4[position3]}')

#Listing the kind of crimes that were recorded at schools. (No repetitions)
query5 = 'SELECT DISTINCT SCHOOL.COMMUNITY_AREA_NUMBER, SCHOOL.NAME_OF_SCHOOL, CRIME.PRIMARY_TYPE, CRIME.DESCRIPTION FROM CHICAGO_PUBLIC_SCHOOLS AS SCHOOL, CHICAGO_CRIME_DATA AS CRIME \
WHERE SCHOOL.COMMUNITY_AREA_NUMBER = CRIME.COMMUNITY_AREA_NUMBER;'
result5 = cur.execute(query5).fetchall()
print("Listing the kind of crimes that were recorded at schools. (No repetitions)")
for position4 in range(len(result5)):
    print(f'{result5[position4]}')

#Listing 5 community areas with highest % of households below poverty line
query6 = 'SELECT COMMUNITY_AREA_NUMBER, COMMUNITY_AREA_NAME, PERCENT_HOUSEHOLDS_BELOW_POVERTY FROM CHICAGO_CENSUS_DATA  ORDER BY PERCENT_HOUSEHOLDS_BELOW_POVERTY DESC LIMIT 5;'
result6 = cur.execute(query6).fetchall()
print("Listing 5 community areas with highest % of households below poverty line")
for position5 in range(len(result6)):
    print(f'{result6[position5]}')

#Which community area is most crime prone? Display the coumminty area number only
query7 = 'SELECT COMMUNITY_AREA_NUMBER  FROM CHICAGO_CRIME_DATA GROUP BY COMMUNITY_AREA_NUMBER ORDER BY COUNT(*) DESC LIMIT 1;'
result7 = cur.execute(query7).fetchall()
print("Which community area is most crime prone? Display the coumminty area number only")
print(result7)

#Obtaining the name of the community area with highest hardship index
query8 = 'SELECT COMMUNITY_AREA_NAME FROM CHICAGO_CENSUS_DATA WHERE HARDSHIP_INDEX = (SELECT MAX(HARDSHIP_INDEX) FROM CHICAGO_CENSUS_DATA)'
result8 = cur.execute(query8).fetchall()
print("Obtaining the name of the community area with highest hardship index")
print(result8)

#Obtaining the Community Area Name with most number of crimes?
query9 = 'SELECT COMMUNITY_AREA_NAME FROM CHICAGO_CENSUS_DATA  WHERE COMMUNITY_AREA_NUMBER = (SELECT COMMUNITY_AREA_NUMBER FROM CHICAGO_CRIME_DATA GROUP BY COMMUNITY_AREA_NUMBER ORDER BY COUNT(*) DESC LIMIT 1);'
result9 = cur.execute(query9).fetchall()
print("Obtaining the Community Area Name with most number of crimes")
print(result9)
con.close()