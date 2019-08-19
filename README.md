# SparkInterviewQuestions
A repository to hold majorly asked Spark Related Interview Questions.

# Question 1 : Get all the employees  whose year of joining is between 2017 and 2018 and salary is >50.

Customer table 

|cust_id|cust_name|yoj|
|---|---|---|
|1|X|2019-01-01 12:02:00.0|
|2|y|2018-01-01 12:02:00.0|
|3|Z|2017-01-01 12:02:00.0|

Department table 

|id |amount|
|---|---|
| 1 |100|
| 1 |200|

[View Solution](https://github.com/ShikharSundriyal/SparkInterviewQuestions/blob/master/src/main/scala/com/ss/SparkQuestion1.scala)

# Question 2 : Count the number of people residing in each city, there are 2 tables employee(empid,name,citycode) and city(citycode,cityname)

Employee table :

|empid|empname|empcitycode|
|---|---|---| 
1|a|1|
2|b|2|
3|c|1|
4|d|1|
5|e|3|

City Table :

|CityCode|CityName|
|---|---|
|1|delhi|
|2|punjab|
|3|mumbai|

[View Solution](https://github.com/ShikharSundriyal/SparkInterviewQuestions/blob/master/src/main/scala/com/ss/SparkQuestion2.scala)

# Question 3 : Get all the records from left table which are not present in right table

Table Left : 

|id|left|
|---|---|
|0|zero|
|1|one|

Table Right :
 
|id|right|
|---|---|
|0|zero|
|2|two|
|3|three|

[View Solution](https://github.com/ShikharSundriyal/SparkInterviewQuestions/blob/master/src/main/scala/com/ss/SparkQuestion3.scala)

# Question 4 : Find avg salary department wise using rdd approach

Table Dept :

|DEPT|EMP_NAME|SALARY|
|---|---|---|
|1|RAM|10|
|2|Shyam|30|
|1|Vijay|10|

[View Solution](https://github.com/ShikharSundriyal/SparkInterviewQuestions/blob/master/src/main/scala/com/ss/SparkQuestion4.scala)

# Question 5 : Find the lowest salary in each department

Table Dept :

|id|amount|
|---|---|
|1|100|
|1|200|
|2|300|
|2|50|
|3|50|

[View Solution](https://github.com/ShikharSundriyal/SparkInterviewQuestions/blob/master/src/main/scala/com/ss/SparkQuestion5.scala)
