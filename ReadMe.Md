# Coding Challenge  - San Fransisco Public works dataset

This project is completely done using python and Pyspark. Following is the structure of the project

           |-- parameters/
           |      |-- config.json
           |-- Main.py
           |-- Tests.py
           |-- ReadMe.Md
           
The Project has Main.py which is parametrised to be able to read config for input and output, any other config related.

Tests.py Consists of unit test fir each of the method in Main.py.

Solutions for all the four questions have been provided. They have also been modularised into four different methods

In the question1, it says 'area that is covered' - I assumed it to be street, hence split the address column to extract the street name and found trees per street.

Also, I felt site_order may also be relevant to area, hence grouped by this column to extract the counts. 

So, Question1 has two solutions :

###Answer for question1. 
####|site_order|count_by_site|

|         1|        90962|

|         2|        31088|

|         3|        16326|

|         4|        10575|

|         5|         7053|

+-----------+---------------+

####|street_name|count_by_street|

|     Sunset|           3539|

|        San|           2575|

|    Mission|           1771|

|       03rd|           1655|

|      Geary|           1628|


###Answer for question 2:

####|           tree_name|count_by_tree|

|                    |        14772|

| Sycamore: London...|        11383|

| New Zealand Xmas...|         8677|

|        Brisbane Box|         8236|

|       Victorian Box|         6922|

|        Swamp Myrtle|         6781|

|         Cherry Plum|         6728|


###3.Cherry Plum DPW Maintained Count :121
###4.Banyan Fig with Permit Number Count :144