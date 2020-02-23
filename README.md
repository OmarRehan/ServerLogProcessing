# Million Song Dataset/Server Log Processing
--------------
## Project Objectives :-
---------------------------
- The Main Objective of this project is to process Server Log Data of Million Song Dataset, and ingest the processed data into Postgresql Database with the below Funcionalities:-
    - Read & Process data to be in the Correct format for Analysis
    - Load Data into the Db Initial & Incremental automatically
    - Update Data already loaded in case corrupted files were already loaded 

## Project Phases :-
-----------------------
1- Data Analysis:

    - Conducted Data Analysis using python to analyze and decide the design of the Schema
    - Analysis was done using two Jupyter Notebooks one for Play Log files and another for Song Logs files (EDA - LogFiles.ipynb,EDA - SongFiles.ipynb)

2- Schema Design:

    - Schema Design exists in PostgresModel.png
    - DDLs & DMLs for Creation and Loading to this Schema exist in sql_queries.py

3- Development:

    - The Main Python Script to start the ETL Process exists in etl.py, which contains all the Phases of Reading the Logs, Filtering Reformatting & Cleaning the Data and Error Handling

4- Data Validation:

    - Conducted Unit Testing on Data Loading using this Notebook test.ipynb, to validate the Counts and the Loaded data

## Processing Steps:-
------------------------
1- Create Connection to the Database

2- Start Reading the Files exist in Log Files Directory, Count them and prints to the console how many files found for processing

3- Iterate through log files, and for each of the files extract the required data and process it whether this is an initial, an incremental Run or old File the code will handle it

    -in case the data has been processed and ingested into the database, a message shows the name of the processed file, the number of processed Records in this file & Total Number of Processed Files, then Moves this Processed file to Archive directory to avoid reprocessing it again in the next execution of the code
    
    - In Case of Failure Prints a Warning Message on the console to show which file faced problems and leaves it in the same location 
    
4- Iterate through Song files to extract the required data and process them with the same strategy as the previous step

5- finally the Console Prints message to show how many records where inserted into the Song Play Table which is the Latest Loaded table which needs data from Log Files , Song Files & The Tables in the database


## How To Start Processing:-
----------------------------------
- Initial Load:

    1- Execute create_tables.py to create the Tables or relace them using this command
    
        python create_tables.py


    2- Execute etl.py to Load the Data in data/ Directory into the Database
    
        python etl.py

- Incremental Load:    

    - Execute etl.py to Load the Data in data/ Directory into the Database, whether it is New Files, or Old Files contain already loaded data in the Database with changes
    
        python etl.py