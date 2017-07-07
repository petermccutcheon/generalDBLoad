# -*- coding: utf-8 -*-
#************************************************************************************************************************************************
#***** Author:		Peter McCutcheon
#***** Created:		6/10/2017
#***** Modified:
#*****
#------------------------------------------------------------------------------------------------------------------------------------------------
#***** Description:
#*****
#*****		This program loads one or more SQL tables in either MySQL or SQLite from one one or more CSV files.
#*****		The program can run interactively or it can run using a JSON file.
#*****
#*************************************************************************************************************************************************

#=====================================================================================================
#==		Import all packages here.
#=====================================================================================================

import csv
import json
import extendedInput as EI
from dbmaint import DBMaint
import getpass

exitOutString = "Q"

#=====================================================================================================
#==		Define any functions here.
#=====================================================================================================

#----------------------------------------------------
#   Function to process most of the interactive input.
#----------------------------------------------------
def processInteractiveInput():
    databaseType = EI.extendInput("Enter database type: ", {'primary': 'DTS', 'secondary': 'LOS', 'tertiary': ''}, {'list':["MySQL", "SQLite"], 'range': []}, exitOutString)
    if databaseType.upper() == exitOutString:
        return exitOutString
		
    databaseName = EI.extendInput("Enter database name: ", {'primary': 'DTS', 'secondary': '', 'tertiary': ''}, {'list': [], 'range': []}, exitOutString)
    if databaseName.upper() == exitOutString:
        return exitOutString
		
    if databaseType.upper() == "MYSQL":
        databaseUser = EI.extendInput("Enter the database username: ", {'primary': 'DTS', 'secondary': '', 'tertiary': ''}, {'list': [], 'range': []}, exitOutString)
        databasePass = getpass.getpass("Enter the database password: ")
        #databasePass = EI.extendInput("Enter the database password: ", {'primary': 'DTS', 'secondary': '', 'tertiary': ''}, {'list': [], 'range': []}, exitOutString)
    else:
        databaseUser = ""
        databasePass = ""
		
    tableName = EI.extendInput("Enter the table name: ", {'primary': 'DTS', 'secondary': '', 'tertiary': ''}, {'list': [], 'range': []}, exitOutString)
    if tableName.upper() == exitOutString:
	    return exitOutString
		
    maxExceptions = EI.extendInput("Enter the maximum exceptions allowed: ", {'primary': 'DTI', 'secondary': 'LOI', 'tertiary': 'IR'}, {'list': [-1,0], 'range': [1,'L']}, exitOutString)
    if type(maxExceptions) is str:
        if maxExceptions.upper() == exitOutString:
            return exitOutString
        else:
            print("Error--generalDBLoad-- Exceptions input is a string, but not 'Q', exiting program.")
            return exitOutString
		
    csvFilename = EI.extendInput("Enter the CSV filename: ", {'primary': 'DTS', 'secondary': '', 'tertiary': ''}, {'list': [], 'range': []}, exitOutString)
    if csvFilename.upper() == exitOutString:
	    return exitOutString
		
    return {'numberOfTables': 1, 'maxExceptions':maxExceptions, "exceptionCount": 0, 'tables':[{'databaseType':databaseType, 'databaseName':databaseName, 'databaseUser':databaseUser, 'databasePass':databasePass, 'tablename':tableName, 'insertCount': 0, 'csvName':[csvFilename], 'fieldNames':[]}]}

#----------------------------------------------------------------
#   Function to open the JSON file and load it into a dictionary.
#----------------------------------------------------------------	
def processJSONInput(jsonFile):
    print("Processing JSON file.")

    with open(jsonFile, 'r') as jFile:
        jsonData = json.load(jFile)
    
    return jsonData
    
#----------------------------------------------
#   Standard exception handler for this program.
#----------------------------------------------
def exceptHandler(mode, msg):
    if mode == "P":
        print("--generalDBLoad-- Exception: " + str(msg))
    
    return False

#=====================================================================================================
#==	Main Process
#=====================================================================================================

#
#	Get the processing type input.
#
processingTypeValid = False
while not processingTypeValid:
    processingType = input("Enter the processing Type: ")
    if processingType.upper() == "I":
        ret = processInteractiveInput()
        if ret == exitOutString:
            inputInfo = {'tables': []}
        else:
            inputInfo = ret			
        processingTypeValid = True
    else:
        if processingType.upper() == "J":
            JSONFile = input("Enter the JSON filename: ")
            ret = processJSONInput(JSONFile)
            if not ret:
                inputInfo = {'tables': []}
            else:
                inputInfo = ret			
            processingTypeValid = True
        else:
            if processingType.upper() == exitOutString:
                quit()
            else:
                print("Invalid entry: Please enter an I (Interactive), J (JSON), or Q (Quit).")
                processingTypeValid = False

#
#	Process the tables.
#
for tbl in inputInfo['tables']:
    #
    #   For the current table connect to the database.  If the connection object is not
    #   created then there is an error and the program needs to abort.
    #
    insertCount = 0
    db = DBMaint(msgMode="", dbtype=tbl['databaseType'], dbname=tbl['databaseName'], dbuser=tbl['databaseUser'], dbpass=tbl['databasePass'])
    try:
        tmp = str(db.cnx)
    except:
        #
        #   If there is an error connecting to the database then exit the program.
        #
        quit()
    #
    #   Load the field name list for this table into the dictionary data structure.
    #
    ret = db.getColumns(tbl['tablename'])
    if ret:
        tbl['fieldNames'] = ret
    else:
        #
        #   If there was an error then the program will exit.  It is possible that the
        #   table does not exist in the database.
        #
        quit()
    #
    #   Process the CSV files requested.
    #
    for csvFile in tbl['csvName']:
        print("CSV name: " + str(csvFile))
        with open(csvFile,'rt') as fin:
            cin = csv.reader(fin)
            for row in cin:
                ret = db.addTo(tbl['tablename'], tbl['fieldNames'], row)
                if not ret:
                    inputInfo['exceptionCount'] += 1
                    ret = exceptHandler("P", "There was some sort of error adding the row to the table.")
                else:
                    tbl['insertCount'] += 1

    #
    #   Close the DB connection and remove the DB object.
    #
    del db
    
#=====================================================================================================
#==	Close any open file, DBs, cursors, etc. End program
#=====================================================================================================

if len(inputInfo['tables']) != 0:
    print("")
    print("==> generalDBLoad Complete:")
    print("")
    print("----> Exception Count: " + str(inputInfo['exceptionCount']))
    print("")
    print("==> Tables processed:")
    for tbl in inputInfo["tables"]:
        print("----> Table: " + tbl['tablename'] + " rows inserted: " + str(tbl['insertCount']))