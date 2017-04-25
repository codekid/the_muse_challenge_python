
import urllib2
import json
import requests
import MySQLdb

conn = MySQLdb.connect(host= "localhost",
              user="db_user",
              passwd="password",
              db="the_muse_challenge")




# Connect to the databse, insert data and commit changes
def insertData(
	jobDesc		,
	jobName				,
	jobType				,
	jobPubDate			,
	jobShortName		,
	jobModelType		,
	jobId				,
	jobLocationName		,
	jobCategoryName		,
	jobLevels			,
	jobLevelsName		,
	jobTags				,
	jobLandingPage		,
	jobCompanyId		,
	jobCompanyShortName	,
	jobCompanyName		):

	global conn

	sql=""
	x = conn.cursor()
	print "preparing sql"
	try:
		
		# prepare the SQL statements
		sql = "INSERT INTO STAGING_JOBS (CONTENTS, NAME, TYPE, PUBLICATION_DATE, SHORT_NAME, MODEL_TYPE, ID, LOCATION_NAME, CATEGORY_NAME, LEVEL_NAME, LEVEL_SHORT_NAME, REFS_LANDING_PAGE, COMPANY_ID, COMPANY_SHORT_NAME, COMPANY_NAME) VALUES( %s, %s, %s, REPLACE(REPLACE(%s,'T',' '),'Z',' '), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
		params = [
			jobDesc.encode('utf-8')				,
			jobName.encode('utf-8')				,
			jobType.encode('utf-8')				,
			jobPubDate			,
			jobShortName		,
			jobModelType		,
			jobId				,
			jobLocationName		,
			jobCategoryName		,
			jobLevels			,
			jobLevelsName		,
			# jobTags				,
			jobLandingPage		,
			jobCompanyId		,
			jobCompanyShortName	,
			jobCompanyName
			]
		
		# execute the statement
		x.execute(sql, params)
		conn.commit()
	except Exception, e:
		
		print "An error occured: %s" % e
		conn.rollback()

# Concatenate the values in the JSON object
# Eg.
# - categories: [
#	 	{
#	 		name: "human resources"
#	 	}
#	 	{
#	 		name: "Human Resources"
#	 	}
#	 ]
# Final output string will be returned as "human resources,Human Resources"
def concatJSON(jsonObj, key):

	#check if the object is empty
	isJsonEmpty = jsonKeyExists(jsonObj, key)
	# isJsonEmpty = jsonObj.has_key(key)
	text = ''

	if (isJsonEmpty == ''):
		
		for count,i in enumerate(jsonObj):
			
			if (count == 0):
				text = i[key]
			else:
				text = text + "," + i[key]

		return text
	else :
		return text

# Checks if there is data in inner arrays
# Eg.
# - categories: [
#	 	{
#	 		name: "hr name" <-- checks if this inner section isn't empty
#	 	}
#	 ]
def jsonKeyExists(jsonObject, key):

	if (key in jsonObject):

		return jsonObject[key]
	else:
		return ''


def printResults(data):

	# load the json data into a variable to be accessed
	theJSON = json.loads(data)

	# print theJSON["results"][0]["locations"][0]["name"]

	# loops through the data assigns
	# assigns each item of data to a variable then inserts it into the database
	for i in theJSON["results"]:
		
		jobDesc = i["contents"]
		jobName = i["name"]
		jobType = i["type"]
		jobPubDate = i["publication_date"]
		jobShortName = i["short_name"]
		jobModelType = i["model_type"]
		jobId = i["id"]
		
		# jobLocationName = i["locations"][0]["name"]
		jobLocationName 	= concatJSON(i["locations"], "name")
		jobCategoryName 	= concatJSON(i["categories"], "name")
		jobLevels 			= concatJSON(i["levels"],"name")
		jobLevelsName 		= concatJSON(i["levels"],"short_name")
		jobTags 			= concatJSON(i,"tags")
		jobLandingPage 		= jsonKeyExists(i["refs"],"landing_page")
		jobCompanyId 		= jsonKeyExists(i["company"],"id")
		jobCompanyShortName = jsonKeyExists(i["company"],"short_name")
		jobCompanyName 		= jsonKeyExists(i["company"],"name")

		print "about to insert data into Db"
		# pass data to function that will insert records
		insertData(jobDesc	,
		jobName				,
		jobType				,
		jobPubDate			,
		jobShortName		,
		jobModelType		,
		jobId				,
		jobLocationName		,
		jobCategoryName		,
		jobLevels			,
		jobLevelsName		,
		jobTags				,
		jobLandingPage		,
		jobCompanyId		,
		jobCompanyShortName	,
		jobCompanyName		)

def menu():

	choice = 0
	pageNumber = 0
	while (choice != 5):
		print "##################################################################################"
		print "#                        What would you like to do?                              #"
		print "##################################################################################"
		print "#1. 	Insert all Data from site                                               #"
		print "#2. 	Insert a single page from site                                          #"
		print "#3. 	Answer question with data in database(ensure all data is inserted first)#"
		print "#4. 	Delete all data                                                         #"
		print "#5. 	Exit                                                                    #"
		print "##################################################################################"

		while True:
			try:
				choices = raw_input()
				print choices

				choice = int(choices)
				break
			except ValueError:
				print "you can onnly enter a number"


		if (choice == 1):
		
			print "Inserting all data"
			insertAllData()
		elif (choice == 2):
			

			while True:
				
				try:
					
					print "What page do you want to extract from the site?"
					page = raw_input()
				
					pageNumber = int(page)
					break
				except ValueError:
					print "You can only enter a number"

			insertSinglePageData(pageNumber)
		elif (choice == 3):
		
			print "Answering Question"
			answerQuery()
		elif (choice == 4):
		
			print "Delete all data"
			deleteAllData()
		elif (choice == 5):
			
			print "Exiting..."
		else :

			print "Unknown Option."

def insertSinglePageData(pageNumber):

	# define a variable to hold the source URL
  	# feed lists the jobs available by companies on the muse website
	url = "https://api-v2.themuse.com/jobs?page=%d" % pageNumber
	urlData = urllib2.Request(url, headers={'User-Agent' : "Magic Browser"})

	# Open the url and read data
	webUrl = urllib2.urlopen(urlData)
	print " the code returned is " + str(webUrl.getcode())

	if (webUrl.getcode() == 200):
		
		data = webUrl.read()
		print "got data"
		printResults(data)
	else:
	  	
	  	print "Received an error from the sever, cannot retrieve results " +str(webUrl.getcode())


# get the maximum number of pages of data at source
# a request to the site for data returns a JSON object that tells you how many pages there are
# this will be a tag called page_count	
def returnMaxPageCount():

	# Collect only the first page's result.
	url = "https://api-v2.themuse.com/jobs?page=0"
	urlData = urllib2.Request(url, headers={'User-Agent' : "My Brower"})

	# Open the url and read data
	webUrl = urllib2.urlopen(urlData)

	if (webUrl.getcode() == 200):
		
		data = webUrl.read()
		theJSON = json.loads(data)
		maxPageCount  = theJSON["page_count"]
		return maxPageCount
	else:
		print "Received an error from the sever, cannot retrieve results " +str(webUrl.getcode())


# Insert all records from source into the Db
def insertAllData():

	# Get the max number of pages
	maxPageCount = returnMaxPageCount()

	# loop through the number of pages and insert the data
	for i in range(0,maxPageCount):
		
		insertSinglePageData(i)
		print i


# Deletes all data from the STAGING_JOBS table in the Db
def deleteAllData():

	global conn

	try:
		
		x = conn.cursor()

		sql = "DELETE FROM `STAGING_JOBS`"

		# execute the statement
		answer = x.execute(sql)
		conn.commit()

		print "Delete Complete!"

	except Exception, e:
		
		print "An error occured: %s" % e
		conn.rollback()

	
# Answers the question that was given in the assignment
def answerQuery():

	global conn

		
	try:
		
		x = conn.cursor()

		sql = "SELECT COUNT(*)RECORD_COUNT FROM `STAGING_JOBS` WHERE PUBLICATION_DATE BETWEEN '2016-09-01 00:00:01' AND '2016-09-30 23:59:59' AND  LOCATION_NAME = 'New York City Metro Area'"

		# execute the statement
		answer = x.execute(sql)
		conn.commit()

		print "How many jobs with the location ""New York City Metro Area"" were published from September 1st to 30th 2016? That would be: %d" % answer
	except Exception, e:
		
		print "An error occured: %s" % e
		conn.rollback()
	


def main():
	
	menu();


if __name__ == "__main__":
	
	menu()
	# main()
