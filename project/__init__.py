from flask import Flask, jsonify, request
from pymongo import MongoClient
import string

#create the application
app = Flask(__name__)
app.config["DEBUG"] = True
app.config["TESTING"] = True


# MongoClient is defined in pymongo and used to connect with Mongo Server
#client is an instance of MongoClient 
client = MongoClient("mongodb://127.0.0.1:27017") # host uri
db = client.Twitter  # Select the database
tasks_collection = db.raw_data  # Select the collection name


# location -> It is a list and contains all the name of country
# date -> It is also a list and contains all the dates used in collection 
location=["Afghanistan",
            "Åland Islands",
            "Albania",
            "Algeria",
            "American Samoa",
            "AndorrA",
            "Angola",
            "Anguilla",
            "Antarctica",
            "Antigua And Barbuda",
            "Argentina",
            "Armenia",
            "Aruba",
            "Australia",
            "Austria",
            "Azerbaijan",
            "Bahamas",
            "Bahrain",
            "Bangladesh",
            "Barbados",
            "Belarus",
            "Belgium",
            "Belize",
            "Benin",
            "Bermuda",
            "Bhutan",
            "Bolivia",
            "Bosnia And Herzegovina",
            "Botswana",
            "Bouvet Island",
            "Brazil",
            "British Indian Ocean Territory",
            "Brunei Darussalam",
            "Bulgaria",
            "Burkina Faso",
            "Burundi",
            "Cambodia",
            "Cameroon",
            "Canada",
            "Cape Verde",
            "Cayman Islands",
            "Central African Republic",
            "Chad",
            "Chile",
            "China",
            "Christmas Island",
            "Cocos (Keeling) Islands",
            "Colombia",
            "Comoros",
            "Congo",
            "Congo",
            "Cook Islands",
            "Costa Rica",
            "Cote DIvoire",
            "Croatia",
            "Cuba",
            "Cyprus",
            "Czech Republic",
            "Denmark",
            "Djibouti",
            "Dominica",
            "Dominican Republic",
            "Ecuador",
            "Egypt",
            "El Salvador",
            "Equatorial Guinea",
            "Eritrea",
            "Estonia",
            "Ethiopia",
            "Falkland Islands, Malvinas",
            "Faroe Islands",
            "Fiji",
            "Finland",
            "France",
            "French Guiana",
            "French Polynesia",
            "French Southern Territories",
            "Gabon",
            "Gambia",
            "Georgia",
            "Germany",
            "Ghana",
            "Gibraltar",
            "Greece",
            "Greenland",
            "Grenada",
            "Guadeloupe",
            "Guam",
            "Guatemala",
            "Guernsey",
            "Guinea",
            "Guinea-bissau",
            "Guyana",
            "Haiti",
            "Heard Island And Mcdonald Islands",
            "Holy See, Vatican City State",
            "Honduras",
            "Hong Kong",
            "Hungary",
            "Iceland",
            "India",
            "Indonesia",
            "Iran",
            "Iraq",
            "Ireland",
            "Isle Of Man",
            "Israel",
            "Italy",
            "Jamaica",
            "Japan",
            "Jersey",
            "Jordan",
            "Kazakhstan",
            "Kenya",
            "Kiribati",
            "Korea",
            "Korea",
            "Kuwait",
            "Kyrgyzstan",
            "Lao People's Democratic Republic",
            "Latvia",
            "Lebanon",
            "Lesotho",
            "Liberia",
            "Libyan Arab Jamahiriya",
            "Liechtenstein",
            "Lithuania",
            "Luxembourg",
            "Macao",
            "Macedonia",
            "Madagascar",
            "Malawi",
            "Malaysia",
            "Maldives",
            "Mali",
            "Malta",
            "Marshall Islands",
            "Martinique",
            "Mauritania",
            "Mauritius",
            "Mayotte",
            "Mexico",
            "Micronesia",
            "Moldova",
            "Monaco",
            "Mongolia",
            "Montserrat",
            "Morocco",
            "Mozambique",
            "Myanmar",
            "Namibia",
            "Nauru",
            "Nepal",
            "Netherlands",
            "Netherlands Antilles",
            "New Caledonia",
            "New Zealand",
            "Nicaragua",
            "Niger",
            "Nigeria",
            "Niue",
            "Norfolk Island",
            "Northern Mariana Islands",
            "Norway",
            "Oman",
            "Pakistan",
            "Palau",
            "Palestinian Territory",
            "Panama",
            "Papua New Guinea",
            "Paraguay",
            "Peru",
            "Philippines",
            "Pitcairn",
            "Poland",
            "Portugal",
            "Puerto Rico",
            "Qatar",
            "Reunion",
            "Romania",
            "Russian Federation",
            "Rwanda",
            "Saint Helena",
            "Saint Kitts And Nevis",
            "Saint Lucia",
            "Saint Pierre And Miquelon",
            "Saint Vincent And The Grenadines",
            "Samoa",
            "San Marino",
            "Sao Tome And Principe",
            "Saudi Arabia",
            "Senegal",
            "Serbia And Montenegro",
            "Seychelles",
            "Sierra Leone",
            "Singapore",
            "Slovakia",
            "Slovenia",
            "Solomon Islands",
            "Somalia",
            "South Africa",
            "South Georgia And The South Sandwich Islands",
            "Spain",
            "Sri Lanka",
            "Sudan",
            "Suriname",
            "Svalbard And Jan Mayen",
            "Swaziland",
            "Sweden",
            "Switzerland",
            "Syrian Arab Republic",
            "Taiwan",
            "Tajikistan",
            "Tanzania",
            "Thailand",
            "Timor-leste",
            "Togo",
            "Tokelau",
            "Tonga",
            "Trinidad And Tobago",
            "Tunisia",
            "Turkey",
            "Turkmenistan",
            "Turks and Caicos Islands",
            "Tuvalu",
            "Uganda",
            "Ukraine",
            "United Arab Emirates",
            "United Kingdom",
            "United States",
            "United States Minor Outlying Islands",
            "Uruguay",
            "Uzbekistan",
            "Vanuatu",
            "Venezuela",
            "Viet Nam",
            "Virgin Islands, British",
            "Virgin Islands, United States",
            "Wallis and Futuna",
            "Western Sahara",
            "Yemen",
            "Zambia",
            "Zimbabwe"]
date_list=tasks_collection.distinct("created")


#This is the root-end point of API. Whatever be the function after this root-end that function will invoke and
#return the response to the API.
#@Params: q_no is question number or query number and str is any string  
#@Return: It return the response of to the Postman
#
#What Function does:
#It takes two parameters q_no and str. Whatever the condition is matched it invoke the corresponding function and get the result.
#Then it return the responses after parsing the result in json format. 
@app.route('/api/tasks/<int:q_no>', methods=['GET'])
def get_tasks(q_no):
      query_date=""
      country=""
      top="-1"
      if 'date' in request.args:
            query_date=request.args['date']
      if 'country' in request.args:
            country=request.args['country']
      if 'top' in request.args:
            top=request.args['top']
      if (q_no == 1 and country != ""):
            return jsonify(ques1(country))
      elif (q_no == 2 and query_date != ""):
            return jsonify(ques2(query_date))
      elif(q_no == 3 and top != "-1"):
            return jsonify(ques3(top))
      elif (q_no ==4 and top !="-1" and country !=""):
            return jsonify(ques4(top,country))
      else:
            return jsonify([])


#@Params: Country is string datatype variable.
#@Return: It return the result after performing the query. It return a list.
#
#What Function does:
#It takes one argument. Whatever be the value of the argument it perform the query and return the result.
#If the value of argument is "All" the it return the result for all country.
#If value of argument is not "All" then it only return the result for that country.
def ques1(country):
	task_list=[]
	country=string.capwords(country.lower())
	if(country == "All"):
		list1=tasks_collection.aggregate([
			{"$group":{"_id":"$location","Count":{"$sum":1}}},
			{"$project": {"Country":"$_id","Count":1,"_id":0}},
			{"$sort": {"Country":1}}])
		for doc in list1:
			task_list.append(doc)
	else:
		list1=tasks_collection.aggregate([
			{"$match":{"location":country}},
			{"$group":{"_id":"$location","Count":{"$sum":1}}},
			{"$project": {"Country":"$_id","Count":1,"_id":0}}])
		for doc in list1:
	 		task_list.append(doc)
	return task_list


#@Params: str in String datatype variable. It contains a particular date or has value "all" or "All".
#Return: It return the result after performing the query. It return a list.
#
#What Function does:
#It takes one argument and return the result after performing the query.
#If value matches with the first condition then it return the result for the all dates.
#If value matches with the second condition then it return the result for that particular date.
def ques2(str):
	task_list=[]
	#return str;
	if(str == "all" or str == "All"):
		#return str
		for x in date_list:
			list2=[]
			list1=tasks_collection.aggregate([
				{"$match":{"created":x}},
				{"$group":{"_id":"$location","Count":{"$sum":1}}},
				{"$project":{"Country":"$_id","Count":1,"_id":0}},
				{"$sort":{"Count":-1}}])
			for doc in list1:
				list2.append(doc)
			task_list.append({'Date':x,'Result':list2})
		return task_list
	else:
		list3=[]
		x=str
		#return x
		listx=tasks_collection.aggregate([
				{"$match":{"created":x}},
				{"$group":{"_id":"$location","Count":{"$sum":1}}},
				{"$project":{"Country":"$_id","Count":1,"_id":0}}])
		for doc in listx:
			list3.append(doc)
		task_list.append({'Date':x,'Result':list3})
		return task_list


#@Params: It take only one argument from API.
#@Return: It return the response for that query.
#
#What function does:
#It count the occurence of words and return the result. 
def ques3(top):
	task_list=[]
	top=int(top,10)
	cursor=tasks_collection.aggregate([
		{"$project":{"word":{"$split":["$text"," "]}}},
		{"$unwind":"$word"},
		{"$group":{"_id":"$word","Count":{"$sum":1} }},
		{"$sort":{"Count":-1}},
		{"$limit":top},
		{"$project":{"Word":"$_id","Count":1,"_id":0 }}])
	for doc in cursor:
		task_list.append(doc)
	return task_list
#@Params: It takes two arguments from API.
#@Return: It return the response for that query.
#
#What function does:
#It count the occurence of words per country and return the result. 
def ques4(top,country):
      task_list=[]
      top=int(top,10)
      country=string.capwords(country.lower())
      if(country == "all" or country == 'All'):
            for country_name in location:
                  list1=[]
                  cursor=tasks_collection.aggregate([
                        {"$match":{"location":country_name}},
                        {"$project":{"word":{"$split":["$text"," "]}}},
                        {"$unwind":"$word"},
                        {"$group":{"_id":"$word","Count":{"$sum":1} }},
                        {"$sort":{"Count":-1}},
                        {"$limit":top},
                        {"$project":{"Word":"$_id","Count":1,"_id":0 }}])
                  for doc in cursor:
                        list1.append(doc)
                  task_list.append({"Country":country_name,"Result":list1})
      else:
            cursor=tasks_collection.aggregate([
                        {"$match":{"location":country}},
                        {"$project":{"word":{"$split":["$text"," "]}}},
                        {"$unwind":"$word"},
                        {"$group":{"_id":"$word","Count":{"$sum":1} }},
                        {"$sort":{"Count":-1}},
                        {"$limit":top},
                        {"$project":{"Word":"$_id","Count":1,"_id":0 }}])
            list1=[]
            for doc in cursor:
                        list1.append(doc)
            task_list.append({"Country":country,"Result":list1})
      return task_list

                  



