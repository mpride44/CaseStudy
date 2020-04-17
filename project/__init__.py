from flask import Flask, jsonify,json
from pymongo import MongoClient
from bson.raw_bson import RawBSONDocument
from bson import json_util

app = Flask(__name__)
app.config["DEBUG"] = True
app.config["TESTING"] = True

client = MongoClient("mongodb://127.0.0.1:27017")  # host uri
db = client.Twitter  # Select the database
tasks_collection = db.raw_data2  # Select the collection name

def ques1(location):
	task_list=[]
	list1=tasks_collection.aggregate([
		{"$match":{"created":"2020-04-15"}},
		{"$group":{"_id":"$location","Count":{"$sum":1}}},
		{"$project": {"Country":"$_id","Count":1,"_id":0}},
		{"$sort": {"Country":1}}])
	for doc in list1:
		task_list.append(doc)
	return task_list

def ques2(date):
	task_list=[]
	for x in date:
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

def ques3():
	task_list=[]
	cursor=tasks_collection.aggregate([
		{"$project":{"word":{"$split":["$text"," "]}}},
		{"$unwind":"$word"},
		{"$group":{"_id":"$word","Count":{"$sum":1} }},
		{"$sort":{"Count":-1}},
		{"$limit":100},
		{"$project":{"Word":"$_id","Count":1,"_id":0 }}])
	for doc in cursor:
		task_list.append(doc)
	return task_list

@app.route('/api/tasks/<int:q_no>', methods=['GET'])
def get_tasks(q_no):
	location =["Afghanistan","Åland Islands","Albania","Algeria","American Samoa","AndorrA","Angola","Anguilla","Antarctica","Antigua and Barbuda","Argentina","Armenia","Aruba","Australia","Austria",
				"Azerbaijan","Bahamas","Bahrain","Bangladesh","Barbados","Belarus","Belgium","Belize","Benin","Bermuda","Bhutan","Bolivia","Bosnia and Herzegovina","Botswana","Bouvet Island","Brazil",
				"British Indian Ocean Territory","Brunei Darussalam","Bulgaria","Burkina Faso","Burundi","Cambodia","Cameroon","Cote DIvoire",
				"Canada","Cape Verde","Cayman Islands","Central African Republic","Chad","Chile","China","Christmas Island","Cocos (Keeling) Islands","Colombia","Comoros","Congo","Cook Islands","Costa Rica",
				"Croatia","Cuba","Cyprus","Czech Republic","Denmark","Djibouti","Dominica","Dominican Republic","Ecuador","Egypt","El Salvador","Equatorial Guinea","Eritrea","Estonia","Ethiopia","Falkland Islands (Malvinas)",
				"Faroe Islands","Fiji","Finland","France","French Guiana","French Polynesia","French Southern Territories","Gabon","Gambia","Georgia","Germany","Ghana","Gibraltar","Greece","Greenland","Grenada","Guadeloupe",
				"Guam","Guatemala","Guernsey","Guinea","Guinea-Bissau","Guyana","Haiti","Heard Island and Mcdonald Islands","Holy See (Vatican City State)","Honduras","Hong Kong","Hungary","Iceland","India","Indonesia",
				"Iran","Iraq","Ireland","Isle of Man","Israel","Italy","Jamaica","Japan","Jersey","Jordan","Kazakhstan","Kenya","Kiribati","Korea","Korea","Kuwait","Kyrgyzstan","Lao PeopleS Democratic Republic","Latvia",
				"Lebanon","Lesotho","Liberia","Libyan Arab Jamahiriya","Liechtenstein","Lithuania","Luxembourg","Macao","Macedonia","Madagascar","Malawi","Malaysia","Maldives","Mali","Malta","Marshall Islands","Martinique"]
	date=tasks_collection.distinct("created")
	task_list = []
	if (q_no == 1):
		return jsonify(ques1(location))
	elif (q_no == 2):
		return jsonify(ques2(date))
	elif (q_no == 3):
		return jsonify(ques3())
		