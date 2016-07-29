#!/usr/bin/env python
# -*- coding: utf-8 -*-
# So, the problem is that the gigantic file is actually not a valid XML, because
# it has several root elements, and XML declarations.
# It is, a matter of fact, a collection of a lot of concatenated XML documents.
# So, one solution would be to split the file into separate documents,
# so that you can process the resulting files as valid XML documents.

import xml.etree.ElementTree as ET
import pprint
import re
from collections import defaultdict
from pymongo import MongoClient


MAPFILE = 'seoul_south-korea.osm'

#clinic_type_ignore = re.compile(r'[^,&\(\)/\-;\s]+', re.IGNORECASE)

problemchars = re.compile(r'[=\+/&<>;\'"\?%#$@\,\. \t\r\n]')

mapping = { "hanuiwon": "Oriental Medicine",
			"yeoseongbyeongwon" : "Obstetrics & Gynecology",
			"yeoseonguiwon": "Obstetrics & Gynecology",
			"yeoseong": "Obstetrics & Gynecology",
			"soagwa": "Pediatric",
			"singyeongoegwa" : "Neurosurgery",
			"singyeong" : "Neurosurgery",
			"jeonghyeongoegwa" : "Orthopedics",
			"gajeonguihak": "Family Medicine",
			"jaehwaluihakgwa": "Rehabilitation Medicine",
			"tongjeunguihakgwa": "Pain Medicine",
			"tongjeung": "Pain Medicine",
			"dentist": "Dental",
			"hopital": "Hospital",
			"hostpital": "Hospital"
			}

def update_hospital_name(hospitals):

	data = {}
	for h in hospitals:
		old_hospital_name = h["tags"]["name:en"]
		new_hospital_enname = update_name(old_hospital_name, mapping)
		if old_hospital_name != new_hospital_enname:
			new_hospital_enname = 	update_clinic(new_hospital_enname)
			data[h["id"]] = new_hospital_enname
			print old_hospital_name, "=>", new_hospital_enname
	print
	print "Total", hospitals.count(), "documents: ", len(data), "documents to be updated..."
	return data

def update_clinic(name):
	clinic_re = re.compile(r'clinic|hospital', re.IGNORECASE)
	if clinic_re.search(name) is None: 
		name = name + " Clinic"
	return name

def update_name(name, mapping):
	for m in mapping:
		err_re = re.compile(r''+m, re.IGNORECASE)
		if err_re.search(name):
			name = re.sub(r''+m, ' '+mapping[m]+' ', name, flags=re.IGNORECASE)
	return name

def get_data(osmfile):
	print "Parsing and cleaning osm..."
	osm_file = open(osmfile, "r")
	clinics = []
	for event, elem in ET.iterparse(osm_file, events=("start",)):
		if elem.tag == "node" or elem.tag == "way":
			data = {}
			for attr in elem.attrib:
				data[attr] = elem.attrib[attr]

			data["tags"] = {}
			for tag in elem.iter("tag"):
				k = "{}".format(tag.attrib['k'].encode('utf-8'))
				v = "{}".format(tag.attrib['v'].encode('utf-8'))
				# skip problem chars
				if problemchars.search(k):
					break
				# strip postal code
				if k == "addr:postcode":
					v = strip_postalcode(v)

				data["tags"][k] = v
			clinics.append(data)

	return clinics

def insert_data(data, db):
	print "Insert data..."
	db.seoul.insert(data)

def update_db(data, db):
	print "Update data..."
	for d in data:
		db.seoul.update({'id':d}, {'$set': {"tags":{"name:en":data[d]}}})

def find(db, pipeline):
	print "Find data...", pipeline
	result = db.seoul.find(pipeline)
	return result

def aggregate(db, pipeline):
	print "Aggregate data...", pipeline
	result = db.seoul.aggregate(pipeline)
	return result

def make_pipeline_postcode():
	# complete the aggregation pipeline
	pipeline = [
	{"$match":{"tags.addr:postcode": {"$exists":1}} } ,
	{"$group": {"_id":"$tags.addr:postcode" , "count":{"$sum":1}}},
	{"$sort" : {  "count": -1  } }] 
	return pipeline

def make_pipeline_city():
	pipeline = [{"$match":{"tags.addr:city":{"$exists":1}}}, 
	{"$group":{"_id":"$tags.addr:city", "count":{"$sum":1}}}, 
	{"$sort":{"count":1}}]
	return pipeline

def make_query():
	pipeline = {"tags.amenity": "hospital", "tags.name:en" : {"$exists": 1}}
	return pipeline

def strip_postalcode(v):
	return re.sub(r'-', '', v)

def test():
	client = MongoClient("mongodb://localhost:27017")
	db = client.seoul

	result = db.seoul.delete_many({})
	if result.deleted_count > 0:
		print result.deleted_count, " is deleted..."

	print "Reading XML.."
	data = get_data(MAPFILE)
	insert_data(data, db)

	pipeline = make_pipeline_postcode()
	result = aggregate(db, pipeline)

	pipeline = make_pipeline_city()
	result = aggregate(db, pipeline)
	

	pipeline = make_query()
	result = find(db, pipeline)

	new_hospital_names = update_hospital_name(result)
	update_db(new_hospital_names, db)
	
	print "... success!"

if __name__ == "__main__":
	test()