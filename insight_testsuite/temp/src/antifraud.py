import pandas
import csv
from dateutil import parser
from datetime import datetime
from time import time
import pickle


#for chunnk in pd.read_csv ("digital-wallet/paymo_input/temp.csv", usecols=fields,header=0, skipinitialspace=True, chunksize=10000):

def getmaxUsers():
	with open("paymo_input/batch_payment.csv", "rU") as csvfile:
		datareader = csv.reader(csvfile)
		max_id = 0
		for row in datareader:
			try:
				_ = parser.parse(row[0])
			except:
				continue
        		id1 = int(row[1])
        		id2 = int(row[2])
			if max(id1,id2) > max_id:
				max_id =  max(id1,id2)
	return max_id


def checkFList(id1, id2):
	if id1 >= len(idList):
		return "unverified"
	try:
		if not id2 in idList[id1][1]:
			#pass
			#print id1, "(payer) has never had a transaction with (payee)", id2, ". Do you want to proceed?"
			return "unverified"
	except:
		if idList[id1] == '':
			#pass
			#print id1, "(payer) has never had a transaction with (payee)", id2, ". Do you want to proceed?"
			return "unverified"
		else:
			print "PROBLEM: Something went wrong: ", row
	return "trusted"


def checkSOF(id1, id2):
	if id1 >= len(idList):
		return "unverified"
	try:
		if not id2 in idList[id1][2]:
			#pass
			#print id1, "(payer) has never had a transaction with (payee)", id2, ". Do you want to proceed?"
			return "unverified"
	except:
		if idList[id1] == '':
			#pass
			#print id1, "(payer) has never had a transaction with (payee)", id2, ". Do you want to proceed?"
			return "unverified"
		else:
			print "PROBLEM: Something went wrong: ", row
	return "trusted"

def checkFOF(id1, id2):
	if id1 >= len(idList):
		return "unverified"
	try:
		if not id2 in idList[id1][4]:
			#pass
			#print id1, "(payer) has never had a transaction with (payee)", id2, ". Do you want to proceed?"
			return "unverified"
	except:
		if idList[id1] == '':
			#pass
			#print id1, "(payer) has never had a transaction with (payee)", id2, ". Do you want to proceed?"
			return "unverified"
		else:
			print "PROBLEM: Something went wrong: ", row
	return "trusted"


def createFList(filename, idList):
	start = time()
	newId = getNewNetworkId()
	with open(filename, "rU") as csvfile:
		datareader = csv.reader(csvfile)
		for row in datareader:
			try:
				_ = parser.parse(row[0])
			except:
				continue
			id1 = int(row[1])
			id2 = int(row[2])
			#if id1 == 34 or id2 == 34:
			#	print "34 First"
			#	print idList[34]
			try:
				idList[id1][1].add(id2)
			except:
				idList[id1] = {1: set([id2]), 2: set([]), 4: set([newId.next()])}
			try:
				idList[id2][1].add(id1)
			except:
				idList[id2] = {1: set([id1]), 2: set([]), 4: set([newId.next()])}
			#if id1 == 34 or id2 == 34:
			#	print "34 Second"
			#	print idList[34]
	end = time()
	print "Completed in ", end - start


def getNewNetworkId():
        i = 0
        while True:
                yield i
                i += 1


def createSOF(idList):
	start = time()
	for i in xrange(len(idList)):
		temp = set([])
		try:
			for j in idList[i][1]:
				temp |= idList[j][1]
		except:
			#idList[i] = {1: set([]), 2: set([]), 4: set([])}
			print "PROBLEM IDLIST", idList[i]
			continue
		idList[i][2] |= temp
	end = time()
	print "Completed in ", end - start

def createFOF(idList):
	for i in xrange(10):
	#for i in xrange(len(idList)):
		temp = set([])
		try:
			start = time()
			for j in idList[i][2]:
				temp |= idList[j][2]
				temp |= idList[j][1]
				#print "TEMP = ", temp
			end = time()
			print "Completed in ", end - start
		except:
			print "PROBLEM IDLIST", idList[i]
			continue
		#print "IDLIST PRE = ", idList[i][4]
		idList[i][4] |= temp
		#print "IDLIST AFT = ", idList[i][4]


#idList = [''] * (getmaxUsers() + 1)
#filename = "paymo_input/batch_payment.csv"
#idList = [''] * 77360
idList = [''] * 100
filename = "paymo_input/temp.csv"
createFList(filename, idList)
#pickle.dump(idList, open('idList_saved.txt', 'wb'))
idList = pickle.load(open('paymo_input/idList_saved.txt', 'rb'))
createSOF(idList)
createFOF(idList)
with open("paymo_input/stream_payment.csv", "rU") as csvfile:
        datareader = csv.reader(csvfile)
        for row in datareader:
		try:
        		_ = parser.parse(row[0])
        	except:
                	continue
        	id1 = int(row[1])
        	id2 = int(row[2])
		start = time()
		print "for", id1, "-->", id2, checkFList(id1, id2)
		end = time()
		print "checked in ", end - start
		start = time()
		print "for", id1, "-->", id2, checkSOF(id1, id2)
		end = time()
		print "checked in ", end - start
		start = time()
		print "for", id1, "-->", id2, checkFOF(id1, id2)
		end = time()
		print "checked in ", end - start
