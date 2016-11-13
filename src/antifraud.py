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
		return False
	try:
		if not id2 in idList[id1][1]:
			#pass
			#print id1, "(payer) has never had a transaction with (payee)", id2, ". Do you want to proceed?"
			return False
	except:
		if idList[id1] == '':
			return False
		else:
			print "PROBLEM: Something went wrong: ", row
	return True

def getNewNetworkId():
        i = 0
        while True:
                yield i
                i += 1

def checkSOF(id1, id2):
	if id1 >= len(idList):
		return False
	try:
		if not id2 in idList[id1][2]:
			#pass
			#print id1, "(payer) has never had a transaction with (payee)", id2, ". Do you want to proceed?"
			return False
	except:
		if idList[id1] == '':
			#pass
			#print id1, "(payer) has never had a transaction with (payee)", id2, ". Do you want to proceed?"
			return False
		else:
			print "PROBLEM: Something went wrong: ", row
	return True

def checkFOF(id1, id2):
	if id1 >= len(idList):
		return False
	try:
		if not set.intersection(idList[id1]['nid'], idList[id2]['nid']):
			#pass
			#print id1, "(payer) has never had a transaction with (payee)", id2, ". Do you want to proceed?"
			return False
		else:
		        temp = set([])
                        start = time()
                        for j in idList[id1][2]:
                                temp |= idList[j][2]
                                temp |= idList[j][1]
                                #print "TEMP = ", temp
			idList[id1][4] |= temp
                        end = time()
                        #print "Completed in ", end - start
			if not id2 in idList[id1][4]:
				return False
	except:
			return False
	return True


def createFList(filename, idList):
	start = time()
	newIdGen = getNewNetworkId()
	with open(filename, "rU") as csvfile:
		datareader = csv.reader(csvfile)
		for row in datareader:
			try:
				_ = parser.parse(row[0])
			except:
				continue
			id1 = int(row[1])
			id2 = int(row[2])
			try:
				idList[id1][1].add(id2)
			except:
				if idList[id2] == '':
					newId = {newIdGen.next()}
				else:
					newId = idList[id2]['nid']
				idList[id1] = {1: {id2}, 2: set(), 4: set(), 'nid': newId}
			try:
				idList[id2][1].add(id1)
			except:
				idList[id2] = {1: {id1}, 2: set(), 4: set(), 'nid': idList[id1]['nid']}
	end = time()
	print "Completed in ", end - start




def createSOF(idList):
	start = time()
	for i in xrange(len(idList)):
		temp = set([])
		try:
			for j in idList[i][1]:
				temp |= idList[j][1]
		except:
			continue
		idList[i][2] |= temp
	end = time()
	print "Completed in ", end - start




## def createFOF(idList):
## 	for i in xrange(10):
## 	#for i in xrange(len(idList)):
## 		temp = set([])
## 		try:
## 			start = time()
## 			for j in idList[i][2]:
## 				temp |= idList[j][2]
## 				temp |= idList[j][1]
## 				#print "TEMP = ", temp
## 			end = time()
## 			print "Completed in ", end - start
## 		except:
## 			print "PROBLEM IDLIST", idList[i]
## 			continue
## 		#print "IDLIST PRE = ", idList[i][4]
## 		idList[i][4] |= temp
## 		#print "IDLIST AFT = ", idList[i][4]


#idList = [''] * (getmaxUsers() + 1)
#filename = "paymo_input/batch_payment.csv"
#idList = [''] * 77360
idList = [''] * 100
filename = "paymo_input/temp.csv"
createFList(filename, idList)
#pickle.dump(idList, open('idList_saved.txt', 'wb'))
#idList = pickle.load(open('paymo_input/idList_saved.txt', 'rb'))
createSOF(idList)
#createFOF(idList)
with open("paymo_input/temp.csv", "rU") as csvfile:
#with open("paymo_input/stream_payment.csv", "rU") as csvfile:
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
		#print "checked in ", end - start
		start = time()
		print "for", id1, "-->", id2, checkSOF(id1, id2)
		end = time()
		#print "checked in ", end - start
		start = time()
		print "for", id1, "-->", id2, checkFOF(id1, id2)
		end = time()
		#print "checked in ", end - start

for i in xrange(len(idList)):
	print i, "    ", idList[i]
