import pandas
import csv
from dateutil import parser
from datetime import datetime
from time import time
import pickle


class paymo:


	## Initiliaze the class object with the payment information given to us
	## in the fileName. Since reading the file takes a long time we have an option to
	## serialize previosuly read data and read it back in. We call it the shortcut option.
	## this is off by default.
	def __init__ (self, fileName, shortcut = True):
		if shortcut:
			self.idList = [''] * 77360
			self.idList = pickle.load(open('paymo_input/idList_saved.txt', 'rb'))
		else:
			self.idList = [''] * (self.getmaxUsers(fileName) + 1)
			self.createFList(fileName, self.idList)
			pickle.dump(self.idList, open('paymo_input/idList_saved.txt', 'wb'))
		self.createSOF(self.idList)


	## This finds the maximum number of users in the payment network. This is needed in order
	## initialize the size of the adjacency list. The list can be grown dynamically as more users are added
	def getmaxUsers(self, fileName):
		start = time()
		with open(fileName, "rU") as csvfile:
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
		end = time()
		print "Found max number of users in ", end - start

	## Each user is part of a clique. We maintain the clique number that the user is in here.
	## Two users that dont share a clique are reported as unverified.
	## This is a generator function.
	def getNewNetworkId(self):
	    i = 0
	    while True:
	        yield i
	        i += 1

	## read the file and create the adjacency list for each user/vertex. Adjacency list saves
	## memory and has a constant time lookup.
	def createFList(self, fileName, idList):
		start = time()
		newIdGen = self.getNewNetworkId()
		with open(fileName, "rU") as csvfile:
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
					idList[id2]['nid'] |= idList[id1]['nid']
					idList[id1]['nid'] = idList[id2]['nid']
				except:
					idList[id2] = {1: {id1}, 2: set(), 4: set(), 'nid': idList[id1]['nid']}
		end = time()
		print "Completed First order friends adjacency list in ", end - start

	## Find the second order friends of each user/vertex. Finding this upfront in a batch processing
	## is fast enough (20 seconds) and cuts down on lookup during runtime.
	def createSOF(self, idList):
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
		print "Completed Second order friends adjacency list in ", end - start


	## This checks if the two users are adjacent to each other. In this case we made assumption that
	## id1 -> payment -> id2. Thus we only check in the adjacency list of id1.
	def checkFList(self, id1, id2):
		if id1 >= len(self.idList):
			return False
		try:
			if not id2 in self.idList[id1][1]:
				#pass
				#print id1, "(payer) has never had a transaction with (payee)", id2, ". Do you want to proceed?"
				return False
		except:
			if self.idList[id1] == '':
				return False
			else:
				print "PROBLEM: Something went wrong: ", row
		return True

		

	## Check for friend of friends relationship. 
	def checkSOF(self, id1, id2):
		if id1 >= len(self.idList):
			return False
		try:
			if not id2 in self.idList[id1][2]:
				#pass
				#print id1, "(payer) has never had a transaction with (payee)", id2, ". Do you want to proceed?"
				return False
		except:
			if self.idList[id1] == '':
				#pass
				#print id1, "(payer) has never had a transaction with (payee)", id2, ". Do you want to proceed?"
				return False
			else:
				print "PROBLEM: Something went wrong: ", row
		return True


	## Check for third order and fourth order degree of friends
	def checkFOF(self, id1, id2):
		if id1 >= len(self.idList):
			return False
		try:
			if not set.intersection(self.idList[id1]['nid'], self.idList[id2]['nid']):
				#pass
				#print id1, "(payer) has never had a transaction with (payee)", id2, ". Do you want to proceed?"
				return False
			else:
			        temp = set([])
	                        #start = time()
	                        for j in self.idList[id1][2]:
	                                temp |= self.idList[j][2]
	                                temp |= self.idList[j][1]
	                                #print "TEMP = ", temp
				self.idList[id1][4] |= temp
	                        #end = time()
	                        #print "Completed in ", end - start
				if not id2 in self.idList[id1][4]:
					return False
		except:
			return False
		return True


		
fileName1 = "paymo_input/batch_payment.txt"
fileName2 = "paymo_input/stream_payment.txt"
#fileName1 = "paymo_input/temp.csv"
#fileName2 = "paymo_input/temp.csv"
outFile1 = "paymo_output/output1.txt"
outFile2 = "paymo_output/output2.txt"
outFile3 = "paymo_output/output3.txt"
out1 = open(outFile1, 'w')
out2 = open(outFile2, 'w')
out3 = open(outFile3, 'w')


## Create a payment object for payment network
#paymoObj = paymo(fileName1, True)
paymoObj = paymo(fileName1, False)

## Open streaming processing file
with open(fileName2, "rU") as csvfile:
    datareader = csv.reader(csvfile)
    for row in datareader:
	try:
        	_ = parser.parse(row[0])
        except:
                continue
        id1 = int(row[1])
        id2 = int(row[2])
        unverified = True
        if not paymoObj.checkFList(id1, id2):
        	out1.write('unverified\n')
        else:
        	out1.write('trusted\n')
        	unverified = False
        if unverified and not paymoObj.checkSOF(id1, id2):
        	out2.write('unverified\n')
        else:
        	out2.write('trusted\n')
        	unverified = False
        if unverified and not paymoObj.checkFOF(id1, id2):
        	out3.write('unverified\n')
        else:
        	out3.write('trusted\n')

#for k in range(len(paymoObj.idList)):
#	print k, paymoObj.idList[k]

out1.close()
out2.close()
out3.close()
