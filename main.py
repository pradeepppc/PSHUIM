from pyspark import SparkContext, SparkConf
from collections import defaultdict
from Transaction import Transaction
from operator import add
from pEFIM import pEFIM
import sys
import time

def defaultvalue():
    return []

# build a neighborhood dictionary from neighborhood file
def BuildNeighborhoodFile(file):
    global nbh
    f = open(file, 'r')
    line = f.readline()
    while line:
        line = line.strip().split(' ')
        neighbors = [int(x) for x in line[1:]]
        nbh[int(line[0])] = set(neighbors)
        line = f.readline()
    f.close()

def buildTransaction(line):
#     gets a input line and builds a transaction with the line
    line = line.strip().split(':')
    items = line[0].strip().split(' ')
    items = [int(item) for item in items]
    twu = float(line[1])
    utilities = line[2].strip().split(' ')
    utilities = [float(utility) for utility in utilities]
    pmus = line[3].strip().split(' ')
    pmus = [float(pmu) for pmu in pmus]
    # creating a transaction
    transaction = Transaction(items, utilities, twu, pmus)
    return transaction

def getFileStats(transactions):
    transactionUtilities = transactions.flatMap(lambda x: [x.getTransactionUtility()])
    totalutility = transactionUtilities.reduce(add)
    datasetLen = len(transactionUtilities.collect())
    return {
        'len' : datasetLen,
        'totalUtility' : totalutility
    }

# this function not only revises the transaction but also calculates the NSTU value of each secondary item
def reviseTransactions(transaction):
    transaction.removeUnpromisingItems(oldNamesToNewNames_broadcast.value)
    return transaction

# calculates the neighborhood subtree utility of secondary items
def calculateNSTUFirstTime(transaction):
    # secondary items
    secondaryItems = list(oldNamesToNewNames_broadcast.value.keys())
    items = transaction.getItems()
    utilities = transaction.getUtilities()
    itemsUtilityList = []
    for idx, item in enumerate(items):
        i = idx + 1
        sum_utility = utilities[idx] 
        while(i < len(items)):
            if items[i] in nbhNew_broadcast.value[item]:
                sum_utility += utilities[i]
            i += 1
        itemsUtilityList.append((item, sum_utility))
    return itemsUtilityList

# this function just collects the transaction and prints the items and utilities present in the transaction
def printTransactions(transactions):
    for transaction in transactions.collect():
        print('transaction start')
        print(transaction.getItems())
        print(transaction.getUtilities())
        print('transaction ends') 

# divides the items between the partitions based on certain techniques
def divideItems(items, numPartitions, partitionType):
    itemNode = {}
    NodeToItemMap = {}
    for i in range(numPartitions):
        NodeToItemMap[i] = []
    if partitionType == 'lookup':
        i = 0
        inc = 1
        flag = False
        for item in items:
            itemNode[item] = i
            NodeToItemMap[i].append(item)
            i += inc
            if (i == 0) or (i == numPartitions -1):
                if flag:
                    if i == 0:
                        inc = 1
                    else:
                        inc = -1
                    flag = False
                else:
                    inc = 0
                    flag = True
    for i in range(numPartitions):
        NodeToItemMap[i] = set(NodeToItemMap[i])
        
    return itemNode, NodeToItemMap

def defaultBooleanValue():
    return False

def mapTransaction(transaction):
    items = transaction.getItems()
    utilities = transaction.getUtilities()
    totalUtility = transaction.getTransactionUtility()
    mapItemToNodeID = itemToNodeMap_broadcast.value
    mapNodeID = defaultdict(defaultBooleanValue)
    transactionList = []
    cumulativeUtility = 0
    primaryItems = list(mapItemToNodeID.keys())
    for idx, item in enumerate(items):
        if item not in primaryItems:
            cumulativeUtility += utilities[idx]
            continue
        nodeID = mapItemToNodeID[item]
        # if this transaction is not assigned to the node 
        if not mapNodeID[nodeID]:
            # create a new transaction
            newTransaction = Transaction(items[idx:], utilities[idx:], totalUtility - cumulativeUtility)
            transactionList.append((nodeID, newTransaction))
            mapNodeID[nodeID] = True
        cumulativeUtility += utilities[idx]
    return transactionList

def parllelEFIM(nodeData):
    currNode = nodeData[0]
    transactions = nodeData[1]
    primaryItems = nodeToItemsMap_broadcast.value
    primaryItems = primaryItems[currNode]
    mapItemsToNeighbors = nbh_broadcast.value
    minUtil = minUtil_broadcast.value
    oldNamesToNewNames = oldNamesToNewNames_broadcast.value
    newNamesToOldNames = newNamesToOldNames_broadcast.value
    secondaryItems = list(newNamesToOldNames.keys())
    pefim = pEFIM(mapItemsToNeighbors, minUtil, primaryItems, secondaryItems, transactions, newNamesToOldNames, oldNamesToNewNames)
    output = pefim.runAlgo()
    return output

if __name__ == '__main__':
	# a dictionary that maps the items to its neighbors
	nbh = defaultdict(defaultvalue)

	# variables used in the algorithm	
	APP_NAME = "PSHUIM"
	conf = SparkConf().setAppName(APP_NAME)
	sc = SparkContext(conf=conf)
	inputfile = sys.argv[1]
	nbhfile = sys.argv[2]
	outfile = sys.argv[3]
	numPartitions = int(sys.argv[5])
	minUtil = int(sys.argv[4])
	partitionType = 'lookup'

	BuildNeighborhoodFile(nbhfile)

	startTime = time.time()

	# reading the data from the text file and transorfming each line into a transaction
	transactions = sc.textFile(inputfile, numPartitions).map(lambda x : buildTransaction(x))
	transactions.persist()

	# compute the statistics of the database
	filestats = getFileStats(transactions)

	# calculate the pmu value for each item present in the database
	pmuDict = dict(transactions.flatMap(lambda x: [item for item in zip(x.getItems(), x.getPmus())]).reduceByKey(add).filter(lambda x: x[1] >= minUtil).collect())

	# the keys in the dictionary are the items which we keep in the database we call them as primary items
	secondaryItems = list(pmuDict.keys())

	# sorting the primary keys in increasing order of their PMU values
	secondaryItems.sort(key = lambda x: pmuDict[x])

	# give new names to the items based upon their ordering starting from 1
	oldNamesToNewNames = {} # dictionary for storing the mappings from old names to new names
	newNamesToOldNames = {} # dictionary to map from new names to old names
	currentName = 1
	for idx, item in enumerate(secondaryItems):
	    oldNamesToNewNames[item] = currentName
	    newNamesToOldNames[currentName] = item
	    secondaryItems[idx] = currentName
	    currentName += 1

	# broadcasting the oldNamesToNewNames Dictionary which will be used by the transaction to get the revised transaction
	oldNamesToNewNames_broadcast = sc.broadcast(oldNamesToNewNames)
	newNamesToOldNames_broadcast = sc.broadcast(newNamesToOldNames)
	nbh_broadcast = sc.broadcast(dict(nbh))
	minUtil_broadcast = sc.broadcast(minUtil)

	# convert the items from old names to new names in nbh dictionary
	nbhNew = {}
	oldsecondaryItems = list(oldNamesToNewNames.keys())
	for key in nbh.keys():
	    if key in oldsecondaryItems:
	        newNeighbors  = [oldNamesToNewNames[x] for x in nbh[key] if x in oldsecondaryItems]
	        if len(newNeighbors) != 0:
	            nbhNew[oldNamesToNewNames[key]] = set(newNeighbors)
	nbhNew_broadcast = sc.broadcast(nbhNew)

	# Remove non secondary items from each transaction and sort remaining items in increasing order of their PMU values
	revisedTransactions = transactions.map(reviseTransactions).filter(lambda x: len(x.getItems()) > 0)
	revisedTransactions.persist()
	transactions.unpersist()

	# Calculate the neighborhood subtree utility of each item in secondary item
	NSTU_dict = dict(revisedTransactions.flatMap(calculateNSTUFirstTime).reduceByKey(add).filter(lambda x: x[1] >= minUtil).collect())

	# primary items or the items which need to be projected in DFS traversal of the search space
	primaryItems = list(NSTU_dict.keys())
	primaryItems.sort(key= lambda x: pmuDict[newNamesToOldNames[x]])


	itemToNodeMap, nodeToItemsMap = divideItems(primaryItems, numPartitions, partitionType)
	itemToNodeMap_broadcast = sc.broadcast(itemToNodeMap)
	nodeToItemsMap_broadcast = sc.broadcast(dict(nodeToItemsMap))

	# creating a new key-value RDD where key is node id and value is list of transactions at that node id
	partitionTransactions = revisedTransactions.flatMap(mapTransaction).groupByKey().mapValues(list)
	partitionTransactions.persist()
	revisedTransactions.unpersist()

	# repartition the data into nodes depending upon the key
	# transactions = transactions.partitionBy(numPartitions, lambda k: int(k[0]))
	# partitioner = RangePartitioner(numPartitions)

	huis = partitionTransactions.map(parllelEFIM).groupByKey().map(lambda x : (x[0], list(x[1]))).collect()

	itemsets = [y for x in huis[0][1] if len(x) > 0 for y in x]
	
	endTime = time.time()

	print("total time taken ", endTime - startTime)
	print("number of HUIs generated ", len(itemsets))