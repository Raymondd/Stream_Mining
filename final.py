from memory_profiler import memory_usage
from time import sleep
import time
from optparse import OptionParser
import itertools
import operator
import resource


def Update(t, L, i):
	
	for subset in itertools.combinations(t, i):
		if subset in L[i-1]:
			L[i-1][subset] += 1
		elif i <= 2:
			L[i-1][subset] = 1
		else:
			for subset2 in itertools.combinations(t, i-1):
				if subset2 not in L[i-2]: 
					return L
			L[i-1][subset] = 1			
	return L
	
def reduceFreq(L, i):
	for s, n in L[i-1].items():
		L[i-1][s] -= 1
		if L[i-1][s] == 0:
			del L[i-1][s]
	return L


def stream_mining_fixed(D, theta):
	L = [{},{},{}]
	T = []
	f = 0
	c = 0
	error = .1
	
	for t in D:
		T.append(t)
		L = Update(t, L, 1)
		L = Update(t, L, 2)
		f = 100
		threshold = 1.0/theta*error*f
		if len(L[1]) >= threshold:
			L = reduceFreq(L, 2)
			c += .4
		
			for i in range(1, len(L)+1):
				for trans in T:
					Update(trans, L, i)
				reduceFreq(L, i)
			T = []

	while c < (len(D)*theta):
		c += 1
		for i in range(1, len(L)+1):
			L = reduceFreq(L, i)
	L = reduceFreq(L, 1)
	L = reduceFreq(L, 2)
	return L

#FILE IO
def dataFromFile(fname):
        """Function which reads from the file and yields a generator"""
        file_iter = open(fname, 'rU')
        for line in file_iter:
                line = line.strip().rstrip(',')                         # Remove trailing comma
                record = frozenset(line.split(','))
                yield record

def getItemSetTransactionList(data_iterator):
    transactionList = list()
    itemSet = set()
    for record in data_iterator:
        transaction = frozenset(record)
        transactionList.append(transaction)
        for item in transaction:
            itemSet.add(frozenset([item]))              # Generate 1-itemSets
    return itemSet, transactionList




if __name__ == "__main__":
	'''optparser = OptionParser()
	optparser.add_option('-f', '--inputFile', dest='input', help='filename containing csv', default=None)
	(options, args) = optparser.parse_args()
	
	if options.input is not None:
		inFile = dataFromFile(fileName[0])
		itemSet, transactionList = getItemSetTransactionList(inFile)
	else: 
		print 'No dataset filename specified, system with exit\n'
		sys.exit('System will exit')'''
	'''fileNames = ["dataset_718kb.csv", "dataset_1mb.csv","dataset_2mb.csv", "dataset_2mb.csv", "dataset_5mb.csv", "dataset_10mb.csv", "dataset_21mb.csv", "dataset_50mb.csv", "dataset_500mb.csv", "dataset_1gb.csv"]'''
		
	fileNames = ["INTEGRATED-DATASET.csv"]
	
	for name in fileNames:
		inFile = dataFromFile(name)
		itemSet, transactionList = getItemSetTransactionList(inFile)
		inputData = transactionList
	
		theta = .1
	
		#start = time.time()
		Items = stream_mining_fixed(inputData, theta)
		print Items
		#print name + " TIME: " + str(time.time()-start)
	
	
	
	
	#print("RESULTS: " + str(sorted(Items[0].items(), key=operator.itemgetter(1), reverse=True)))
	#print("RESULTS of 2-itemset: " + str(sorted(Items[1].items(), key=operator.itemgetter(1), reverse=True)))
	#print("RESULTS of 3-itemset: " + str(sorted(Items[2].items(), key=operator.itemgetter(1), reverse=True)))

	#print Items
	
	#print resource.getrusage(resource.RUSAGE_SELF)
	#from guppy import hpy
	#h = hpy()
	#print h.heap()
	#print h.iso()

	#mem_usage = memory_usage(stream_me)
	#print('Memory usage (in chunks of .1 seconds): %s' % mem_usage)