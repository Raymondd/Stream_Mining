file = open("dataset_10mb.csv", 'rU')
data = file.read()
outFile = open("dataset_90mb.csv", 'w')
outFile.write(data + data + data + data + data + data + data + data + data)