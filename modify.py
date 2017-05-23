import json

count = 0
errorlist = []
with open('all.csv') as infile:
	with open('test.csv','w') as outfile:
		for line in infile.readlines():
			items = line.split(',')
			if items[0]=='time' or not items[3] or not float(items[9]) or not float(items[10]) or not float(items[18]) or not items[5]:
				continue
			outfile.write(','.join(items))
			count += 1
			if count % 40 == 0:
				items[5] = "100000"
				items[18] = str(float(items[18])+50)
				outfile.write(','.join(items))
				errorlist.append(count)
				count += 1

with open("error.json",'w') as errfile:
	json.dump(errorlist, errfile)