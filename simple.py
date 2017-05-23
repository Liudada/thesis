import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D
import numpy as np
from pandas import DataFrame, Series
import sys, progressbar, time, json, heapq, multiprocessing

z = []
timestamp = ""
count = 0
with open('test.csv') as infile:
	for line in infile.readlines():
		items = line.split(',')
		if items[0]=='time' or not items[3] or not float(items[9]) or not float(items[10]) or not float(items[18]) or not items[5]:
			continue
		count += 1
		timestamp = time.mktime(time.strptime(items[0],"%Y-%m-%dT%H:%M:%SZ"))
		z.append([items[9],items[10],items[3],items[18],items[5],timestamp])
z = np.array(z, dtype=np.float)
z = DataFrame(z, columns=['x','y','co2','pm2d5','devid','timestamp'])

def getp(i):
	return np.array([z.x[i],z.y[i]])

def dist(p1,p2):
	return np.sqrt((p1[0]-p2[0])**2+(p1[1]-p2[1])**2)

def kmeans(kroot):
	k = kroot**2
	centers = []
	groups = []
	for i in range(k):
		groups.append([])
	xs = np.arange(np.min(z.x),np.max(z.x),(np.max(z.x)-np.min(z.x))/kroot)
	ys = np.arange(np.min(z.y),np.max(z.y),(np.max(z.y)-np.min(z.y))/kroot)
	for i in range(len(xs)):
		for j in range(len(ys)):
			centers.append(np.array([xs[i],ys[j]]))
	mindist = 100
	g = -1
	bar = progressbar.ProgressBar()
	for i in bar(range(len(z.x))):
		p = np.array([z.x[i],z.y[i]])
		for j in range(k):
			d = dist(p, centers[j])
			if d < mindist:
				mindist = d
				g = j
		if g >= 0:
			groups[g].append(i)
			centers[g] = (centers[g]*len(groups[g])+p)/(len(groups[g])+1)
	return groups

def kround(k):
	groups = {}
	bar = progressbar.ProgressBar()
	for i in bar(range(len(z.x))):
		p = (round(z.x[i],k), round(z.y[i],k))
		if p not in groups:
			groups[p] = [i]
		else:
			groups[p].append(i)
	return [groups[k] for k in groups]

def knear(k):
	groups = {}
	bar = progressbar.ProgressBar()
	for i in bar(range(len(z.x))):
		p = (z.x[i], z.y[i])
		ingroup = False
		for c in groups:
			if dist(c, p) <= k:
				groups[c].append(i)
				ingroup = True
				break
		if not ingroup:
			groups[p] = [i]
	return [groups[k] for k in groups]

def gh_filter(data, index, resmap):
	x = data[0]
	dx = 0
	dt = 1.
	g = 0.3
	h = 1/3
	for i in range(len(data)):
		x_est = x + dx*dt
		residual = data[i] - x_est
		if i > 0:
			res = abs(residual)-dist(np.array([z.x[index[i-1]],z.y[index[i-1]]]), np.array([z.x[index[i]],z.y[index[i]]]))*1000-abs(z.timestamp[index[i]]-z.timestamp[index[i-1]])/60
			if res < 0:
				resmap[index[i]] = 0
			else:
				resmap[index[i]] = res
		else:
			resmap[index[i]] = abs(residual)
		dx = dx + h*residual/dt
		x = x_est + g*residual
		#results.append(x)
	return resmap

if len(sys.argv) > 1:
	groups = knear(float(sys.argv[1]))
	for g in groups:
		print(len(g))
	with open('groups.json','w') as groupfile:
		json.dump(groups, groupfile)
	resmap = {}
	for i in groups:
		if i:
			resmap = gh_filter([z.pm2d5[item] for item in i], i, resmap)
	with open('resmap.json','w') as resfile:
		json.dump(resmap, resfile)

	def Getdevmap(i):
		global lock
		global groups
		for g in groups:
			if i in g:
				pos = g.index(i)
				diff = 0
				dn = 0
				for offset in range(-10,10):
					tpos = pos + offset
					if tpos<0 or tpos>=len(z.x):
						continue
					if z.devid[tpos] == devid:
						continue
					tdelta = abs(z.timestamp[tpos]-z.timestamp[pos])
					if tdelta >= 60:
						continue
					diff += (60-tdelta)*(z.pm2d5[pos]-z.pm2d5[tpos])/60
					dn += 1
				return [i,abs(diff)/dn]

	devmap = {}
	devreslist = []
	for i in range(len(z.devid)):
		if z.devid[i] not in devmap:
			devmap[z.devid[i]] = [i]
		else:
			devmap[z.devid[i]].append(i)
	for devid in devmap:
		bar = progressbar.ProgressBar()
		pool = multiprocessing.Pool(4)
		print("processing ... total:", len(devmap[devid]))
		devreslist.extend(pool.map(Getdevmap, devmap[devid]))

	devresmap = {}
	for i in devreslist:
		devresmap[i[0]] = i[1]

	with open('devres.json','w') as devresfile:
		json.dump(devresmap, devresfile)
else:
	groups = json.load(open("groups.json"))
	resmap = json.load(open("resmap.json"))
	devres = json.load(open('devres.json'))
	errlist = json.load(open('error.json'))

	fig = plt.figure()
	ax = fig.add_subplot(111, projection='3d')

	top = []
	length = len(z.x)
	for i in range(length):
		score = resmap[str(i)]
		if score < 0:
			score = 0
		ele = score+devres[str(i)]
		if len(top) < 50:
			heapq.heappush(top, [ele,i])
		else:
			heapq.heappushpop(top, [ele,i])

	for t in top:
		print(resmap[str(t[1])],devres[str(t[1])])

	plt.ion()
	for item in top:
		for g in groups:
			if item[1] in g:
				for offset in range(-10, 10):
					tpos = g.index(item[1])+offset
					if tpos<0 or tpos>=len(g):
						continue
					tpos = g[tpos]
					if offset == 0:
						ax.plot(xs=[z.x[tpos],z.x[tpos]], ys=[z.y[tpos],z.y[tpos]], zs=[0,z.pm2d5[tpos]], zdir='z', c='r')
					else:
						ax.plot(xs=[z.x[tpos],z.x[tpos]], ys=[z.y[tpos],z.y[tpos]], zs=[0,z.pm2d5[tpos]], zdir='z', c='b')
				plt.pause(1)
				ax.clear()