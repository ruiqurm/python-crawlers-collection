import multiprocessing
import requests
import pickle
import csv
import time
import pickle
import os
q = multiprocessing.Queue()
fq = multiprocessing.Queue()
url = "https://opendata.sz.gov.cn/api/29200_00403602/1/service.xhtml?page={}&rows=1000&appKey=在此填充appkey"
n_process = 12
n= [1,2133]

def process(url,q,fq):
	try:
		r = requests.get(url,timeout=15)
	except:
		print("failed {}".format(url))
		fq.put(url)
		q.put([])
	else:
		q.put(r.json()["data"])
		# q.put([])
		print("done {}".format(url))
	exit(0)

if __name__ == "__main__":
	"""
	debug
	"""
	time_start=time.time()
	time_iter_start = 0
	last_len_data = 0
	if (os.path.exists("./tmp.pkl")):
		with open("./tmp.pkl","rb") as file:
			i,fqq,data  = pickle.load(file)
			for i in fqq:
				fq.put(i)
			n[0] = i
	data = []
	print("run from {} to {}".format(n[0],n[1]))
	if(n[0]>n[1]):exit(0)
	print("run on {} process".format(n_process))
	
	try:
		for i in range(n[0],n[1]+1,n_process):
			jobs = []
			time_iter_start = time.time()
			n_process = (n[1]+1 - i) if (i+n_process > n[1]+1) else n_process
			for j in range(0,n_process):
				p = multiprocessing.Process(target=process, args=(url.format(i+j), q,fq))
				jobs.append(p)
				p.start()
			while True:
				running = any(job.is_alive() for job in jobs)
				while (not q.empty()):
					data.extend(q.get())
				if not running:
					break
			time_iter_end = time.time()
			_ = (len(data) - last_len_data)/(time_iter_end - time_iter_start)
			last_len_data = len(data)
			print("now rate={} per second".format(_))
			_ /= 1000
			print("eta={} min".format( (n[1]+1 - i )/_/60) )
	except:
		print("end with i = {}".format(i))
	else:
		print("finished")	
	print("get data={}".format(len(data)))		
	fqq = []
	while (not fq.empty()):
		fqq = fq.get()
	_ = i,fqq,data
	pickle.dump(_,open("./tmp.pkl","wb"))
	file = open("output.csv","w",encoding='gbk',newline='')
	csv_writer = csv.writer(file)
	csv_writer.writerow(['elevation',
		'recorder_speed',
		'system_time',
		'plate_color',
		'gps_time',
		'to_police_num',
		'plate_num',
		'gps_longitude',
		'erro_type',
		'gps_speed',
		'operator',
		'map_latitude',
		'map_longitude',
		'gps_latitude',
		'event',
		'direction',
		'mileage'])
	csv_writer.writerows([[j[1] for j in i.items()] for i in data])
