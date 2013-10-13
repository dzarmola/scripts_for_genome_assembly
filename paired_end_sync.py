import re,time,sys, getopt
from multiprocessing import Process, Pipe


def usage():
        print "-a: plik zrodlowy 1"
        print "-b: plik zrodlowy 2"
        print "-o: nazwa pliku wyjsciowego"
        exit(0)

def wybor(plik1,conn): 
#wybiera z pliku 1 numery sekwencji "dobrych" - spelniajacych kryteria
	fastq=open(plik1)
	dobre=set([])
	l=0
	for line in fastq:
		if l==0: 
			loc=":".join(line.split("\t")[0].split(":")[3:])#":".join(line.split(":")[4:6])
			dobre.add(loc)
		if l==3: l=-1
		l+=1
	conn.send(dobre)
	conn.close()
	fastq.close()


def zapis(dobre,plik1,name): 
	if type(plik1)==str:
		fastq=open(plik1)
	else:
		fastq=plik1
	fastq=plik1#open(plik1)
	nowy=open(name, "a", 0)
	nra=0
	for line in fastq:
		#szuka sekwencji o numerze ze zbioru spelniajacych kryteria
		if nra==0:
			loc=":".join(line.split("\t")[0].split(":")[3:])
			if loc in dobre:
				A1=line
			else:
				nra=-4
		if nra==1:
			seq=line
		if nra==2:
			A3=line
		if nra==3:
			qua=line
			for line in [A1, seq, A3, qua]:#uzupelnia znaki nowej linii
							if not re.search("\n", line): 
								nowy.write("%s\n" % line)
							else: 
								nowy.write("%s" % line)
			nra=-1
		nra+=1
	nowy.close()



test=0 
#parametr testowy, do uruchamiania bez parametrow linii polecen
log=open("log.txt", "a", 0)

if __name__ == '__main__': #jezeli jest to proces glowny

	log.write("\n\n\n\nStart ")
	log.write(time.asctime())
	nazwa="fastq"

	if test==0: #wczytanie wartosci parametrow
		opts, args=getopt.getopt(sys.argv[1:], "a:b:o:")
		if (not "-a" in sys.argv) or (not "-b" in sys.argv): 
			usage()
		for o, a in opts:
			if o =="-a":
				plik1=a
			elif o == "-b":
				plik2=a
			elif o == "-o":
				nazwa=a
			else:
				usage()
	else:
		plik1="fasta.txt"
		plik2="fasta.txt"

	log.write("\nstart odsiewania sekwencji")
	log.write(time.asctime())
	#rozdzielenie zadan
	parent_conn1, child_conn1 = Pipe()
	parent_conn2, child_conn2 = Pipe()
	proces1 = Process(target=wybor, args=(plik1,child_conn1,))
	proces2 = Process(target=wybor, args=(plik2,child_conn2,))
	proces2.start()
	proces1.start()

	d1=parent_conn1.recv()
	d2=parent_conn2.recv()
	
	while proces1.is_alive() or proces2.is_alive(): 
#oczekiwanie na zakonczenie procesow potomnych
		pass

	log.write("\nkoniec odsiewania sekwencji")
	log.write(time.asctime())

	#uzgodnienie listy numerow sekwencji spelniajacej kryteria w obu plikach
	single1=d1-d2
	single2=d2-d1
	wybrane=d1&d2
	#podzial sekwencji na sparowane i pozbawione pary

	log.write("\nPoczatek przerabiania sekwencji ")
	log.write(time.asctime())
	f1=open(plik1)
	f2=open(plik2)
	###zapis sekwencji bez pary
	if single1:
		n=nazwa+"_single_1.fastq"
		proces_single1=Process(target=zapis, args=(single1,f1,n,))
		proces_single1.start()
	if single2:
		n=nazwa+"_single_2.fastq"
		proces_single2=Process(target=zapis, args=(single2,f2,n,))
		proces_single2.start()
	#rownolegly zapis wybranych sekwencji do obu plikow
	proces3=Process(target=zapis, args=(wybrane,plik1,nazwa+"_1.fastq",))
	proces4=Process(target=zapis, args=(wybrane,plik2,nazwa+"_2.fastq",))
	proces3.start()
	proces4.start()

	if single1: proces_single1.join()
	if single2: proces_single2.join()
	proces3.join()
	proces4.join()
	log.write("\nkoniec pracy ")
	log.write(time.asctime())
	log.close()

