#encoding: utf-8

import sys
reload(sys)
sys.setdefaultencoding( "utf-8" )

import urllib2
import cookielib
import chardet
import HTMLParser
import time,threading
import StringIO,gzip
import os

baseurl='http://www.gushiwen.org/'

prefixpath='webpages/'

maxwaiturl=5

isdoclim=0.9
maxfilenamelen=255
filenameforbiddens=['\\','/',':','.','?','&','%','=']
nodec=['doc','xls','pdf','mp4','mp3','zip','rar','tar','gz','7z','flv','mkv','ppt','exe']
maxthread=8
twaitspd=1
idfcookie=False

pgt=0
#pgse=1000

maxtol=64
ctwaitspd=float(maxwaiturl)/maxtol
uurlpc=0
poolc=True

urlal=set([])
urlqu=set([])

exitsgn=False

lckual=threading.Lock()
lckuqu=threading.Lock()
lckpro=threading.Lock()
lckpgt=threading.Lock()

class hrHTMLParser(HTMLParser.HTMLParser):

	def __init__(self):
		HTMLParser.HTMLParser.__init__(self)

	def handle_starttag(self,tag,attrs):
		if tag=="a":
			for(variable,value) in attrs:
				if variable=="href":
					if value!=None:
						if value.find('#')==-1:
							if value.find('://')!=-1:
								addurls(value)
							elif value.startswith("/"):
								addurls("http://so.gushiwen.org"+value)

def multpri(strtp=''):
	global lckpro
	lckpro.acquire()
	try:
		print time.strftime('%Y-%m-%d %H:%M:%S : ',time.localtime(time.time()))+strtp+'\n>> ',
	finally:
		lckpro.release()

def getHref(hdoc):
	hp=hrHTMLParser()
	hp.feed(hdoc)
	hp.close()

def urlfilter(url):
	rt=False
	if url.endswith(".aspx"):
		if url.rfind("fanyi_")>0 or url.rfind("bookv_")>0 or url.rfind("view_")>0:
			if url.rfind("jiucuo")<0:
				rt=True
	return rt

def addurls(url):
	global urlqu,lckuqu
	if url.find("gushiwen.org")>0 and url.find("randShow")==-1:
		if urlfilter(url):
			lckuqu.acquire()
			try:
				if not((url in urlal) or (url in urlqu)):
					urlqu.add(url)
					multpri("add:"+url)
			finally:
				lckuqu.release()

def mvurls(url):
	global urlqu,urlal,lckual,lckuqu
	lckual.acquire()
	try:
		urlal.add(url)
		if url in urlqu:
			lckuqu.acquire()
			try:
				if url in urlqu:
					urlqu.remove(url)
			finally:
				lckuqu.release()
	finally:
		lckual.release()

def gethost(url):
	i=url.find('://')
	tmp=0
	r=''
	if i==-1:
		while tmp<len(url) and url[tmp]!='/':
			r=r+url[tmp]
			tmp=tmp+1
	else:
		while (tmp+3+i)<len(url) and url[tmp+i+3]!='/':
			r=r+url[tmp+i+3]
			tmp=tmp+1
	return r

def getheader(url):
	host=gethost(url)
	header={'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
	'Accept-Encoding':'gzip, deflate',
	'Accept-Language':'zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3',
	'Cache-Control':'max-age=0',
	'Connection':'keep-alive',
	'DNT':'1',
	'Host':host,
	'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:51.0) Gecko/20100101 Firefox/51.0'}
#	'User-Agent':'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2490.86 Safari/537.36'}
	return header

def ugzip(strdt):
	buf=StringIO.StringIO(strdt)
	f=gzip.GzipFile(fileobj=buf)
	return f.read()

def oneGetPageData(url):
	global maxwaiturl
	req=urllib2.Request(url,headers=getheader(url))
	try:
		con=urllib2.urlopen(req,timeout=maxwaiturl)
		doc=con.read()
		if con.info().get('Content-Encoding')=='gzip':
			doc=ugzip(doc)
		rurl=con.geturl()
		con.close()
	except:
		rurl='null'
		doc='null'
	return rurl,doc

def getPageData(url, nretry=5):
	rurl='null'
	doc='null'
	for i in xrange(nretry):
		rurl, doc = oneGetPageData(url)
		if doc != 'null':
			break
	return rurl,doc

def getEncoder(docData):
	encodeInfo=chardet.detect(docData)
	return encodeInfo['encoding']

def isDoc(docData):
	global isdoclim
	encodeInfo=chardet.detect(docData)
	if encodeInfo['confidence']>isdoclim:
		return True
	else:
		return False

def decodePage(docData):
	return docData.decode(getEncoder(docData),'ignore')

def zipurl(url):
	url=url.replace('https','s')
	url=url.replace('http','h')
	url=url.replace('ftp','f')
	url=url.replace('www','w')
	return url

def genFileName(url):
	global filenameforbiddens,maxfilenamelen
	for fbc in filenameforbiddens:
		while url.find(fbc)!=-1:
			url=url.replace(fbc,'')
			url=zipurl(url)
	if len(url)>maxfilenamelen:
		url=url[0:maxfilenamelen//2]+url[(len(url)-maxfilenamelen//2):(len(url)-1)]
	return url

def wPFile(pdata,filewrite,wmethod):
	global pgt,lckpgt
#	global pgse,exitsgn
	global prefixpath
	if not exitsgn:
		lckpgt.acquire()
		try:
			pgt+=1
		finally:
			lckpgt.release()
		try:
			f=open(prefixpath+filewrite,wmethod)
			f.write(pdata)
			f.close()
		except IOError:
			pass
#	if pgt>=pgse:
#		multpri(str(pgt)+' Page(s) get , Broadcast exit signal')
#		exitsgn=True

def sLog():
	logf=open('log.txt','a')
	return logf

def log(logf,msg):
	logf.write(msg)

def eLog(logf):
	logf.close()

def dealSite(url):
	multpri("get:"+url)
	global nodec,isdoclim
	(rurl,hdoc)=getPageData(url)
	cf=False
	fname=genFileName(rurl)
	wm='wb'
	for ff in nodec:
		if fname.find(ff)!=-1:
			cf=True
			break
	if not cf:
		encodeInfo=chardet.detect(hdoc)
		if encodeInfo['confidence']>isdoclim:
			hdoc=hdoc.decode(encodeInfo['encoding'],'ignore')
			getHref(hdoc)
			wm='w'
	mvurls(url)
	if url.find("fanyi_")>0 and url.endswith(".aspx") and fname!="null":
		if wm=='w':
			wPFile(hdoc.encode('utf-8','ignore'),fname,wm)
		else:
			wPFile(hdoc,fname,wm)

def spdm():
	global urlqu,lckuqu,exitsgn
	global maxtol,uurlpc,poolc
	global ctwaitspd
	while not exitsgn:
		if len(urlqu)!=0:
			sgdtmp=False
			lckuqu.acquire()
			try:
				if urlqu:
					tmp=urlqu.pop()
					sgdtmp=True
				else:
					uurlpc+=1
					time.sleep(ctwaitspd)
					if poolc:
						multpri('URL pool is empty')
						poolc=False
					if uurlpc>=maxtol:
						multpri('MAXTOL trigger , Broadcast exit signal')
						exitsgn=True
			finally:
				lckuqu.release()
			if sgdtmp:
				dealSite(tmp)
		else:
			uurlpc+=1
			time.sleep(ctwaitspd)
			if poolc:
				multpri('URL pool is empty')
			poolc=False
			if uurlpc>=maxtol:
				multpri('MAXTOL trigger , Broadcast exit signal')
			exitsgn=True
	multpri('exit signal received , quit')

def ssdcookie():
	cj=cookielib.CookieJar()
	opener = urllib2.build_opener(urllib2.HTTPCookieProcessor(cj));
	urllib2.install_opener(opener);

def init():
	global urlqu,baseurl,exitsgn,prefixpath
	exitsgn=True
	if os.path.exists(prefixpath):
		if os.path.isdir(prefixpath):
			multpri(prefixpath+' is not empty , may overwrite some data')
			exitsgn=False
	else:
		os.makedirs(prefixpath)
		exitsgn=False
	if not exitsgn:
		urlqu.add(baseurl)
		if not idfcookie:
			ssdcookie()
		t=threading.Thread(target=kepthrd)
		t.setDaemon(True)
		t.start()

def lodthrd():
	t=threading.Thread(target=spdm)
	t.setDaemon(True)
	t.start()

def kepthrd():
	global exitsgn,twaitspd,maxthread,maxwaiturl
	global urlqu
	lodthrd()
	multpri('A spider loaded:'+str(len(urlqu)))
	time.sleep(maxwaiturl)
	while not exitsgn:
		while (threading.activeCount()<maxthread+2) and (not exitsgn) and len(urlqu)>0:
			lodthrd()
			multpri('A spider loaded:'+str(len(urlqu)))
			time.sleep(twaitspd)
	multpri('exit signal received , wait spiders to quit')
	time.sleep(twaitspd*(maxthread+3))
	multpri('Force spider(s) to quit:'+str(len(urlqu)))

def main():
	global exitsgn,twaitspd,maxthread
	global pgt
	print '>> ',
	multpri('Start Spider')
	init()
	while not exitsgn:
		pass
	multpri('exit signal received , wait spider(s) keeper to quit')
	time.sleep(twaitspd*(maxthread+5))
	multpri('Force spider(s) keeper to quit')
	multpri(str(pgt)+' Page(s) writed to disk')

if __name__=="__main__":
	baseurl=sys.argv[1].decode("utf-8")
	prefixpath=sys.argv[2].decode("utf-8")
	main()
