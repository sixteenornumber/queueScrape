#!/usr/bin/env python

'''
  Used for stream file testing
  downloads utls and writes the html to files.  these file will be 
  picked up by a spark fileStream process and be parsed accordingly.
  the fileStream requires that files be new to parse correctly.
  
  By adding additional threads, the html throughput can be increased.


'''


import shutil
import sys
import urllib2
from multiprocessing import Queue, Process
import time
from random import randint

class meeseeks:

  def __init__(self):
    self.urlList = [line.strip() for line in open("urlList.txt", 'r')]
    self.fileNumber = 0
    self.htmlQueue = Queue()
    print self.urlList
		
  def urlWorker(self):
    while True:
      url = self.urlList[randint(0,len(self.urlList)-1)]
      print 'downloading', url
      try:
        response = urllib2.urlopen(url, timeout=2)
        html = response.read()
        self.htmlQueue.put(html)
        print '  --done!'
      except:
        print '  --failed'  
	
	
  def fileWriterWorker(self):
    while True:
      html = self.htmlQueue.get()
      tempPath = 'someFilesTemp/'
      donePath = 'someFilesDone/'
      fName = "html_" + str(self.fileNumber) + '.txt'
      f = open(tempPath + fName,"w")
      self.fileNumber += 1
      f.write(html)
      f.close()
      shutil.move(tempPath + fName, donePath + fName)
	
		
		
if __name__ == "__main__":

  ms = meeseeks()
  
  ## fileWritter procs
  numfileWriterProcs = 1
  for i in range(numfileWriterProcs):
    fWriteProcs = Process(target = ms.fileWriterWorker, args=())
    fWriteProcs.start()
  
  ## url download procs
  numUrlProcs = 1
  for i in range(numUrlProcs):
    urlProcs = Process(target = ms.urlWorker, args=())
    urlProcs.start()


	
	
	