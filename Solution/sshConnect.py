#!/usr/bin/python

import sys
import subprocess
import paramiko
import threading
import time
import Queue
import getopt

#Global Variables and default options
queue = Queue.Queue()
serverFile = open('fleetserver_logs', 'w+')
logFile = open('process_log', 'w+')
hours = "2"

f= open('HOSTS')
hosts= f.readlines()
f.close()

class ConnectionThread(threading.Thread):
    def __init__(self, queue, hours):
        threading.Thread.__init__(self)
        self.queue = queue
        self.hours = "1"
    
    def run(self):
        while True:
            host = self.queue.get()
            connectSSH_Paramiko(host,hours)
            self.queue.task_done() 

def processTop10Connections():
 
    f = open('fleetserver_logs')
    allIP= f.readlines()
    f.close()
    
    output = []
    size = len(allIP)
    print "Number of all relevant IP Addresses from all servers: ", size

    #number of all valid hits (Status code 2**)
    p1 = subprocess.Popen(["cat fleetserver_logs | awk '($9 ~ /3**/)' | awk '{ print $1 }' | sort | uniq | sort -rn | head -n 10"],shell=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    p2 = subprocess.Popen(["cat fleetserver_logs | awk '($9 ~ /3**/)' | wc -l "],shell=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)

    outputP1 = p1.stdout.readlines()
    outputP2 = p2.stdout.readlines()

    for x in outputP1:
        for y in outputP2:
            x+=(" HITRATE: "+y+"/"+str(size))
        x = x.translate(None, '\n')
        output.append(x)

    print output
    return output

def connectSSH_Paramiko(host,hours):
    amazonKey = paramiko.RSAKey.from_private_key_file("/home/rorix/Documents/AMAZON/amazonEC2_Key.pem")
    c = paramiko.SSHClient()
    c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    logFile.write("Attempting to connected to:"+host+"\n")
    c.connect( hostname = host, username = "ec2-user", pkey = amazonKey )
    logFile.write("Successfully connected to:"+host+"\n")

    commands = [ "awk -vDate=`date -d'now-"+hours+" hours' +[%d/%b/%Y:%H:%M:%S` ' { if ($4 > Date) print}' /var/log/httpd/access_log"]
    for command in commands:
        msgBegin = "Executing {}".format( command )+"\n"
        logFile.write(msgBegin)
        stdin , stdout, stderr = c.exec_command(command)
        cmdResult = stdout.read()
        if not cmdResult:
            msg = "LOG MESSAGE for SERVER:"+host+" : No IP requests within timeline specified. Try expanding timespan\n"
            logFile.write(msg)
        serverFile.write(cmdResult)
        logFile.write("ERROR LOG:")
        logFile.write(stderr.read())
    c.close()

def threadConnection(numberOfThreads, listOfHosts,hours):
    for i in range(numberOfThreads):
        t = ConnectionThread(queue,hours)
        t.setDaemon(True)
        t.start()
    for host in listOfHosts:
        queue.put(host)
    queue.join()
    serverFile.close()
    logFile.close()

def main():
    start = time.time()
    print "Performing ssh connections: please wait...."
    threadConnection(10,hosts,hours)
    serverFile.close()
    logFile.close()

    print "Elapsed Time: %s" % (time.time() - start)
    print "Connections completed"

    top10IPs = processTop10Connections();
    top10File = open('top10IPAddresses', 'w+')
    for ip in top10IPs:
        top10File.write(ip)
    top10File.close()
    print "Top 10 common addresses file created"

if __name__ == '__main__':
    main()
