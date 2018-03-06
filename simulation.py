# !/usr/bin/env python
# -*- coding: utf-8 -*-
"""Assignment 5. Queu."""

import csv
import argparse

simulation_length = 10010  # how many seconds

class Queue:
    def __init__(self):
        self.items = []

    def isEmpty(self):
        return self.items == []

    def enqueue(self, item):
        self.items.insert(0,item)

    def dequeue(self):
        return self.items.pop()

    def size(self):
        return len(self.items)


class Server:
    def __init__(self):
        self.currentRequest = None
        self.timeRemaining = 0

    def tick(self):
        if self.currentRequest != None:
            self.timeRemaining = self.timeRemaining - 1
            if self.timeRemaining <= 0:
                self.currentRequest = None

    def busy(self):
        if self.currentRequest != None:
            return True
        else:
            return False

    def startNext(self, newrequest):
        self.currentRequest = newrequest
        self.timeRemaining = newrequest.getProcessTime()


class Request:
    def __init__(self, time, proctime, resrc):
        self.timestamp = time
        self.processTime = proctime
        self.resource = resrc

    def getStamp(self):
        return self.timestamp

    def getProcessTime(self):
        return self.processTime

    def getResource(self):
        return self.resource

    def waitTime(self, currenttime):
        return currenttime - self.timestamp

def simulateOneServer(numSeconds, requests):
    server = Server()
    requestQueue = Queue()
    waitingtimes = []

    for currentSecond in range(1, numSeconds + 1):

        if currentSecond in requests:
            for req in requests[currentSecond]:
                requestQueue.enqueue(req)

        if (not server.busy()) and (not requestQueue.isEmpty()):
            nextRequest = requestQueue.dequeue()
            waitingtimes.append(nextRequest.waitTime(currentSecond))
            server.startNext(nextRequest)
        server.tick()

    averageWait = sum(waitingtimes) / len(waitingtimes)
    print("Average Wait %6.2f secs %3d requests remaining." % (averageWait, requestQueue.size()))


def simulateManyServers(numSeconds, requests, nSerevers):
    servers = [Server() for i in range(nSerevers)]
    requestQueue = Queue()
    waitingtimes = []
    which_server = 0  # for choosing server in round robin fashion

    for currentSecond in range(1, numSeconds + 1):
        server = servers[which_server]

        if currentSecond in requests:
            for req in requests[currentSecond]:
                requestQueue.enqueue(req)

        if (not server.busy()) and (not requestQueue.isEmpty()):
            nextRequest = requestQueue.dequeue()
            waitingtimes.append(nextRequest.waitTime(currentSecond))
            server.startNext(nextRequest)
            which_server = (which_server + 1) % nSerevers  # next server's turn

        # simulate all servers
        for s in servers:
            s.tick()

    averageWait = sum(waitingtimes) / len(waitingtimes)
    print("Average Wait %6.2f secs %3d requests remaining." % (averageWait, requestQueue.size()))


def main(file, nSerevers):
    requestsDict = {}

    with open(file, 'rb') as f:
        reader = csv.reader(f, delimiter=',')
        for i, row in enumerate(reader):
            start, resrc, proctime = int(row[0]), row[1], int(row[2])
            if not start in requestsDict:
                requestsDict[start] = []
            req = Request(start, proctime, resrc)
            requestsDict[start].append(req)

            # warn user if input won't get a chance to be simulated:
            if not (simulation_length >= start >= 1):
                print 'warning: found input request that starts outside simulation range'

    if nSerevers == 1:
        simulateOneServer(simulation_length, requestsDict)
    elif nSerevers > 1:
        simulateManyServers(simulation_length, requestsDict, nSerevers)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('file', help='path to file')
    parser.add_argument('--servers', help='number of servers')
    args = parser.parse_args()
    main(args.file, int(args.servers or 1))