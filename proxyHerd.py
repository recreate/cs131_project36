
from twisted.internet import reactor, protocol
from twisted.protocols import basic
from twisted.web import client
import sys
import time

sys.path.append('./python-twitter')
sys.path.append('./python-oauth2-master')
sys.path.append('./httplib2-0.7.7/python2')
import oauth2 
import httplib2
import twitter

global serverName
global storage
global servers
global ports

class ConnectionProtocol(basic.LineReceiver):

	def __init__(self, info=None):
		self.info = info

	def propagate(self):

		ps = []
		k = 0
		for i in self.info:
			if k == 0:
				data = i
				k += 1
				continue
			if k == 1:
				k += 1
				continue
			ps.append(i)
			k += 1
		
		message = "SERVER " + " ".join(ps) + " SEND " + " ".join(data)
		self.sendLine(message)

	def connectionMade(self):
		print "Connected from %s." % self.transport.getPeer().host
		
		if self.info != None:
			self.propagate()

	def lineReceived(self, line):
		global storage
		serverTime = time.time()

		file = open("./server" + serverName, "a")

		input = line.split(None)
		if len(input) > 0 and input[0] == "SERVER":
			end = input.index("SEND")
			data = tuple(input[end+1:])
			ps = input[1:end]

			storage.append(data)
			
			file.write(line + "\n")
			file.close()

			if not ps:
				return
			
			send = [data, ps[0]]
			if len(ps) > 1:
				for i in ps[1:]:
					send.append(i)

			port = ports[servers.index(ps[0])]

			reactor.connectTCP("localhost", port, clientComponent(send))

			return

		if len(input) != 4:
			self.sendLine("? " + line)
			file.write("? " + line)
			file.close()
			return

		command = input[0]
		arg1 = input[1]
		arg2 = input[2]
		arg3 = input[3]

		if command == "IAMAT":
			clientID = arg1
			location = str(arg2)
			clientTime = arg3
			
			try:
				temp = float(clientTime)
				assert(temp >= 0)
			except Exception:
				error_message = "Error: Invalid timestamp: " + clientTime
				file.write(line + "\n")
				file.write(error_message + "\n")
				self.sendLine(error_message)
				file.close()
				return

			timeDelay = serverTime - float(clientTime)
			delay = str(timeDelay)
			if timeDelay > 0:
				delay = "+" + str(timeDelay)
			
			output = "AT " + serverName + " " + delay + " "
			output += clientID + " " + location + " " + str(clientTime)

			plus = str.rfind(location, "+")
			minus = str.rfind(location, "-")
			sep = max(plus, minus)
			if sep == -1:
				error_message = "Error: Invalid latitude and longitude: " + location
				file.write(line + "\n")
				file.write(error_message + "\n")
				self.sendLine(error_message)
				file.close()
				return

			lat = location[:sep]
			longit = location[sep:]
			
			try:
				float(lat)
				float(longit)
			except Exception:
				error_message = "Error: Invalid latitude and longitude: " + location
				file.write(line + "\n")
				file.write(error_message + "\n")
				self.sendLine(error_message)
				file.close()
				return

			data = (serverName, delay, clientID, lat, longit, clientTime)
			storage.append(data)

			file.write(line + "\n")
			file.write(output + "\n")

			self.sendLine(output)
			
			if serverName == "Blake":
				send = [data, "Bryant", "Metta"]
				reactor.connectTCP("localhost", 12611, clientComponent(send))
				send = [data, "Howard", "Gasol"]
				reactor.connectTCP("localhost", 12613, clientComponent(send))

			elif serverName == "Bryant":
				send = [data, "Blake", "Howard"]
				reactor.connectTCP("localhost", 12610, clientComponent(send))
				send = [data, "Gasol"]
				reactor.connectTCP("localhost", 12612, clientComponent(send))
				send = [data, "Metta"]
				reactor.connectTCP("localhost", 12614, clientComponent(send))

			elif serverName == "Gasol":
				send = [data, "Bryant", "Metta"]
				reactor.connectTCP("localhost", 12611, clientComponent(send))
				send = [data, "Howard", "Blake"]
				reactor.connectTCP("localhost", 12613, clientComponent(send))

			elif serverName == "Howard":
				send = [data, "Blake"]
				reactor.connectTCP("localhost", 12610, clientComponent(send))
				send = [data, "Gasol", "Bryant", "Metta"]
				reactor.connectTCP("localhost", 12612, clientComponent(send))

			elif serverName == "Metta":
				send = [data, "Bryant", "Blake", "Howard", "Gasol"]
				reactor.connectTCP("localhost", 12611, clientComponent(send))
			
		elif command == "WHATSAT":
			clientID = arg1
			radius = arg2
			limit = arg3

			try:
				temp = float(radius)
				assert(temp >= 0)
			except Exception:
				error_message = "Error: Invalid radius: " + radius
				file.write(line + "\n")
				file.write(error_message + "\n")
				self.sendLine(error_message)
				file.close()
				return

			try:
				temp = int(limit)
				assert(temp >= 0)
			except Exception:
				error_message = "Error: Invalid tweet limit: " + limit
				file.write(line + "\n")
				file.write(error_message + "\n")
				self.sendLine(error_message)
				file.close()
				return

			t = time.time()
			timeDelay = t - serverTime

			data = None
			for k in storage:
				if k[2] == clientID:
					data = k

			if data is None:
				error_message = "Error: Could not find clientID: " + clientID 
				file.write(line + "\n")
				file.write(error_message + "\n")
				self.sendLine(error_message)
				file.close()
				return

			geocode = (float(data[3]), float(data[4]), radius + "km")

			tweets = ""
			api = twitter.Api()
			search = api.GetSearch(None, geocode)
			num = 0
			lim = min(int(limit), 100)
			for result in search:
				if num == lim:
					break
				tweets += result.AsJsonString()
				num += 1

			output = "AT " + data[0] + " " + data[1] + " " + data[2] + " "
			output += data[3] + data[4] + " " + data[5] + "\n"
			output += tweets 

			file.write(line + "\n")
			file.write(output + "\n")
			self.sendLine(output)

		else:
			self.sendLine("? " + line)
			file.write("? " + line)

		file.close()
	
class serverComponent(protocol.ServerFactory):
	protocol = ConnectionProtocol

class clientComponent(protocol.ClientFactory):
	def __init__(self, info):
		self.info = info

	def buildProtocol(self, addr):
		p = ConnectionProtocol(self.info)
		p.factory = self
		return p

if __name__ == "__main__":
	if len(sys.argv) != 2:
		print "Error: Incorrect commandline arguments.\n"
		sys.exit()

	global serverName
	global servers
	global ports
	global storage

	serverName = str(sys.argv[1])
	servers = ["Blake", "Bryant", "Gasol", "Howard", "Metta"]
	ports = [12610, 12611, 12612, 12613, 12614]
	storage = []

	if serverName not in servers:
		print "Error: Invalid server name.\n"
		sys.exit()

	port = ports[servers.index(serverName)]

	reactor.listenTCP(port, serverComponent())
	print "Running server: " + serverName
	reactor.run()


