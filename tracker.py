import datetime
import time
import sys
import json
import requests
import threading
import logging
from queue import Queue
from requests.exceptions import *

# Work around weak DH key issue on some devices and disable TLS warnings
requests.packages.urllib3.disable_warnings()
requests.packages.urllib3.util.ssl_.DEFAULT_CIPHERS += 'HIGH:!DH:!aNULL'

logging.basicConfig(level=logging.INFO)

def get_data(ip, threadId):
  url = "https://%s:3002/index?type=ajaxConnections&time=0" % ip
  try:
    r = requests.get(url, verify=False, timeout=5)
    if r.status_code == 200:
      content = r.content.decode("utf-8").rstrip()
      j = json.loads(content)[0]
      record = {}
      record['ip'] = ip
      record['serial'] = j['Serial']
      record['timestamp'] = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
      if 'GPS_LATITUDE' in j and 'GPS_LONGITUDE' in j:
        record['lat'] = j['GPS_LATITUDE']
        record['long'] = j['GPS_LONGITUDE']
        record['heading'] = j['GPS_HEADER']
        record['speed'] = j['GPS_SPEED']
        record['time'] = j['GPS_TIME']
      else:
        record['lat'] = "UNKNOWN"
        record['long'] = "UNKNOWN"
        record['heading'] = "UNKNOWN"
        record['speed'] = "UNKNOWN"
        record['time'] = "UNKNOWN"

      print(json.dumps(record))
      #print("%s,%s,%s,%s,%s,%s,%s" % (ip, j['Serial'], record))
      #print(r.content.decode("utf-8").rstrip())
  except ConnectTimeout:
    logging.error("Thread-%d: Connection timeout for %s" % (threadId, ip))
  except ConnectionError as e:
    logging.error("Thread-%d: Connection error for %s - %s" % (threadId, ip, str(e)))
  except ValueError:
    logging.error("Thread-%d: Failed to decode content from $s as JSON" % (ip, threadId))

class getData (threading.Thread):
  def __init__(self, id, hosts):
    logging.info("Thread-%d: Started" % id)
    threading.Thread.__init__(self)
    self.hosts = hosts
    self.id = id

  def run(self):
    while self.hosts.empty() == False:
      ip = self.hosts.get()
      logging.info("Thread-%d: Reading from %s" % (self.id, ip))
      get_data(ip, self.id)
      self.hosts.task_done()
    logging.info("Thread-%d: finished" % self.id)


if __name__ == '__main__':
  hosts = Queue()

  # Load list of hosts from file
  filename = sys.argv[1]
  with open(filename) as f:
    for host in f:
      hosts.put(host.rstrip())

  t = []
  numThreads = 500
  for i in range(numThreads):
    thread1 = getData(i, hosts)
    thread1.start()
    t.append(thread1)
