import sys
import json
import folium
import logging
from multiprocessing import Pool
import os

logging.basicConfig(level=logging.INFO)

def buildTrackMap(serial, vehicle):
    points = []
    pointsWithData = []
    total_lat = 0
    total_long = 0
    avg_lat = 0
    avg_long = 0
    count = 0
    bad_location_values = ["UNKNOWN", "0"]
    for data_point in vehicle:
        if data_point['lat'] in bad_location_values or data_point['long'] in bad_location_values:
            continue
        lat = float(data_point['lat'])
        long = float(data_point['long'])
        timestamp = data_point['timestamp']
        speed = data_point['speed']
        heading = data_point['heading']
        points.append((lat,long))
        pointsWithData.append((lat,long, timestamp, speed, heading))
        total_lat += lat
        total_long += long
        count += 1

    if len(points) > 0:
        avg_lat = total_lat / count
        avg_long = total_long / count
        logging.info("Centering map on %f, %f" % (avg_lat, avg_long))
        tracker_map = folium.Map(location=[avg_lat, avg_long], zoom_start=13)
    
        for point in pointsWithData:
            google_maps_url = """<a target="_blank" href="https://maps.google.com/maps?q=%f,%f" >%f,%f</a>""" % (point[0], point[1], point[0], point[1])
            folium.Marker([point[0], point[1]], popup="Time: %s<br/>Location: %s<br/>Heading: %s<br/>Speed:%s" % (point[2], google_maps_url, point[3],point[4])).add_to(tracker_map)
    
        folium.PolyLine(points, color="red", weight=2.5, opacity=1).add_to(tracker_map)
        return tracker_map
    else:
      return None

def buildPointMap(vehicles):
    pointMap = folium.Map(location=[30, -97], zoom_start=4)

    for serial in vehicles:
        lat = vehicles[serial][0]['lat']
        long = vehicles[serial][0]['long']
        if lat == "UNKNOWN" or lat == "0.0":
            continue
        folium.Marker([float(lat), float(long)], popup="%s: %s" % (vehicles[serial][0]["timestamp"], """<a target="_blank" href="tracks/%s.html">%s</a> """ % (serial, serial))).add_to(pointMap)
    return pointMap

def poolMap(vehicleTuple):
    global bad
    (serial, vehicle) = vehicleTuple
    logging.info("Building map for %s" % (serial))
    trackerMap = buildTrackMap(serial, vehicle)
    if trackerMap != None:
        filename = "tracks/" + serial + ".html"
        trackerMap.save(filename)
    else:
        bad.append(serial)
    
if __name__ == '__main__':
    data=[]
    reject=[]
    datafile = sys.argv[1]
    with open(datafile, "r") as f:
        for line in f:
            try:
                data.append(json.loads(str.rstrip(line)))
            except json.JSONDecodeError:
                reject.append(line)

    vehicles = {}
    for record in data:
        try:
            serial = record['serial']
            if serial not in vehicles:
                vehicles[record['serial']] = []
            try:
                vehicles[record['serial']].append({"ip": record['ip'], "lat": float(record['lat']), 
                                               "long": float(record['long']), "heading": record['heading'],
                                               "speed": record['speed'], "timestamp": record['timestamp']})
            except ValueError:
                vehicles[record['serial']].append({"ip": record['ip'], "lat": record['lat'], 
                                               "long": record['long'], "heading": record['heading'],
                                               "speed": record['speed'], "timestamp": record['timestamp']})            
        except KeyError:
            logging.error("No cell ip in %s" % record)

    if os.path.isdir("tracks") is False:
        os.mkdir("tracks")

    bad = []
    p = Pool(4)
    p.map(poolMap, (vehicles.items()))

    # Remove any bad records before building point map
    logging.info("Removing %d bad records" % len(bad))
    for serial in bad:
        del vehicles[serial]
    nationalMap = buildPointMap(vehicles)
    nationalMap.save("index.html")
