#!/usr/bin/python3
import pygal
import sys, json;
from datetime import datetime
import platform

step = 2
data = json.load(sys.stdin)
classes = set([n["device_class"] for n in data["nodes"]])

hist = pygal.Histogram(title="%s on %s" % (str(datetime.now()), platform.node()), x_title="Utilization %", y_title="# of OSDs")
for c in sorted(classes):
	bins = [[x, 0] for x in range(0,100,step)]
	for node in data["nodes"]:
		if node["device_class"] == c:
			u = (int)(node["utilization"]/step);
			bins[u][1] += 1;
	hist.add(c, [(b[1], b[0], b[0]+step) for b in bins])

hist.render_to_file('/dev/stdout')
