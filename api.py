#!/usr/bin/env python

import tornado.ioloop
import tornado.web

import datetime
import json
import logging
import os
import Queue
import signal
import sys
import threading
import time

def init_log(name):
    log_dir = "/var/log/ati/"
    log_file = log_dir + name + ".log"
    if not os.path.exists(log_dir):
        os.mkdir(log_dir)
    elif not os.path.isdir(log_dir):
        print "critical: unable to initialize log file, exit..."
        return False
    log = logging.getLogger()
    log.setLevel(logging.INFO)
    formatter = logging.Formatter("[%(name)s] [%(asctime)s] [%(levelname)s] [%(filename)s:%(lineno)d %(funcName)s()] \"%(message)s\"")
    filehandler = logging.handlers.RotatingFileHandler(log_file, mode = "a", maxBytes = 1073741824, backupCount = 4)
    filehandler.setFormatter(formatter)
    streamhandler = logging.StreamHandler(sys.stdout)
    streamhandler.setFormatter(formatter)
    log.addHandler(filehandler)
    log.addHandler(streamhandler)
    return True

def handler(signum, frame):
    global stopping
    if stopping:
        return
    stopping = True
    logging.info("received signal: %d" % signum)
    tornado.ioloop.IOLoop.instance().add_callback(shutdown)

def load_pptp_data():
    data = {}
    pptp_dir = "%s/data/pptp/slaves" % sys.path[0]
    files = os.listdir(pptp_dir)
    for file in files:
        if file[-5:] != ".json":
            continue
        type = file[:-5]
        try:
            f = open(pptp_dir + "/" + file)
            if f:
                data[type] = json.load(f)
            f.close()
        except Exception, e:
            logging.warning("load_pptp_data: %r" % e)
    return data

def load_pptp_route_targets():
    targets = []
    route_targets_file = "%s/data/route/targets.json" % sys.path[0]
    try:
        f = open(route_targets_file)
        if f:
            targets = json.load(f)
        f.close()
    except Exception, e:
        logging.warning("load_pptp_route_targets: %r" % e)
    return targets

class TrackHandler(tornado.web.RequestHandler):
    def get(self, role, info):
        tornado.ioloop.IOLoop.current().spawn_callback(self.track, role, info)

    def track(self, role, info):
        self.application.task_track_queue.put({
            "role": str(role),
            "info": str(info)
        })

class PptpAccountHandler(tornado.web.RequestHandler):
    def get(self, type, host):
        runtime = self.application.pptp_account_runtime
        response = {}
        if type in runtime:
            if host in runtime[type]:
                fields = runtime[type][host].keys()
                for field in fields:
                    response[field] = runtime[type][host][field]
        self.write(json.dumps(response))

class PptpRouteTargetsHandler(tornado.web.RequestHandler):
    def get(self):
        runtime = self.application.pptp_route_targets_runtime
        self.write(json.dumps(runtime))

class PptpTrackHandler(tornado.web.RequestHandler):
    def get(self, info):
        tornado.ioloop.IOLoop.current().spawn_callback(self.track, info)

    def track(self, info):
        self.application.pptp_track_queue.put(str(info))

class ApiApplication(tornado.web.Application):
    pptp_account_runtime = {}
    pptp_route_targets_runtime = []
    timeslot = None
    datestr = None
    task_track_queue = Queue.Queue()
    pptp_track_queue = Queue.Queue()
    statistics = {}
    def __init__(self):
        handlers = [
            (r"/api/track/(?P<role>lord|slave)/(?P<info>.+)", TrackHandler),
            (r"/api/pptp/account/(?P<type>.+?)/(?P<host>.+)", PptpAccountHandler),
            (r"/api/pptp/route_targets", PptpRouteTargetsHandler),
            (r"/api/pptp/track/(?P<info>.+)", PptpTrackHandler),
        ]
        self.get_datestr()
        self.get_timeslot()
        tornado.ioloop.PeriodicCallback(self.get_timeslot, 1000).start()
        task_track_thread = threading.Thread(target = self.task_tracker)
        task_track_thread.setDaemon(True)
        task_track_thread.start()
        pptp_track_thread = threading.Thread(target = self.pptp_tracker)
        pptp_track_thread.setDaemon(True)
        pptp_track_thread.start()
        tornado.web.Application.__init__(self, handlers)

    def get_timeslot(self):
        now = time.localtime()
        timeslot = now.tm_hour * 60 + now.tm_min
        if timeslot != self.timeslot:
            self.statistics[timeslot] = {"lord": {}, "task": {}, "slave": {}, "pptp": {}}
            old = self.timeslot
            self.timeslot = timeslot
            if old:
                self.dump_track(old)
            if timeslot == 0:
                self.get_datestr()
            self.pptp_account_runtime = load_pptp_data()
            self.pptp_route_targets_runtime = load_pptp_route_targets()

    def get_datestr(self):
        now = time.localtime()
        self.datestr = time.strftime("%Y%m%d", now)
        dumpdir = "%s/runtime/%s/m" % (sys.path[0], self.datestr)
        if not os.path.exists(dumpdir):
            os.makedirs(dumpdir)

    def task_tracker(self):
        global stopping
        while not stopping:
            try:
                message = self.task_track_queue.get(timeout = 1)
                role = message["role"]
                info = message["info"]
                pieces = info.split("&")
                data = {}
                for piece in pieces:
                    key, value = piece.split("=")
                    data[key] = value
                if role == "lord":
                    self.track_lord(data)
                elif role == "slave":
                    self.track_slave(data)
            except Exception, e:
                pass

    def track_lord(self, data):
        try:
            timeslot = int(data["ts"])
            name = data["name"]
            imp = data["imp"]
            clk = data["clk"]
            lord_statistics = self.statistics[timeslot]["lord"]
            lord_statistics[name] = {
                "impression": imp,
                "click": clk
            }
        except Exception, e:
            logging.warning("track_lord: %r, %d" % e, self.timeslot)

    def track_slave(self, data):
        try:
            name = data["name"]
            state = data["state"]
            imp = int(data["imp"])
            clk = int(data["clk"])
            slave = data["slave"]
            task_statistics = self.statistics[self.timeslot]["task"]
            if name not in task_statistics:
                task_statistics[name] = {
                    "pass": 0,
                    "fail": 0,
                    "impression": 0,
                    "click": 0
                }
            task_item = task_statistics[name]
            task_item[state] += 1
            if state == "pass":
                if imp:
                    task_item["impression"] += 1
                if clk:
                    task_item["click"] += 1
            slave_statistics = self.statistics[self.timeslot]["slave"]
            if slave not in slave_statistics:
                slave_statistics[slave] = {
                    "pass": 0,
                    "fail": 0,
                    "impression": 0,
                    "click": 0
                }
            slave_item = slave_statistics[slave]
            slave_item[state] += 1
            if state == "pass":
                if imp:
                    slave_item["impression"] += 1
                if clk:
                    slave_item["click"] += 1
        except Exception, e:
            logging.warning("track_slave: %r" % e)

    def pptp_tracker(self):
        global stopping
        while not stopping:
            try:
                message = self.pptp_track_queue.get(timeout = 1)
                pieces = message.split("&")
                data = {}
                for piece in pieces:
                    key, value = piece.split("=")
                    data[key] = value
                self.track_pptp(data)
            except Exception, e:
                pass

    def track_pptp(self, data):
        try:
            server = data["server"]
            username = data["username"]
            state = data["state"]
            duration = int(data["duration"])
            pptp_statistics = self.statistics[self.timeslot]["pptp"]
            if server not in pptp_statistics:
                pptp_statistics[server] = {}
            if username not in pptp_statistics[server]:
                pptp_statistics[server][username] = {
                    "ok": 0,
                    "error": 0,
                    "min": -1,
                    "max": -1,
                    "sum": 0,
                }
            if state == "ok":
                pptp_statistics[server][username]["ok"] += 1
            elif state == "error":
                pptp_statistics[server][username]["error"] += 1
            else:
                return
            if pptp_statistics[server][username]["min"] < 0 or duration < pptp_statistics[server][username]["min"]:
                pptp_statistics[server][username]["min"] = duration
            if pptp_statistics[server][username]["max"] < 0 or duration > pptp_statistics[server][username]["max"]:
                pptp_statistics[server][username]["max"] = duration
            pptp_statistics[server][username]["sum"] += duration
        except Exception, e:
            logging.warning("track_pptp: %r" % e)

    def dump_track(self, timeslot):
        dumpdir = "%s/runtime/%s/m" % (sys.path[0], self.datestr)
        track = self.statistics[timeslot]
        names = track.keys()
        for name in names:
            filename = "%s/%s.%d.json" % (dumpdir, name, timeslot)
            f = open(filename, "w+")
            f.write(json.dumps(self.statistics[timeslot][name], indent = 2))
            f.close()
        del(self.statistics[timeslot])

def shutdown():
    global server
    logging.info("stopping application...");
    server.stop()
    io_loop = tornado.ioloop.IOLoop.instance()
    deadline = time.time() + 5
    def stop_loop():
        now = time.time()
        if now < deadline and (io_loop._callbacks or io_loop._timeouts):
            io_loop.add_timeout(now + 1, stop_loop)
        else:
            io_loop.stop()
    stop_loop()

def start():
    global app
    global server
    app = ApiApplication()
    server = tornado.httpserver.HTTPServer(app)
    server.listen(37000)
    logging.info("starting application...");
    tornado.ioloop.IOLoop.current().start()

def main():
    init_log("api_server")
    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)
    start()
    logging.info("application stopped")

if __name__ == '__main__':
    stopping = False
    server = None
    main()
    sys.exit(0)
