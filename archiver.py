#!/usr/bin/env python

import json
import os
import sys
import time
if not (sys.path[0] + "/modules") in sys.path:
    sys.path.append(sys.path[0] + "/modules")
import console

def init_log(name):
    log_dir = "/var/log/ati/"
    log_file = log_dir + name + ".log"
    if not os.path.exists(log_dir):
        os.mkdir(log_dir)
    elif not os.path.isdir(log_dir):
        print "critical: unable to initialize log file, exit..."
        return False
    console.init(name, log_file)
    return True

def load_json_file(filename):
    data = None
    try:
        if os.path.exists(filename):
            f = open(filename, "r")
            content = f.read()
            f.close()
            data = json.loads(content)
    except Exception, e:
        pass
    return data

def dump_json_data(filename, data):
    try:
        f = open(filename, "w")
        f.write(json.dumps(data, indent = 2))
        f.close()
    except Exception, e:
        pass

def load_track():
    track = None
    track_file = sys.path[0] + "/runtime/archiver.track.json"
    data = load_json_file(track_file)
    if data:
        track = {
            "date": int(time.mktime(time.strptime(str(data["date"]), "%Y%m%d"))),
            "slot": int(data["slot"])
        }
    return track

def save_track(track):
    track_file = sys.path[0] + "/runtime/archiver.track.json"
    dump_json_data(track_file, track)

def main():
    init_log("archiver")
    last_date = None
    last_slot = None
    track = load_track()
    if track:
        last_date = track["date"]
        last_slot = track["slot"]
    new_track = {}
    now = time.localtime()
    today = int(time.mktime(time.strptime(time.strftime("%Y%m%d", now), "%Y%m%d")))
    timeslot = now.tm_hour * 60
    runtime_dir = sys.path[0] + "/runtime/"
    date_strs = os.listdir(runtime_dir)
    date_strs.sort()
    for date_str in date_strs:
        if date_str == "archiver.track.json":
            continue
        new_track["date"] = date_str
        date = int(time.mktime(time.strptime(date_str, "%Y%m%d")))
        if date < last_date:
            continue
        base_dir = runtime_dir + date_str + "/"
        minute_dir = base_dir + "m/"
        hour_dir = base_dir + "h/"
        if not os.path.exists(hour_dir):
            os.makedirs(hour_dir)
        day_dir = base_dir + "d/"
        if not os.path.exists(day_dir):
            os.makedirs(day_dir)
        lord_hour_data = None
        lord_day_data = {}
        task_hour_data = None
        task_day_data = {}
        slave_hour_data = None
        slave_day_data = {}
        for i in range(1440):
            new_track["slot"] = str(i) 
            if i % 60 == 0:
                if lord_hour_data:
                    lord_hour_file = "%slord.%d.json" % (hour_dir, i / 60)
                    dump_json_data(lord_hour_file, lord_hour_data)
                lord_hour_data = {}
                if task_hour_data:
                    task_hour_file = "%stask.%d.json" % (hour_dir, i / 60)
                    dump_json_data(task_hour_file, task_hour_data)
                task_hour_data = {}
                if slave_hour_data:
                    slave_hour_file = "%sslave.%d.json" % (hour_dir, i / 60)
                    dump_json_data(slave_hour_file, slave_hour_data)
                slave_hour_data = {}
            if date == last_date and i < last_slot:
                    continue
            if date == today and i == timeslot:
                    break
            console.log.info("%s: %d" % (date_str, i))
            lord_file = "%slord.%d.json" % (minute_dir, i)
            lord_data = load_json_file(lord_file)
            if not lord_data:
                continue
            names = lord_data.keys()
            for name in names:
                if name not in lord_hour_data:
                    lord_hour_data[name] = {"impression": 0, "click": 0}
                lord_hour_data[name]["impression"] += int(lord_data[name]["impression"])
                lord_hour_data[name]["click"] += int(lord_data[name]["click"])
                if name not in lord_day_data:
                     lord_day_data[name] = {"impression": 0, "click": 0}
                lord_day_data[name]["impression"] += int(lord_data[name]["impression"])
                lord_day_data[name]["click"] += int(lord_data[name]["click"])
            task_file = "%stask.%d.json" % (minute_dir, i)
            task_data = load_json_file(task_file)
            if not task_data:
                continue
            names = task_data.keys()
            for name in names:
                if name not in task_hour_data:
                    task_hour_data[name] = {"pass": 0, "fail": 0, "impression": 0, "click": 0}
                task_hour_data[name]["pass"] += int(task_data[name]["pass"])
                task_hour_data[name]["fail"] += int(task_data[name]["fail"])
                task_hour_data[name]["impression"] += int(task_data[name]["impression"])
                task_hour_data[name]["click"] += int(task_data[name]["click"])
                if name not in task_day_data:
                     task_day_data[name] = {"pass": 0, "fail": 0, "impression": 0, "click": 0}
                task_day_data[name]["pass"] += int(task_data[name]["pass"])
                task_day_data[name]["fail"] += int(task_data[name]["fail"])
                task_day_data[name]["impression"] += int(task_data[name]["impression"])
                task_day_data[name]["click"] += int(task_data[name]["click"])
            slave_file = "%sslave.%d.json" % (minute_dir, i)
            slave_data = load_json_file(slave_file)
            if not slave_data:
                continue
            names = slave_data.keys()
            for name in names:
                if name not in slave_hour_data:
                    slave_hour_data[name] = {"pass": 0, "fail": 0, "impression": 0, "click": 0}
                slave_hour_data[name]["pass"] += int(slave_data[name]["pass"])
                slave_hour_data[name]["fail"] += int(slave_data[name]["fail"])
                slave_hour_data[name]["impression"] += int(slave_data[name]["impression"])
                slave_hour_data[name]["click"] += int(slave_data[name]["click"])
                if name not in slave_day_data:
                     slave_day_data[name] = {"pass": 0, "fail": 0, "impression": 0, "click": 0}
                slave_day_data[name]["pass"] += int(slave_data[name]["pass"])
                slave_day_data[name]["fail"] += int(slave_data[name]["fail"])
                slave_day_data[name]["impression"] += int(slave_data[name]["impression"])
                slave_day_data[name]["click"] += int(slave_data[name]["click"])
        lord_day_file = "%slord.json" % day_dir
        dump_json_data(lord_day_file, lord_day_data)
        task_day_file = "%stask.json" % day_dir
        dump_json_data(task_day_file, task_day_data)
        slave_day_file = "%sslave.json" % day_dir
        dump_json_data(slave_day_file, slave_day_data)
    save_track(new_track)

if __name__ == '__main__':
    main()
    sys.exit(0)
