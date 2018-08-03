#!/usr/bin/env python

import json
import os
import sys
if not (sys.path[0] + "/modules") in sys.path:
    sys.path.append(sys.path[0] + "/modules")
import console
import mathtools

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

def load_server_info():
    info = {}
#    file_path = "%s/data/pptp/configs/ipduoduo_servers.txt" % sys.path[0]
    file_path = "%s/data/pptp/configs/bzy_servers.txt" % sys.path[0]
    try:
        fd = open(file_path)
        if fd:
            lines = fd.readlines()
        fd.close()
    except Exception, e:
        console.log.warning("load_server_info: %r" % e)
    for line in lines:
        try:
            data = json.loads(line)
            if not data["enabled"]:
                continue
            name = data["server"]
            rate = data["rate"]
            mppe = data["mppe"]
            info[name] = {"rate": rate, "mppe": mppe}
        except Exception, e:
            console.log.warning("load_server_info: %r" % e)
    return info

def load_account_info():
    info = {}
    file_path = "%s/data/pptp/configs/accounts.txt" % sys.path[0]
    try:
        fd = open(file_path)
        if fd:
            lines = fd.readlines()
        fd.close()
    except Exception, e:
        console.log.warning("load_account_info: %r" % e)
    for line in lines:
        try:
            data = json.loads(line)
            slave = data["slave"]
            if not slave:
                continue
            username = data["username"]
            password = data["password"]
            info[slave] = {"username": username, "password": password}
        except Exception, e:
            console.log.warning("load_account_info: %r" % e)
    return info

def build_pptp_assignment(name, server_info, account_info):
    assignment = {}
    servers = server_info.keys()
    box = {}
    for server in servers:
        box[server] = server_info[server]["rate"]
    slaves = account_info.keys()
    for slave in slaves:
        picked = mathtools.draw_from_box(box)
        mppe = server_info[picked]["mppe"]
        username = account_info[slave]["username"]
        password = account_info[slave]["password"]
        assignment[slave] = {"server": picked, "username": username, "password": password, "mppe": mppe}
    file_path = "%s/data/pptp/slaves/%s.json" % (sys.path[0], name)
    try:
        fd = open(file_path, "w+")
        fd.write(json.dumps(assignment, indent = 2))
        fd.close()
    except Exception, e:
        console.log.warning("build_pptp_assignment: %r" % e)

def main():
    init_log("refresh_pptp_assignment")
    server_info = load_server_info()
    account_info = load_account_info()
    build_pptp_assignment("default", server_info, account_info)

if __name__ == '__main__':
    main()
    sys.exit(0)
