""" Standalone script to analyze typical SciON container log files """

import argparse
import datetime
import os
import re
import sys
import time

DEFAULT_LOGS_DIR = "logs"

RE_CONSOLE_ESC = re.compile(r'\x1b[^m]*m')
RE_EXC_MSG = re.compile(r': ')
RE_LOGENTRY = re.compile(r'(20\d\d-\d\d-\d\d \d\d:\d\d:\d\d).+?(INFO|DEBUG|WARNING|ERROR|CRITICAL).*.:\d+ (.*)$')
RE_RPC_REQ = re.compile(r'SVC RPC REQUEST \((\d+)\) - (.*)$')
RE_RPC_RESP = re.compile(r'SVC RPC REQUEST \((\d+)\) (RESP|ERROR) \((\d+)\) - (.*)$')
RE_RELDATE = re.compile(r'(\d\.?\d*)(s|m|h|d)')

MULTIPLIER = dict(s=1, m=60, h=3600, d=86400)


class LogAnalyzer(object):

    def __init__(self):
        self.logdata = []
        self.logfiles = {}
        self.rpccalls = []
        self.rpc_by_id = {}
        self.rpc_errors = {}
        self.starttime = 0.0
        self.scan_rotating = False
        self.verbose = 0


    def configure(self, opts):
        self.logdir = opts.logdir or DEFAULT_LOGS_DIR
        if not os.path.exists(self.logdir) or not os.path.isdir(self.logdir):
            errout("Directory '%s' not found" % self.logdir)
        self.list_services = bool(opts.list_services)
        self.scan_rotating = bool(opts.rotating)
        self.error_only = bool(opts.error_only)

        if opts.start:
            match = re.search(RE_RELDATE, opts.start)
            if match:
                offset, unit = match.groups()
                if unit not in MULTIPLIER:
                    errout("Illegal start time offset")
                self.starttime = time.time() - float(offset) * MULTIPLIER[unit]
                print " CONFIG: Use start time", self.starttime
            self.starttime = self.starttime or self._parse_ts(opts.start, "%Y-%m-%d %H:%M:%S")
            self.starttime = self.starttime or self._parse_ts(opts.start, "%Y-%m-%d")
            self.starttime = self.starttime or self._parse_ts(opts.start, "%Y%m%d%H%M%S")
            self.starttime = self.starttime or self._parse_ts(opts.start, "%Y%m%d")

        if opts.verbose:
            self.verbose = 1

    def _parse_ts(self, datestr, pattern=None):
        # Assume date strings are local not UTC
        pattern = pattern or "%Y-%m-%d %H:%M:%S"
        try:
            dt = datetime.datetime.strptime(datestr, pattern)
            ts = time.mktime(dt.timetuple())
            return ts
        except Exception:
            return 0.0

    def _collect_logfiles(self):
        num_lines, num_entries = 0, 0
        for filename in os.listdir(self.logdir):
            if not self.scan_rotating and not filename.endswith(".log"):
                continue
            if self.scan_rotating and ".log" not in filename:
                continue

            log_filename = os.path.join(self.logdir, filename)
            self.logfiles[log_filename] = dict()
            with open(log_filename, "r") as f:
                last_msg, running_exc, last_exc = "", [], ""
                for line in f:
                    line = re.sub(RE_CONSOLE_ESC, "", line)  # Remove ESC codes (console color)
                    num_lines += 1
                    match = re.search(RE_LOGENTRY, line)
                    if not match:
                        if not running_exc:
                            running_exc = [last_msg, "\n"]
                        running_exc.append(line)
                        continue
                    num_entries += 1
                    datestr, level, logmsg = match.groups()
                    if running_exc:
                        last_exc = "".join(running_exc)
                    last_msg, running_exc = logmsg, []

                    if self.list_services:
                        match1 = re.search(RE_RPC_REQ, logmsg)
                        if match1 and self._parse_ts(datestr) >= self.starttime:
                            rpc_num, rpc_url = match1.groups()
                            rpc_id = datestr + "/" + rpc_num
                            self.rpc_by_id[rpc_id] = dict(num=rpc_num, date=datestr, url=rpc_url, msg=logmsg)

                        match1 = re.search(RE_RPC_RESP, logmsg)
                        if match1:
                            rpc_num, resp_type, rpc_status, rpc_resp = match1.groups()
                            rpc_id = datestr + "/" + rpc_num
                            if rpc_id in self.rpc_by_id:
                                if resp_type == "ERROR":
                                    self.rpc_by_id[rpc_id].update(dict(status=rpc_status, errmsg=rpc_resp, error=True))
                                    if rpc_resp in self.rpc_errors:
                                        self.rpc_errors[rpc_resp] += 1
                                    else:
                                        self.rpc_errors[rpc_resp] = 1
                                    if  RE_EXC_MSG.split(rpc_resp, 1)[-1] in last_exc:
                                        self.rpc_by_id[rpc_id]["errtrace"] = last_exc
                                        last_exc = ""

                                else:
                                    self.rpc_by_id[rpc_id].update(dict(status=rpc_status, resp=rpc_resp))

        print "Read %s log entries in %s log files (%s lines read)" % (num_entries, len(self.logfiles), num_lines)


    def analyze(self):
        self._collect_logfiles()

        if self.list_services:
            for rpc_id in sorted(self.rpc_by_id):
                rpc_info = self.rpc_by_id[rpc_id]
                if self.error_only and "error" not in rpc_info:
                    continue
                print rpc_info["date"] + ":", rpc_info["msg"]
                if "status" in rpc_info:
                    print " ", rpc_info["status"], "-", rpc_info.get("resp", "")
                if "error" in rpc_info:
                    print "  ERROR:", rpc_info.get("errmsg", "")
                if "errtrace" in rpc_info:
                    print rpc_info["errtrace"]

        if self.error_only and self.rpc_errors:
            print "\nRPC error report:"
            for errmsg in sorted(self.rpc_errors):
                err_info = self.rpc_errors[errmsg]
                print "  ", errmsg, "(" + str(err_info) + ")"


def errout(message, code=1):
    print "ERROR", message
    sys.exit(code)

def main():
    print "===== LogAnalyzer ====="
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--logdir', action='store', help='Log file directory')
    parser.add_argument('-r', '--rotating', action='store_true', help='Scan also rotated log files')
    parser.add_argument('-s', '--start', action='store', help='Start date or since rel time e.g. 30m 1d')
    parser.add_argument('-ls', '--list_services', action='store_true', help='Show service calls')
    parser.add_argument('-e', '--error_only', action='store_true', help='Only show errors')
    parser.add_argument('-utc', '--utc', action='store_true', help='Interpret dates as UTC')
    parser.add_argument('-v', '--verbose', action='store', help='Verbose output')
    opts = parser.parse_args()

    la = LogAnalyzer()
    la.configure(opts)
    la.analyze()


if __name__ == '__main__':
    main()