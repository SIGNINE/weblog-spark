from datetime import datetime, timedelta
import re

SESSION_WINDOW = 15 # min
TIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"
LINE_FORMAT = re.compile("([^ ]*) ([^ ]*) ([^ ]*):([0-9]*) ([^ ]*) ([-.0-9]*) ([-.0-9]*) ([-.0-9]*) (-|[0-9]*) (-|[0-9]*) (-|[0-9]*) ([0-9]*) \"([^ ]*) ([^ ]*) ([^ ]*)\" \"(.*)\" ([^ ]*) ([^ ]*)$")
INDEX = { 'session_id': 0, 'time': 1, 'url': 14 } # For final session RDD

"""
Match log line with regex and return list of data points
"""
def parse(line):
    m = LINE_FORMAT.search(line)
    return [ m.group(i) for i in range(1, 18) ]

"""
Used for finding log lines that failed parsing
Returns bad lines with (False, (line, error msg))
"""
def validate_log_line(line):
    try:
        parse(line)
    except Exception as e:
        return (False, (line, e.message))
    return (True, (line, ''))

def process_log_line(line):
    row = parse(line)
    row[0] = datetime.strptime(row[0], TIME_FORMAT)
    ip = row[2]
    return (ip, row)

"""
Iterates through entries and groups them by session
Each session has a session_id which is <ip>_<session number>
Returns (session_id, sessions) tuple
"""
def sessionize(ip, entries):
    sessions = []
    session_num = 1
    session_id = "%s_%s"%(ip, str(session_num))
    current_session = (session_id, [])
    prev_time = entries[0][0]

    for e in entries:
        current_time = e[0]
        if (current_time - prev_time) > timedelta(minutes=SESSION_WINDOW):
            sessions.append(current_session)
            session_num += 1
            session_id = "%s_%s"%(ip, session_num)
            current_session = (session_id, [])
        prev_time = current_time
        e.insert(0, session_id)
        current_session[1].append(e)
    return sessions

"""
Calculates duration of sessions by subracting times of last and first row entries of a session
Takes (session_id, sessions) tuples and return (session_id, duration) tuples
"""
def session_duration(session):
    entries = session[1]
    delta = (entries[-1][INDEX['time']] - entries[0][INDEX['time']]).seconds
    return (session[0], delta)

"""
Used to check for ideal session window
Takes (ip, sessions) tuple, iterates through sessions and calculates duration from previous session
Returns (ip, list of durations) tuple
"""
def time_between_sessions(s):
    sessions = s[1]
    if len(sessions) == 1:
        return (k, [0])
    else:
        deltas = []
        for i in range(1, len(sessions)):
            t = (sessions[i][1] - sessions[i-1][2]).seconds
            deltas.append(t)
        return (s[0], deltas)
