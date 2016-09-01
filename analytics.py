from pyspark import SparkConf, SparkContext
from helpers import *

conf = SparkConf().setAppName("web logs analysis")
sc = SparkContext(conf=conf)

logs = sc.textFile('/root/data/2015_07_22_mktplace_shop_web_log_sample.log')

# Used to check that all log entries were successfuly parsed
# a = logs.map(validate_log_line).filter(lambda x: x[0] == True)
# print a.count()
# # 1158500
# print logs.count()
# # 1158500

# Number of log entries with parsing errors
# a = logs.map(validate_log_line).filter(lambda x: x[0] == False).groupByKey().mapValues(list)
# res = a.collect()
# print len(res[0][1])
# 162

"""
Sessionize
1. Group by ip
2. Sort values by time
3. Sort (ip, rows) tuples by length of rows
4. Build session from rows
"""
sessions = logs.map(process_log_line).groupByKey() \
                                     .mapValues(list) \
                                     .map(lambda (k, v): (k, sorted(v, key=lambda x: x[0]))) \
                                     .sortBy(lambda (k, v): len(v), ascending=False) \
                                     .flatMap(lambda (k, v): sessionize(k, v))
# Total sessions
print "Total sessions"
print sessions.count()
print
# 20294

# Max session time (top 10 most engaged users)
session_durations = sessions.map(session_duration).sortBy(lambda (k, v): v, ascending=False)
res = session_durations.take(10)

print "Top 10 session times (s)"
for i in res: print i[0], i[1]
print
# session_id duration (s)
# 52.74.219.71_4 2069
# 119.81.61.166_4 2068
# 106.186.23.95_4 2068
# 125.19.44.66_4 2068
# 54.251.151.39_4 2067
# 192.8.190.10_2 2067
# 203.191.34.178_1 2065
# 213.239.204.204_1 2065
# 180.179.213.70_4 2065
# 180.151.80.140_3 2065

# Average session time
total_sessions = sessions.count()
total_time = session_durations.map(lambda x: x[1]).reduce(lambda x, y: x + y)
print "Average session time (s)"
print total_time / total_sessions
print

"""
Unique URL visits per session
1. Map (session, rows) tuples, put each url in a set and return length of set
2. Sort (session, uniqe_hits) tuples by uniqe_hits
"""
url_hits = sessions.map(lambda (k, v): (k, len(set([row[INDEX['url']] for row in v ])))) \
                   .sortBy(lambda x: x[1], ascending=False)
res = url_hits.take(10)

print "Most unique hits per session"
for i in res: print i[0], i[1]
print
# session_id hits
# 52.74.219.71_4 9532
# 119.81.61.166_5 8016
# 52.74.219.71_5 5478
# 119.81.61.166_7 3928
# 119.81.61.166_8 3637
# 119.81.61.166_0 3334
# 52.74.219.71_8 2907
# 119.81.61.166_6 2786
# 106.186.23.95_4 2731
# 52.74.219.71_3 2572

"""
Average time between sessions (s)
1. Convert (session_id, sessions) tuples to (ip, (session_num, session_start_time, session_end_time)) tuples
2. Group by ip
3. Map through and return (ip, session_deltas) tuples
4. Remove all tuples with only 1 session
5. Map through and return (ip, average_session_delta) tuples
6. Add all average_session_deltas of all tuples (reduce)
"""
session_deltas = sessions.map(lambda (k, v): (k.split('_')[0], (k.split('_')[1], v[0][1], v[-1][1]))) \
                        .groupByKey() \
                        .mapValues(lambda x: sorted(x, key=lambda y: y[0])) \
                        .map(time_between_sessions) \
                        .filter(lambda x: len(x[1]) > 1) \
                        .map(lambda (k, v): (k, sum(v)/len(v))) \
                        .map(lambda x: x[1])

total_time = session_deltas.reduce(lambda x, y: x + y)
avg_time = total_time / session_deltas.count()

print "Average time between sessions (s)"
print avg_time
# 10557 seconds
# 3 hours
