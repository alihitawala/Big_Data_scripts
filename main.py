import os, fnmatch, codecs, re
import json
from pprint import pprint

JOB_SUBMITTED = "JOB_SUBMITTED"
JOB_INITED = "JOB_INITED"
TASK_STARTED = "TASK_STARTED"
TASK_FINISHED = "TASK_FINISHED"
JOB_FINISHED = "JOB_FINISHED"

class Event :
    def __init__(self, type, name, id, time):
        self.type = type
        self.name = name
        self.id = id
        self.time = time

class Timeline:
    def __init__(self, type, id, start, end):
        self.type = type
        self.id = id
        self.start = start
        self.end = end

    def setEnd(self, end):
        self.end = end


def find(pattern, path):
    result = []
    for root, dirs, files in os.walk(path):
        for name in files:
            if fnmatch.fnmatch(name, pattern):
                result.append(os.path.join(root, name))
    return result

files = find('*.jhist', 'vm3/history/done')
for file in files:
    with open(file, 'r') as fin:
        data = fin.read().splitlines(True)
    data_new = []
    for line in data:
        if (line != "\n"):
            data_new.append(line)
        else:
            print "got empty line"
    data = data_new
    if (data[0][0] == 'A'):
        print "removing first line"
        with open(file, 'w') as fout:
            fout.writelines(data[1:])
    else:
        with open(file, 'w') as fout:
            fout.writelines(data[0:])

data = []
for file in files:
    with codecs.open(file,'rU','utf-8') as f:
        for line in f:
           data.append(json.loads(line))

task_started = []
domain_submit = "org.apache.hadoop.mapreduce.jobhistory.JobSubmitted"
domain_jfinish = "org.apache.hadoop.mapreduce.jobhistory.JobFinished"
domain_init = "org.apache.hadoop.mapreduce.jobhistory.JobInited"
domain_tstart = "org.apache.hadoop.mapreduce.jobhistory.TaskStarted"
domain_tfinish = "org.apache.hadoop.mapreduce.jobhistory.TaskFinished"
events = []
for task in data:
    if task['type'] == 'JOB_SUBMITTED':
        events.append(Event(
            task['type'],
            task['event'][domain_submit]['jobid'],
            task['event'][domain_submit]['jobid'],
            int(task['event'][domain_submit]['submitTime'])
        ))
    if task['type'] == 'JOB_INITED':
        events.append(Event(
            task['type'],
            task['event'][domain_init]['jobid'],
            task['event'][domain_init]['jobid'],
            int(task['event'][domain_init]['launchTime'])
        ))
    if task['type'] == 'JOB_FINISHED':
        events.append(Event(
            task['type'],
            task['event'][domain_jfinish]['jobid'],
            task['event'][domain_jfinish]['jobid'],
            int(task['event'][domain_jfinish]['finishTime'])
        ))
    if task['type'] == 'TASK_STARTED':
        events.append(Event(
            task['type'],
            task['event'][domain_tstart]['taskType'],
            task['event'][domain_tstart]['taskid'],
            int(task['event'][domain_tstart]['startTime'])
        ))
    if task['type'] == 'TASK_FINISHED':
        events.append(Event(
            task['type'],
            task['event'][domain_tfinish]['taskType'],
            task['event'][domain_tfinish]['taskid'],
            int(task['event'][domain_tfinish]['finishTime'])
        ))

# To return a new list, use the sorted() built-in function...
sorted_events = sorted(events, key=lambda x: x.time, reverse=False)
base_time = sorted_events[0].time
timelines = []
for event in sorted_events:
    # print event.type + " " + event.id + " " + event.name + " " + str(event.time-base_time)
    if event.type == JOB_SUBMITTED:
        timelines.append(Timeline("Job", event.id, event.time, event.time))
    elif event.type == JOB_FINISHED or event.type == TASK_FINISHED:
        for e in timelines:
            if e.id == event.id:
                e.end = event.time
    elif event.type == TASK_STARTED:
        name = "TASK" + "_" + event.name
        timelines.append(Timeline(name, event.id, event.time, event.time))

for t in timelines:
    print t.type, t.id, t.start, t.end