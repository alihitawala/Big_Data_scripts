# -*- coding: utf-8-unix -*-

import datetime
import gantt

# Change font default
gantt.define_font_attributes(fill='black',
                             stroke='black',
                             stroke_width=0,
                             font_family="Verdana")
# Create some tasks
t1 = gantt.Task(name='tache1',
                start=datetime.date(2014, 12, 25),
                duration=4,
                percent_done=44,
                color="#FF8080")
t2 = gantt.Task(name='tache2',
                start=datetime.date(2014, 12, 28),
                duration=6,)
t7 = gantt.Task(name='tache7',
                start=datetime.date(2014, 12, 28),
                duration=5,
                percent_done=50)
t3 = gantt.Task(name='tache3',
                start=datetime.date(2014, 12, 25),
                duration=4,
                depends_of=[t1, t7, t2],)
t4 = gantt.Task(name='tache4',
                start=datetime.date(2015, 01, 01),
                duration=4,
                depends_of=t1,)
t5 = gantt.Task(name='tache5',
                start=datetime.date(2014, 12, 23),
                duration=3)
t6 = gantt.Task(name='tache6',
                start=datetime.date(2014, 12, 25),
                duration=4,
                depends_of=t7,)
t8 = gantt.Task(name='tache8',
                start=datetime.date(2014, 12, 25),
                duration=4,
                depends_of=t7,)
# Create a project
p1 = gantt.Project(name='Projet 1')
# Add tasks to this project
p1.add_task(t1)
p1.add_task(t7)
p1.add_task(t2)
p1.add_task(t3)
p1.add_task(t5)
p1.add_task(t8)
p1.make_svg_for_tasks(filename='test_full2.svg',
                     today=datetime.date(2014, 12, 31))