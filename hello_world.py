#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Code example for article
Распределенное выполнение Python задач...
"""

import time
import mesos.interface
from mesos.interface import mesos_pb2
import mesos.native

class SimpleScheduler(mesos.interface.Scheduler):
    """
    Mesos Scheduler Stub implementation
    """
    def __init__(self):
        self._next_id = 0

    def resourceOffers(self, driver, offers):
        print "Accepting offer: ", offers[0].id.value

        task = mesos_pb2.TaskInfo()
        # Required field
        task.name = "Simple Scheduler Task"
        # Required field, should be unique for framework
        task.task_id.value = str(self._next_id)
        self._next_id += 1

        # Required, specifies offer
        task.slave_id.value = offers[0].slave_id.value
        # command to execute
        task.command.value = "echo Hello Mesos World"

        cpus = task.resources.add()
        cpus.name = "cpus"
        cpus.type = mesos_pb2.Value.SCALAR
        cpus.scalar.value = 1

        mem = task.resources.add()
        mem.name = "mem"
        mem.type = mesos_pb2.Value.SCALAR
        mem.scalar.value = 1 # Megabyte


        driver.launchTasks([offers[0].id], [task])


def main():
    """
    Prepares framework and starts driver.
    Blocks until driver exit.
    """
    framework = mesos_pb2.FrameworkInfo()
    framework.user = ""
    framework.name = "Simple Scheduler Framework"

    scheduler = SimpleScheduler()

    driver = mesos.native.MesosSchedulerDriver(
        scheduler,
        framework,
        "localhost:5050"
    )
    driver.start()
    time.sleep(10)
    driver.stop()

if __name__ == "__main__":
    main()
