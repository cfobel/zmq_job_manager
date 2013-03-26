zmq_job_manager
===============

# Overview #

The main goal here is to develop a central job manager that can delegate jobs
to worker processes.

## Optional broker ##

Rather than connecting the workers directly to the job manager, one or more
brokers may instead be connected to the job manager.  Each broker may then
service requests for the job manager to near-by workers, rather than having the
worker communicate directly with the job manager.  The motivating use case for
this functionality is the management of jobs running on a cluster, where:

 * The worker processes do not have access to the internet
 * There is at least one node that can connect both to the internet and to the
   worker processes (which we'll refer to as a `'cluster bridge'`)
 * There is an internet-accessible server, on which the job manager is running

Given these three conditions, the following setup can be used to interface
between the remote workers (which do not have internet access) and the job
manager (on a different local network than the workers, but accessible over the
internet):

                  <-- TCP internet -->  <-- TCP cluster network -->
    Machine : [Job server]  | [Cluster bridge node] | [Cluster worker]
    Process : [Job manager] | [Broker]              | [Worker]
    Network : Internet only | Internet and cluster  | Cluster only


# Demo usage #

## Terminal 1 ##

Run:

    python zmq_job_manager/master.py 'tcp://*:2468'


## Terminal 2 ##

Run:

    python zmq_job_manager/broker.py 'tcp://*:2469' 'tcp://localhost:2468'


## Terminal 3 ##

Run:

    python zmq_job_manager/worker.py tcp://localhost:2469


# Expected output #

## Terminals 1 and 3 ##

Both terminals 1 and 3 should show output similar to the following (with
different times):

    ['[2013-03-26 18:49:07.056746] hello world']
    ['[2013-03-26 18:49:07.056807] hello world']
    ['[2013-03-26 18:49:07.056831] hello world']
    ['[2013-03-26 18:49:07.056848] hello world']
    ['[2013-03-26 18:49:07.056863] hello world']
