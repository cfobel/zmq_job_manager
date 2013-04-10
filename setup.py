#!/usr/bin/env python

from distutils.core import setup

setup(name = 'zmq_job_manager',
    version = '0.0.1',
    description = 'ZeroMQ job manager',
    keywords = 'zmq job manager cluster',
    author = 'Christian Fobel',
    url = 'https://github.com/cfobel/zmq_job_manager',
    license = 'GPL',
    long_description = '''''',
    packages = ['zmq_job_manager'],
    scripts = ['scripts/launch_worker']
)
