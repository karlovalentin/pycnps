#!/usr/bin/python
import random
import time
from termcolor import colored
import csv
import sys
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.policies import DCAwareRoundRobinPolicy
from cassandra.policies import TokenAwarePolicy
from cassandra.policies import RetryPolicy
from cassandra.query import dict_factory
from random import randint
import MySQLdb

#recibimos el epoch
print sys.argv[0]
print sys.argv[1]