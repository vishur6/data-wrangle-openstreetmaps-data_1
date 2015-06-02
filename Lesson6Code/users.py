#!/usr/bin/env python
# -*- coding: utf-8 -*-
import xml.etree.cElementTree as ET
import pprint
import re
"""
Your task is to explore the data a bit more.
The first task is a fun one - find out how many unique users
have contributed to the map in this particular area!

The function process_map should return a set of unique user IDs ("uid")
"""

def get_user(element):
    uid = None
    if element.attrib.has_key("uid"):
	#if the element's attribute has a key "uid", then return its corresponding value
        uid = element.attrib["uid"]
    return uid


def process_map(filename):
    users = set()
    for _, element in ET.iterparse(filename):
        uid = get_user(element)
        if uid != None:
			#add the user to set so that only unique users are present in the set
            users.add(uid)

    return users


def test():

    users = process_map('example.osm')
    pprint.pprint(users)
    assert len(users) == 6



if __name__ == "__main__":
    test()