#!/usr/bin/python

@outputSchema("hits:long")
def calculate(inputBag):
  hits = len(inputBag)
  return hits


  