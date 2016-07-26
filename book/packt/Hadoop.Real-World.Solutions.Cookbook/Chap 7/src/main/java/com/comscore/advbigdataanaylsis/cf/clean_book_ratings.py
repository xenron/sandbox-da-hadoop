#!/usr/bin/python

import sys

def rewrite(ratings_file, csv_file):
  f_writer = open(csv_file, 'w');
  f_reader = open(ratings_file, 'r');
  bfirst = False;
  for line in f_reader:
    if bfirst == False:
      bfirst = True;
      continue;
    line = line.replace('"','');
    line = line.replace(';',',');
    line = line.replace(' ','');
    line = line.replace('+','');
    if '/' in line:
      continue;
    #number check
    tokens = line.split(',');
    try:
      int(tokens[1]);
    except:
      continue;
    f_writer.write(line);

if __name__ == "__main__":
  ratings_file = sys.argv[1];
  csv_file = sys.argv[2];
  print 'ratings_file = ', ratings_file;
  print 'csv file = ', csv_file;
  rewrite(ratings_file, csv_file);
