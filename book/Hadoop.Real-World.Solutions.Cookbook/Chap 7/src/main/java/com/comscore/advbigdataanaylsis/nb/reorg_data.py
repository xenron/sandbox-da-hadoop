#!/usr/bin/python

import sys
import glob
import random
import os
import shutil

def reorg(base_input_dir, train_output_dir, test_output_dir):
  if os.path.exists(train_output_dir):
    shutil.rmtree(train_output_dir);
  if os.path.exists(test_output_dir):
    shutil.rmtree(test_output_dir);
  os.mkdir(train_output_dir);
  os.mkdir(test_output_dir);
  os.mkdir(train_output_dir + '/pos/');
  os.mkdir(train_output_dir + '/neg/');
  os.mkdir(test_output_dir + '/pos/');
  os.mkdir(test_output_dir + '/neg/');

  pos_files = glob.glob(base_input_dir + '/pos/*');
  random.shuffle(pos_files);
  train_pos_files = pos_files[0:(int)(len(pos_files)*.80)];
  test_pos_files = pos_files[(int)(len(pos_files)*.80):];
  for f in train_pos_files:
    shutil.copy(f, train_output_dir + '/pos/');
  for f in test_pos_files:
    shutil.copy(f, test_output_dir + '/pos/');

  neg_files = glob.glob(base_input_dir + '/neg/*');
  random.shuffle(neg_files);
  train_neg_files = neg_files[0:(int)(len(neg_files)*.80)];
  test_neg_files = neg_files[(int)(len(neg_files)*.80):];
  for f in train_neg_files:
    shutil.copy(f, train_output_dir + '/neg/');
  for f in test_neg_files:
    shutil.copy(f, test_output_dir + '/neg/');

if __name__ == "__main__":
  base_input_dir = sys.argv[1];
  train_output_dir = sys.argv[2];
  test_output_dir = sys.argv[3];
  print 'base = ', base_input_dir;
