#!/usr/bin/python

import sys

# your code goes in here
def clean(line):
  line = line.replace('"','');
  line = line.replace(';',',');
  return line;

def build_book_rec_set(book_rec_vector_pairs):
  rec_set = set();
  for line in book_rec_vector_pairs:
    tokens = line.split(':');
    rec_set.add(int(tokens[0])); #isbn
  return rec_set;

def print_stats(ratings_file, users_file, books_file, count):
  f_ratings = open(ratings_file, 'r');
  f_users = open(users_file, 'r');
  f_books = open(books_file, 'r');

  user_rec_map = {};
  lines_read = 0
  for line in sys.stdin:
    lines_read += 1;
    if lines_read == count:
      break;
    (user_id, book_rec) = line.split('\t');
    book_rec = book_rec.replace('[','');
    book_rec = book_rec.replace(']','');
    book_rec_vector_pairs = book_rec.split(',');
    rec_set = build_book_rec_set(book_rec_vector_pairs);
    user_rec_map[user_id] = rec_set;

  isbn_title_map = {};
  for line in f_books:
    line = clean(line);
    tokens = line.split(',');
    try:
      isbn = int(tokens[0]);
    except:
      continue;
    title = tokens[1];
    isbn_title_map[isbn] = title;

  user_ratings_map = {};
  for line in f_ratings:
    line = clean(line);
    tokens = line.split(',');
    userid = tokens[0];
    try:
      isbn = int(tokens[1]);
    except:
     continue;
    rating = tokens[2];
    if not userid in user_rec_map:
      continue;
    ratings_set = None;
    if userid in user_ratings_map:
      ratings_set = user_ratings_map[userid];
    else:
      ratings_set = set()
    ratings_set.add(tuple([isbn, rating]))
    user_ratings_map[userid] = ratings_set;

  max_printed = 5
  for userid in user_rec_map.keys():
    print '==========';
    print 'user id = ', userid;
    print 'rated:';
    if userid in user_ratings_map:
      rated_set = user_ratings_map[userid];
      rated_count = 0;
      for t in rated_set:
        if t[0] in isbn_title_map:
          print isbn_title_map[t[0]], ' with: ', t[1];
          rated_count += 1;
          if rated_count == max_printed:
            break;
    print 'recommended:';
    recommended_set = user_rec_map[userid];
    for isbn in recommended_set:
      if isbn in isbn_title_map:
        print isbn_title_map[isbn];
      else:
        print 'book id : ', isbn
    print ''

if __name__ == "__main__":
  books_file = sys.argv[1];
  users_file = sys.argv[2];
  ratings_file = sys.argv[3];
  count = sys.argv[4];
  print 'ratings file = ', ratings_file;
  print 'users file = ', users_file;
  print 'books file = ', books_file;
  print 'count = ', count;
  print_stats(ratings_file, users_file, books_file, count);

