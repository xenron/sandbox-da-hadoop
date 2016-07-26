#!/bin/bash

cat BX-Users.csv | cut -d ';' -f1 | sed 's/"//g' > cleaned_book_users.txt
