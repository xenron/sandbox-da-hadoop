#!/bin/bash

hive -v --hivevar threshold=10 -i 03_using_hive_noninteractively_init.hql -f 03_using_hive_noninteractively_script.hql

hive -S -e "use ch3; describe top_athletes;" | cut -f 1 | paste -s - > output.tsv

hive -S -e "use ch3; select * from top_athletes ;" >> output.tsv

cat output.tsv
