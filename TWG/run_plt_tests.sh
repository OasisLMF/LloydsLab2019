#! /bin/bash

# Run postgres in Docker
docker run -d --name my_postgres -p 54320:5432 postgres:11

# Run performance tests
python test_plt_formats.py 1000 10 10 postgres://postgres@localhost:54320/test out.csv --num_summaries_per_summary_set 100 --do_header 
python test_plt_formats.py 10000 10 10 postgres://postgres@localhost:54320/test out.csv --num_summaries_per_summary_set 100
python test_plt_formats.py 100000 10 10 postgres://postgres@localhost:54320/test out.csv --num_summaries_per_summary_set 100
python test_plt_formats.py 100000 10 100 postgres://postgres@localhost:54320/test out.csv --num_summaries_per_summary_set 100