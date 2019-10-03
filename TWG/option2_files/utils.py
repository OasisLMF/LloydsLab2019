import boto3
import click
import csv
import os
import random
import sys
import shutil
import tempfile
import humanize
import subprocess
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import time
import gc
import dask.dataframe as dd
from pathlib import Path
from scipy.stats import beta, poisson

# Shared utils

def get_readable_filezize(file):
    return humanize.naturalsize(os.path.getsize(file))


def csv_to_gz(csv_file, gz_file):
    subprocess.check_call(['gzip', '-k', csv_file])


def csv_to_parquet(csv_file, parquet_file):
    chunksize = 100000
    csv_stream = pd.read_csv(csv_file, chunksize=chunksize, low_memory=False)
    for i, chunk in enumerate(csv_stream):
        if i == 0:
            # Guess the schema of the CSV file from the first chunk
            parquet_schema = pa.Table.from_pandas(df=chunk).schema
            # Open a Parquet file for writing
            parquet_writer = pq.ParquetWriter(
                parquet_file, parquet_schema, compression='snappy')
        # Write CSV chunk to the parquet file
        table = pa.Table.from_pandas(chunk, schema=parquet_schema)
        parquet_writer.write_table(table)
    parquet_writer.close()


def write_summary_info(num_summaries_per_summary_set, summary_file):
    with open(summary_file, 'w') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(['summary_id', 'value'])
        for summary_index in range(num_summaries_per_summary_set):
            csvwriter.writerow(
                [summary_index, 'attribute_{}'.format(summary_index)])


def write_plt_csv(
        event_rate, num_periods, num_samples, prob_of_loss,
        num_summaries_per_summary_set,
        loss_alpha, loss_beta, loss_max,
        output_file):

    with open(output_file, 'w') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(
            ['period', 'event_id', 'summary_id', 'sample_id', 'loss'])
        for period in range(0, num_periods):
            if period % 10000 == 0:
                events_per_period = poisson.rvs(event_rate, size=10000)
            for event_id in range(0, events_per_period[period % 10000]):
                event_losses = beta.rvs(
                    loss_alpha, loss_beta, size=num_summaries_per_summary_set * num_samples)
                for summary_id in range(0, num_summaries_per_summary_set):
                    if random.uniform(0, 1) < prob_of_loss:
                        for sample_id in range(0, num_samples):
                            loss = event_losses[summary_id *
                                                num_samples + sample_id] * loss_max
                            csvwriter.writerow(
                                [period, event_id, summary_id, sample_id, loss])
