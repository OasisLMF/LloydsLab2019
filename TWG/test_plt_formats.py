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
import utils
import gc
import dask.dataframe as dd
from pathlib import Path
from scipy.stats import beta, poisson
from console_progressbar import u

#
# Utility to benchmark size and query perfomance of
# different data formats.
#

def query_aal_by_summary(summary_df, plt_df, num_periods, num_samples):
    summary_df.set_index('summary_id')
    plt_df.set_index('summary_id')
    plt_df = plt_df.merge(summary_df, on='summary_id')
    plt_df.groupby('value').apply(
        lambda x: x['loss'].sum()/num_periods * num_samples).compute()


def query_aal_by_summary_csv(summary_file, plt_file, num_periods, num_samples):
    summary_df = dd.read_csv(summary_file)
    plt_df = dd.read_csv(plt_file)
    query_aal_by_summary(summary_file, plt_file, num_periods, num_samples)


def query_aep_by_summary_csv(summary_file, plt_file, num_periods):
    summary_df = dd.read_csv(summary_file)
    plt_df = dd.read_csv(plt_file)
    query_aal_by_summary(summary_file, plt_file, num_periods, num_samples)


def query_aal_by_summary_gz_csv(summary_file, plt_file, num_periods, num_samples):
    summary_df = dd.read_csv(summary_file)
    plt_df = dd.read_csv(plt_file, compression='gzip')
    query_aal_by_summary(summary_file, plt_file, num_periods, num_samples)


def query_aep_by_summary_gz_csv(summary_df, plt_df, num_periods):
    summary_df.set_index('summary_id')
    plt_df.set_index('summary_id')
    plt_df = plt_df.merge(summary_df, on='summary_id')
    agg_plt_df = plt_df.groupby(['sample_id', 'period'])[
        'loss'].sum().to_frame("period_loss").reset_index()
    agg_plt_df.groupby('sample_id').apply(
        lambda x: x['period_loss'].quantile(200.0 / num_periods)).compute()

def query_aep_by_summary_gz_csv(summary_file, plt_file, num_periods):
    summary_df = dd.read_csv(summary_file)
    plt_df = dd.read_csv(plt_file, compression='gzip')
    query_aep_by_summary_gz_csv(summary_df, plt_df, num_periods)

def query_aal_by_summary_parquet(summary_file, plt_file, num_periods, num_samples):
    summary_df = dd.read_csv(summary_file)
    plt_df = dd.read_parquet(plt_file)
    query_aep_by_summary_gz_csv(summary_df, plt_df, num_periods)

def query_aep_by_summary_parquet(summary_file, plt_file, num_periods):
    summary_df = dd.read_csv(summary_file)
    plt_df = dd.read_parquet(plt_file)
    query_aep_by_summary_gz_csv(summary_df, plt_df, num_periods)

@click.command()
@click.argument('num_periods', required=True, type=int)
@click.argument('event_rate', required=True, type=int)
@click.argument('num_samples', required=True, type=int)
@click.option('--num_summary_sets', type=int, default=1)
@click.option('--num_summaries_per_summary_set', type=int, default=10)
@click.option('--prob_of_loss', type=float, default=0.1)
@click.option('--loss_alpha', type=float, default=1.0)
@click.option('--loss_beta', type=float, default=10.0)
@click.option('--loss_max', type=float, default=100000.0)
def main(
        num_periods, event_rate,
        num_samples, output_dir,
        num_summary_sets, num_summaries_per_summary_set,
        prob_of_loss, loss_alpha, loss_beta, loss_max):

    with tempfile.TemporaryDirectory() as temp_dir:

        # Create CSV files
        summary_file = os.path.join(temp_dir, 'summary_info.csv')
        utils.write_summary_info(
            num_summaries_per_summary_set,
            summary_file)
        plt_csv_file = os.path.join(temp_dir, 'plt.csv')
        utils.write_plt_csv(
            event_rate, num_periods, num_samples, prob_of_loss,
            num_summaries_per_summary_set, loss_alpha, loss_beta, loss_max,
            output_file=plt_csv_file)

        # Create gzipped CSV PLT file
        plt_csv_gz_file = os.path.join(temp_dir, 'plt.csv.gz')
        utils.csv_to_gz(
            plt_csv_file,
            plt_csv_gz_file
        )

        # Create parquet PLT file
        plt_parquet_file = os.path.join(temp_dir, 'plt.parquet')
        utils.csv_to_parquet(
            plt_csv_file,
            plt_parquet_file
        )

        gc.collect()

        start = time.time()
        query_aal_by_summary_csv(
            summary_file, plt_csv_file, num_periods, num_samples)
        end = time.time()
        aal_csv_time = end - start

        gc.collect()

        start = time.time()
        query_aep_by_summary_csv(summary_file, plt_csv_file, num_periods)
        end = time.time()
        aep_csv_time = end - start

        gc.collect()

        start = time.time()
        query_aal_by_summary_gz_csv(
            summary_file, plt_csv_gz_file, num_periods, num_samples)
        end = time.time()
        aal_gz_csv_time = end - start

        gc.collect()

        start = time.time()
        query_aep_by_summary_gz_csv(
            summary_file, plt_csv_gz_file, num_periods)
        end = time.time()
        aep_gz_csv_time = end - start

        gc.collect()

        start = time.time()
        query_aal_by_summary_parquet(
            summary_file, plt_parquet_file, num_periods, num_samples)
        end = time.time()
        aal_parquet_time = end - start

        gc.collect()

        start = time.time()
        query_aep_by_summary_parquet(
            summary_file, plt_parquet_file, num_periods)
        end = time.time()
        aep_parquet_time = end - start

        gc.collect()

        print("CSV size: {}".format(utils.get_readable_filezize(plt_csv_file)))
        print("CSV AAL query time: {}s".format(round(aal_csv_time, 1)))
        print("CSV AEP query time: {}s".format(round(aep_csv_time, 1)))

        print("CSV gzip size: {}".format(utils.get_readable_filezize(plt_csv_gz_file)))
        print("CSV gzip AAL query time: {}s".format(round(aal_gz_csv_time, 1)))
        print("CSV gzip AEP query time: {}s".format(round(aep_gz_csv_time, 1)))

        print("Parquet size: {}".format(utils.get_readable_filezize(plt_parquet_file)))
        print("Parquet AAL query time: {}s".format(round(aal_parquet_time, 1)))
        print("Parquet AEP query time: {}s".format(round(aep_parquet_time, 1)))

if __name__ == "__main__":
    main()
