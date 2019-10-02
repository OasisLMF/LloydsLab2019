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
import sqlalchemy
import sqlalchemy_utils

#
# Utility to benchmark size and query perfomance of
# different data formats.
#


def query_aal_by_summary(summary_df, plt_df, num_periods, num_samples):
    summary_df.set_index('summary_id')
    plt_df.set_index('summary_id')
    plt_df = plt_df.merge(summary_df, on='summary_id')
    aal_df = plt_df.groupby('value').apply(
        lambda x: x['loss'].sum()/num_periods * num_samples).compute()


def query_aep_by_summary(summary_df, plt_df, num_periods):
    summary_df.set_index('summary_id')
    plt_df.set_index('summary_id')
    plt_df = plt_df.merge(summary_df, on='summary_id')
    agg_plt_df = plt_df.groupby(['sample_id', 'period'])[
        'loss'].sum().to_frame("period_loss").reset_index()
    agg_plt_df.groupby('sample_id').apply(
        lambda x: x['period_loss'].quantile(min(200.0 / num_periods, 1.0))).compute()


def query_aal_by_summary_csv(summary_file, plt_file, num_periods, num_samples):
    summary_df = dd.read_csv(summary_file)
    plt_df = dd.read_csv(plt_file)
    query_aal_by_summary(summary_df, plt_df, num_periods, num_samples)


def query_aep_by_summary_csv(summary_file, plt_file, num_periods):
    summary_df = dd.read_csv(summary_file)
    plt_df = dd.read_csv(plt_file)
    query_aep_by_summary(summary_df, plt_df, num_periods)


def query_aal_by_summary_sql(engine, num_periods, num_samples):
    rs = engine.execute(""" 
        SELECT    
         summary_id, 
         SUM(loss) / {}
        FROM
         plt
        GROUP BY
         summary_id
    """.format(num_periods * num_samples))


def query_aep_by_summary_sql(engine, num_periods):
    rs = engine.execute(""" 
        SELECT
         summary_id,
         percentile_cont({}) WITHIN GROUP (ORDER BY loss ASC)
        FROM
        (
         SELECT 
          summary_id,
          period,
          SUM(loss) AS loss
         FROM 
          plt 
         GROUP BY
          summary_id,
          period
        ) AS aplt
        GROUP BY
         summary_id
    """.format(min(200.0 / num_periods, 1.0)))


def query_aal_by_summary_gz_csv(summary_file, plt_file, num_periods, num_samples):
    summary_df = dd.read_csv(summary_file)
    plt_df = dd.read_csv(plt_file, compression='gzip')
    query_aal_by_summary(summary_df, plt_df, num_periods, num_samples)


def query_aep_by_summary_gz_csv(summary_file, plt_file, num_periods):
    summary_df = dd.read_csv(summary_file)
    plt_df = dd.read_csv(plt_file, compression='gzip')
    query_aep_by_summary(summary_df, plt_df, num_periods)


def query_aal_by_summary_parquet(summary_file, plt_file, num_periods, num_samples):
    summary_df = dd.read_csv(summary_file)
    plt_df = dd.read_parquet(plt_file)
    query_aal_by_summary(summary_df, plt_df, num_periods, num_samples)


def query_aep_by_summary_parquet(summary_file, plt_file, num_periods):
    summary_df = dd.read_csv(summary_file)
    plt_df = dd.read_parquet(plt_file)
    query_aep_by_summary(summary_df, plt_df, num_periods)


@click.command()
@click.argument('num_periods', required=True, type=int)
@click.argument('event_rate', required=True, type=int)
@click.argument('num_samples', required=True, type=int)
@click.argument('conn_string', required=True, type=str)
@click.argument('output_file', required=True, type=str)
@click.option('--num_summaries_per_summary_set', type=int, default=10)
@click.option('--prob_of_loss', type=float, default=0.1)
@click.option('--loss_alpha', type=float, default=1.0)
@click.option('--loss_beta', type=float, default=10.0)
@click.option('--loss_max', type=float, default=100000.0)
@click.option('--do_header', is_flag=True)
def main(
        num_periods, event_rate,
        num_samples, conn_string, output_file,
        num_summaries_per_summary_set,
        prob_of_loss, loss_alpha, loss_beta, loss_max,
        do_header):

    with tempfile.TemporaryDirectory() as temp_dir:

        # Uncomment to persist files for debug
        # temp_dir = '/tmp'

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

        # Insert into database
        if sqlalchemy_utils.database_exists(conn_string):
            sqlalchemy_utils.drop_database(conn_string)
        sqlalchemy_utils.create_database(conn_string)
        engine = sqlalchemy.create_engine(conn_string)
        plt_df = pd.read_csv(plt_csv_file)
        summary_info_df = pd.read_csv(summary_file)        
        plt_df = plt_df.merge(summary_info_df, on='summary_id')
        plt_df.to_sql('plt', engine)

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
        query_aal_by_summary_sql(
            engine, num_periods, num_samples)
        end = time.time()
        aal_sql_time = end - start

        gc.collect()

        start = time.time()
        query_aep_by_summary_sql(engine, num_periods)
        end = time.time()
        aep_sql_time = end - start

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

        mode = 'w'
        if do_header:
            mode = 'a'
        with open(output_file, mode) as f:
            if do_header:
                f.writelines("num_periods,event_rate,num_samples,csv_size,csv_gz_size,parquet_size,csv_aal_time,csv_aep_time,csv_gz_aal_time,csv_gz_aep_time,parquet_aal_time,parquet_aep_time,sql_aal_time,sql_aep_time\n")
            
            f.writelines("{},{},{},{},{},{},{},{},{},{},{},{},{},{}\n".format(
                num_periods, event_rate, num_samples,
                os.path.getsize(plt_csv_file), os.path.getsize(plt_csv_gz_file), os.path.getsize(plt_parquet_file),
                round(aal_csv_time, 2), round(aep_csv_time, 2),
                round(aal_gz_csv_time, 2), round(aep_gz_csv_time, 2),
                round(aal_parquet_time, 2), round(aep_parquet_time, 2),
                round(aal_sql_time, 2), round(aep_sql_time, 2)))

if __name__ == "__main__":
    main()
