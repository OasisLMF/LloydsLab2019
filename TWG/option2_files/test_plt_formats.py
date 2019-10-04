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
    # Dask version
    # aal_df = plt_df.groupby('value').apply(
    #     lambda x: x['loss'].sum()/num_periods * num_samples).compute()
    aal_df = plt_df.groupby('value').apply(
        lambda x: x['loss'].sum()/num_periods * num_samples)


def query_aep_by_summary(summary_df, plt_df, num_periods):
    summary_df.set_index('summary_id')
    plt_df.set_index('summary_id')
    plt_df = plt_df.merge(summary_df, on='summary_id')
    agg_plt_df = plt_df.groupby(['sample_id', 'period'])[
        'loss'].sum().to_frame("period_loss").reset_index()
    # Dask version
    # agg_plt_df.groupby('sample_id').apply(
    #     lambda x: x['period_loss'].quantile(min(200.0 / num_periods, 1.0))).compute()
    agg_plt_df.groupby('sample_id').apply(
        lambda x: x['period_loss'].quantile(min(200.0 / num_periods, 1.0)))


def load_csv(summary_file, plt_file):
    summary_df = pd.read_csv(summary_file)
    plt_df = pd.read_csv(plt_file)
    return (summary_df, plt_df)


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


def load_gz_csv(summary_file, plt_file):
    summary_df = pd.read_csv(summary_file)
    plt_df = pd.read_csv(plt_file, compression='gzip')
    return (summary_df, plt_df)


def load_parquet(summary_file, plt_file):
    summary_df = pd.read_csv(summary_file)
    plt_df = pd.read_parquet(plt_file)
    return (summary_df, plt_df)


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

        # # Insert into database
        # gc.collect()

        # start = time.time()

        # if sqlalchemy_utils.database_exists(conn_string):
        #     sqlalchemy_utils.drop_database(conn_string)
        # sqlalchemy_utils.create_database(conn_string)
        # engine = sqlalchemy.create_engine(conn_string)
        # plt_df = pd.read_csv(plt_csv_file)
        # summary_info_df = pd.read_csv(summary_file)
        # plt_df = plt_df.merge(summary_info_df, on='summary_id')
        # plt_df.to_sql('plt', engine)

        # end = time.time()
        # sql_load_time = end - start

        gc.collect()

        start = time.time()
        (summary_df, plt_df) = load_csv(summary_file, plt_csv_file)
        end = time.time()
        csv_load_time = end - start

        gc.collect()

        start = time.time()
        plt_df.to_csv(os.path.join(temp_dir, 'temp_plt.csv'), index=False)
        end = time.time()
        csv_write_time = end - start

        gc.collect()

        start = time.time()
        (summary_df, plt_df) = load_gz_csv(summary_file, plt_csv_gz_file)
        end = time.time()
        csv_gz_load_time = end - start

        gc.collect()

        start = time.time()
        plt_df.to_csv(os.path.join(temp_dir, 'temp_plt.csv'), compression='gzip', index=False)

        end = time.time()
        csv_gz_write_time = end - start

        gc.collect()

        start = time.time()
        (summary_df, plt_df) = load_parquet(summary_file, plt_parquet_file)
        end = time.time()
        parquet_load_time = end - start

        gc.collect()

        start = time.time()
        plt_df.to_parquet(os.path.join(temp_dir, 'temp_plt.parquet'), index=False)

        end = time.time()
        parquet_write_time = end - start

        gc.collect()

        # start = time.time()
        # query_aal_by_summary(
        #     summary_df, plt_df, num_periods, num_samples)
        # end = time.time()
        # aal_pd_time = end - start

        # gc.collect()

        # start = time.time()
        # query_aep_by_summary(summary_df, plt_df, num_periods)
        # end = time.time()
        # aep_pd_time = end - start

        # gc.collect()

        # start = time.time()
        # query_aal_by_summary_sql(
        #     engine, num_periods, num_samples)
        # end = time.time()
        # aal_sql_time = end - start

        # gc.collect()

        # start = time.time()
        # query_aep_by_summary_sql(engine, num_periods)
        # end = time.time()
        # aep_sql_time = end - start

        # gc.collect()

        mode = 'a'
        if do_header:
            mode = 'w'
        with open(output_file, mode) as f:
            if do_header:
                f.writelines(
                    "num_periods,event_rate,num_samples,csv_size,csv_gz_size,parquet_size," +
                    "csv_write_time,csv_load_time,csv_gz_write_time,csv_gz_load_time,parquet_write_time,parquet_load_time" +
#                    "sql_load_time," +
#                    "aal_pd_time,aep_pd_time,aal_sql_time,aep_sql_time"+
                    "\n")
            f.writelines((
                "{},{},{},{},{},{}," +
                "{:.2f},{:.2f},{:.2f},{:.2f},{:.2f},{:.2f}," +
                # "{:.2f}," +
                # "{:.2f},{:.2f},{:.2f},{:.2f}" +
                "\n").format(
                num_periods, event_rate, num_samples, os.path.getsize(
                    plt_csv_file), os.path.getsize(plt_csv_gz_file), os.path.getsize(plt_parquet_file),
                csv_write_time, csv_load_time, csv_gz_write_time, csv_gz_load_time, parquet_write_time, parquet_load_time
#                sql_load_time,
#                aal_pd_time, aep_pd_time, aal_sql_time, aep_sql_time
            ))


if __name__ == "__main__":
    main()
