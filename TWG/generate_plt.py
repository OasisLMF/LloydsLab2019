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
from console_progressbar import ProgressBar


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


def create_data_package(
        root_directory,
        num_summary_sets, num_summaries_per_summary_set,
        do_gul, do_il,
        summary_file, plt_file):

    os.mkdir(root_directory)

    # TODO
    Path(os.path.join(root_directory, "analysis_settings.json")).touch()
    # TODO
    Path(os.path.join(root_directory, "results_package.json")).touch()

    outputs_directory = os.path.join(root_directory, "outputs")
    os.mkdir(outputs_directory)

    if do_gul:
        gul_directory = os.path.join(outputs_directory, "gul")
        os.mkdir(gul_directory)
        for summary_set_index in range(num_summary_sets):
            summary_set_directory = os.path.join(
                gul_directory, str(summary_set_index))
            os.mkdir(summary_set_directory)
            shutil.copyfile(
                summary_file,
                os.path.join(summary_set_directory, "summary_info.json"))
            shutil.copyfile(
                plt_file,
                os.path.join(summary_set_directory, "plt.csv"))

    if do_il:
        il_directory = os.path.join(outputs_directory, "il")
        os.mkdir(il_directory)
        for summary_set_index in range(num_summary_sets):
            summary_set_directory = os.path.join(
                il_directory, str(summary_set_index))
            os.mkdir(summary_set_directory)
            shutil.copyfile(
                summary_file,
                os.path.join(summary_set_directory, "summary_info.json"))
            shutil.copyfile(
                plt_file,
                os.path.join(summary_set_directory, "plt.csv"))


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


def query_aal_by_summary_csv(summary_file, plt_file, num_periods, num_samples):
    summary_df = dd.read_csv(summary_file)
    summary_df.set_index('summary_id')
    plt_df = dd.read_csv(plt_file)
    plt_df.set_index('summary_id')
    plt_df = plt_df.merge(summary_df, on='summary_id')
    plt_df.groupby('value').apply(
        lambda x: x['loss'].sum()/num_periods * num_samples).compute()


def query_aep_by_summary_csv(summary_file, plt_file, num_periods):
    summary_df = dd.read_csv(summary_file)
    summary_df.set_index('summary_id')
    plt_df = dd.read_csv(plt_file)
    plt_df.set_index('summary_id')
    plt_df = plt_df.merge(summary_df, on='summary_id')
    agg_plt_df = plt_df.groupby(['sample_id', 'period'])[
        'loss'].sum().to_frame("period_loss").reset_index()
    agg_plt_df.groupby('sample_id').apply(
        lambda x: x['period_loss'].quantile(200.0 / num_periods)).compute()


def query_aal_by_summary_gz_csv(summary_file, plt_file, num_periods, num_samples):
    summary_df = dd.read_csv(summary_file)
    summary_df.set_index('summary_id')
    plt_df = dd.read_csv(plt_file, compression='gzip')
    plt_df.set_index('summary_id')
    plt_df = plt_df.merge(summary_df, on='summary_id')
    plt_df.groupby('value').apply(
        lambda x: x['loss'].sum()/num_periods * num_samples).compute()


def query_aep_by_summary_gz_csv(summary_file, plt_file, num_periods):
    summary_df = dd.read_csv(summary_file)
    summary_df.set_index('summary_id')
    plt_df = dd.read_csv(plt_file, compression='gzip')
    plt_df.set_index('summary_id')
    plt_df = plt_df.merge(summary_df, on='summary_id')
    agg_plt_df = plt_df.groupby(['sample_id', 'period'])[
        'loss'].sum().to_frame("period_loss").reset_index()
    agg_plt_df.groupby('sample_id').apply(
        lambda x: x['period_loss'].quantile(200.0 / num_periods)).compute()


def query_aal_by_summary_parquet(summary_file, plt_file, num_periods, num_samples):
    summary_df = dd.read_csv(summary_file)
    summary_df.set_index('summary_id')
    plt_df = dd.read_parquet(plt_file)
    plt_df.set_index('summary_id')
    plt_df = plt_df.merge(summary_df, on='summary_id')
    plt_df.groupby('value').apply(
        lambda x: x['loss'].sum()/num_periods * num_samples).compute()


def query_aep_by_summary_parquet(summary_file, plt_file, num_periods):
    summary_df = dd.read_csv(summary_file)
    summary_df.set_index('summary_id')
    plt_df = dd.read_parquet(plt_file)
    plt_df.set_index('summary_id')
    plt_df = plt_df.merge(summary_df, on='summary_id')
    agg_plt_df = plt_df.groupby(['sample_id', 'period'])[
        'loss'].sum().to_frame("period_loss").reset_index()
    agg_plt_df.groupby('sample_id').apply(
        lambda x: x['period_loss'].quantile(200.0 / num_periods)).compute()


@click.command()
@click.argument('num_periods', required=True, type=int)
@click.argument('event_rate', required=True, type=int)
@click.argument('num_samples', required=True, type=int)
@click.argument('output_dir', type=click.Path(writable=True))
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
        prob_of_loss, loss_alpha, loss_beta, loss_max,
        do_gul=True, do_il=True):

    do_benchmark_query_times = True
    do_create_data_package = True

    # Create the output package structure
    #
    # |
    # |- results_package.json
    # |- analysis_settings.json
    # |
    # |- results
    #       |
    #       |- GUL
    #       |   |
    #       |   |- S1
    #       |   |  |
    #       |   |  |- summary_info.json
    #       |   |  |- plt.csv
    #       |
    #       |   |- S2
    #       |   |  |- summary_info.json
    #       |   |  |- plt.csv
    #       |
    #       |- IL
    #       |   |
    #       |   |- S1
    #       |   |  |
    #       |   |  |- summary_info.json
    #       |   |  |- plt.csv
    #       |   |
    #       |   |- S2
    #       |   |  |- summary_info.json
    #       |   |  |- plt.csv
    #

    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)

    with tempfile.TemporaryDirectory() as temp_dir:

        # temp_dir = '/tmp'

        # Create CSV files
        summary_file = os.path.join(temp_dir, 'summary_info.csv')
        write_summary_info(
            num_summaries_per_summary_set,
            summary_file)
        plt_csv_file = os.path.join(temp_dir, 'plt.csv')
        write_plt_csv(
            event_rate, num_periods, num_samples, prob_of_loss,
            num_summaries_per_summary_set, loss_alpha, loss_beta, loss_max,
            output_file=plt_csv_file)

        # Create gzipped CSV PLT file
        plt_csv_gz_file = os.path.join(temp_dir, 'plt.csv.gz')
        csv_to_gz(
            plt_csv_file,
            plt_csv_gz_file
        )

        # Create parquet PLT file
        plt_parquet_file = os.path.join(temp_dir, 'plt.parquet')
        csv_to_parquet(
            plt_csv_file,
            plt_parquet_file
        )

        if do_benchmark_query_times:

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

        print("CSV size: {}".format(get_readable_filezize(plt_csv_file)))
        if do_benchmark_query_times:
            print("CSV AAL query time: {}s".format(round(aal_csv_time, 1)))
            print("CSV AEP query time: {}s".format(round(aep_csv_time, 1)))

        print("CSV gzip size: {}".format(
            get_readable_filezize(plt_csv_gz_file)))
        if do_benchmark_query_times:
            print("CSV gzip AAL query time: {}s".format(
                round(aal_gz_csv_time, 1)))
            print("CSV gzip AEP query time: {}s".format(
                round(aep_gz_csv_time, 1)))

        print("Parquet size: {}".format(get_readable_filezize(plt_parquet_file)))
        if do_benchmark_query_times:
            print("Parquet AAL query time: {}s".format(
                round(aal_parquet_time, 1)))
            print("Parquet AEP query time: {}s".format(
                round(aep_parquet_time, 1)))

        if do_create_data_package:
            create_data_package(
                output_dir,
                num_summary_sets, num_summaries_per_summary_set,
                do_gul, do_il,
                summary_file, plt_csv_file)


if __name__ == "__main__":
    main()
