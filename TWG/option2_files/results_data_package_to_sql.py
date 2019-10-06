import click
import io
import sqlalchemy
import sqlalchemy_utils
import utils
import tarfile
import pandas as pd

# Create denormalized reporting database


@click.command()
@click.argument('results_data_package', required=True, type=str)
@click.argument('connection_string', required=True, type=str)
def main(results_data_package, connection_string):

    if not sqlalchemy_utils.database_exists(connection_string):
        sqlalchemy_utils.create_database(connection_string)

    if not utils.check_results_package_file(results_data_package):
        print('Invalid results package file.')
        exit(1)

    engine = sqlalchemy.create_engine(connection_string)

    summary = utils.get_summary(results_data_package)

    with tarfile.open(results_data_package) as tar:
        for resource in summary['resources']:
            if resource['path'].startswith("outputs"):
                (_, perspective, summary_set, output_file) = str.split(
                    resource['path'], '/')
                output_type, ext = str.split(output_file, '.')
                if output_type != 'summary_info':
                    print('{}_{}_{}'.format(
                        output_type, summary_set, perspective))
                    summary_info_file = "outputs/{}/{}/summary_info.csv".format(
                        perspective, summary_set)
                    csv_contents = tar.extractfile(summary_info_file).read()
                    summary_info_df = pd.read_csv(
                        io.BytesIO(csv_contents), encoding='utf8')
                    csv_contents = tar.extractfile(resource['path']).read()
                    df = pd.read_csv(io.BytesIO(csv_contents), encoding='utf8')
                    df.merge(summary_info_df, on='summary_id')
                    df.to_sql('{}_{}_{}'.format(
                        output_type, summary_set, perspective), engine)


if __name__ == "__main__":
    main()
