import utils
import click

@click.command()
@click.argument('csv_file', required=True, type=str)
@click.argument('parquet_file', required=True, type=str)
def main(csv_file, parquet_file):
    utils.csv_to_parquet(csv_file, parquet_file)

if __name__ == "__main__":
    main()



