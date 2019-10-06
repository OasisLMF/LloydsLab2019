import click
import os
import shutil
import tempfile
import utils
from pathlib import Path

#
# Utility to generate a reference results package structure.
#


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

        summary_file = os.path.join(temp_dir, 'summary_info.csv')
        utils.write_summary_info(
            num_summaries_per_summary_set,
            summary_file)
        plt_csv_file = os.path.join(temp_dir, 'plt.csv')
        utils.write_plt_csv(
            event_rate, num_periods, num_samples, prob_of_loss,
            num_summaries_per_summary_set, loss_alpha, loss_beta, loss_max,
            output_file=plt_csv_file)

        create_data_package(
            output_dir,
            num_summary_sets, num_summaries_per_summary_set,
            do_gul, do_il,
            summary_file, plt_csv_file)


if __name__ == "__main__":
    main()
