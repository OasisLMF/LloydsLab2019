#! /bin/bash

#
# Run GEM model and create results package.
#

export cwd=$(pwd)

# Create GEM repo with Dominican Republic model data

rm -rf gem
git clone https://github.com/oasislmf/gem
cd gem
pip install oasislmf
make -C model_data/GMO/
cd $cwd

# Run an analysis

oasislmf model run -C gem/oasislmf.json -r oasis_output -a analysis_settings.json -x gem/tests/data/dom-rep-146-oed-location.csv

mkdir -p results_package_example/output/groundup_loss/all
mkdir -p results_package_example/output/groundup_loss/by_geography
mkdir -p results_package_example/output/insured_loss/all
mkdir -p results_package_example/output/insured_loss/by_geography

# Copy results into the results package directory structure

python create_h5.py