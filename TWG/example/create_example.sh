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

cp ./oasis_output/output/gul_S1_aalcalc.csv results_package_example/output/groundup_loss/all/aal.csv
cp ./oasis_output/output/gul_S1_eltcalc.csv results_package_example/output/groundup_loss/all/elt.csv
cp ./oasis_output/output/gul_S1_leccalc_full_uncertainty_aep.csv results_package_example/output/groundup_loss/all/aep_full_uncertainty.csv
cp ./oasis_output/output/gul_S1_leccalc_full_uncertainty_oep.csv results_package_example/output/groundup_loss/all/oep_full_uncertainty.csv
cp ./oasis_output/output/gul_S1_pltcalc.csv results_package_example/output/groundup_loss/all/plt.csv
cp ./oasis_output/output/gul_S1_summary-info.csv results_package_example/output/groundup_loss/all/summary_info.csv

cp ./oasis_output/output/gul_S2_aalcalc.csv results_package_example/output/groundup_loss/by_geography/aal.csv
cp ./oasis_output/output/gul_S2_eltcalc.csv results_package_example/output/groundup_loss/by_geography/elt.csv
cp ./oasis_output/output/gul_S2_leccalc_full_uncertainty_aep.csv results_package_example/output/groundup_loss/by_geography/aep_full_uncertainty.csv
cp ./oasis_output/output/gul_S2_leccalc_full_uncertainty_oep.csv results_package_example/output/groundup_loss/by_geography/oep_full_uncertainty.csv
cp ./oasis_output/output/gul_S2_pltcalc.csv results_package_example/output/groundup_loss/by_geography/plt.csv
cp ./oasis_output/output/gul_S2_summary-info.csv results_package_example/output/groundup_loss/by_geography/summary_info.csv

cp ./oasis_output/output/il_S1_aalcalc.csv results_package_example/output/insured_loss/all/aal.csv
cp ./oasis_output/output/il_S1_eltcalc.csv results_package_example/output/insured_loss/all/elt.csv
cp ./oasis_output/output/il_S1_leccalc_full_uncertainty_aep.csv results_package_example/output/insured_loss/all/aep_full_uncertainty.csv
cp ./oasis_output/output/il_S1_leccalc_full_uncertainty_oep.csv results_package_example/output/insured_loss/all/oep_full_uncertainty.csv
cp ./oasis_output/output/il_S1_pltcalc.csv results_package_example/output/insured_loss/all/plt.csv
cp ./oasis_output/output/il_S1_summary-info.csv results_package_example/output/insured_loss/all/summary_info.csv

cp ./oasis_output/output/il_S2_aalcalc.csv results_package_example/output/insured_loss/by_geography/aal.csv
cp ./oasis_output/output/il_S2_eltcalc.csv results_package_example/output/insured_loss/by_geography/elt.csv
cp ./oasis_output/output/il_S2_leccalc_full_uncertainty_aep.csv results_package_example/output/insured_loss/by_geography/aep_full_uncertainty.csv
cp ./oasis_output/output/il_S2_leccalc_full_uncertainty_oep.csv results_package_example/output/insured_loss/by_geography/oep_full_uncertainty.csv
cp ./oasis_output/output/il_S2_pltcalc.csv results_package_example/output/insured_loss/by_geography/plt.csv
cp ./oasis_output/output/il_S2_summary-info.csv results_package_example/output/insured_loss/by_geography/summary_info.csv

# Create the results package archive

cd results_package_example 
tar cvf ../results_package_example.tar *
