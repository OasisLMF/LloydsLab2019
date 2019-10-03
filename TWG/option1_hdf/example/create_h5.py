import pandas as pd
import h5py

def add_csv_to_store(csv_file, name, store):
    df = pd.read_csv(csv_file)
    store.put(name, df, format='table', data_columns=True)

with pd.HDFStore(path='example_results_package.h5', mode='a') as hdf:

    add_csv_to_store(
        './oasis_output/output/gul_S1_aalcalc.csv', '/output/groundup_loss/all/aal', hdf)
    add_csv_to_store(
        './oasis_output/output/gul_S1_eltcalc.csv', 'results_package_example/output/groundup_loss/all/elt', hdf)
    add_csv_to_store(
        './oasis_output/output/gul_S1_leccalc_full_uncertainty_aep.csv', 'results_package_example/output/groundup_loss/all/aep_full_uncertainty', hdf)
    add_csv_to_store(
        './oasis_output/output/gul_S1_leccalc_full_uncertainty_oep.csv', 'results_package_example/output/groundup_loss/all/oep_full_uncertainty', hdf)
    add_csv_to_store(
        './oasis_output/output/gul_S1_pltcalc.csv', 'results_package_example/output/groundup_loss/all/plt', hdf)
    add_csv_to_store(
        './oasis_output/output/gul_S1_summary-info.csv', 'results_package_example/output/groundup_loss/all/summary_info', hdf)

    add_csv_to_store(
        './oasis_output/output/gul_S2_aalcalc.csv', 'results_package_example/output/groundup_loss/by_geography/aal', hdf)
    add_csv_to_store(
        './oasis_output/output/gul_S2_eltcalc.csv', 'results_package_example/output/groundup_loss/by_geography/elt', hdf)
    add_csv_to_store(
        './oasis_output/output/gul_S2_leccalc_full_uncertainty_aep.csv', 'results_package_example/output/groundup_loss/by_geography/aep_full_uncertainty', hdf)
    add_csv_to_store(
        './oasis_output/output/gul_S2_leccalc_full_uncertainty_oep.csv', 'results_package_example/output/groundup_loss/by_geography/oep_full_uncertainty', hdf)
    add_csv_to_store(
        './oasis_output/output/gul_S2_pltcalc.csv', 'results_package_example/output/groundup_loss/by_geography/plt', hdf)
    add_csv_to_store(
        './oasis_output/output/gul_S2_summary-info.csv', 'results_package_example/output/groundup_loss/by_geography/summary_info', hdf)

    add_csv_to_store(
        './oasis_output/output/il_S1_aalcalc.csv', 'results_package_example/output/insured_loss/all/aal', hdf)
    add_csv_to_store(
        './oasis_output/output/il_S1_eltcalc.csv', 'results_package_example/output/insured_loss/all/elt', hdf)
    add_csv_to_store(
        './oasis_output/output/il_S1_leccalc_full_uncertainty_aep.csv', 'results_package_example/output/insured_loss/all/aep_full_uncertainty', hdf)
    add_csv_to_store(
        './oasis_output/output/il_S1_leccalc_full_uncertainty_oep.csv', 'results_package_example/output/insured_loss/all/oep_full_uncertainty', hdf)
    add_csv_to_store(
        './oasis_output/output/il_S1_pltcalc.csv', 'results_package_example/output/insured_loss/all/plt', hdf)
    add_csv_to_store(
        './oasis_output/output/il_S1_summary-info.csv', 'results_package_example/output/insured_loss/all/summary_info', hdf)

    add_csv_to_store(
        './oasis_output/output/il_S2_aalcalc.csv', 'results_package_example/output/insured_loss/by_geography/aal', hdf)
    add_csv_to_store(
        './oasis_output/output/il_S2_eltcalc.csv', 'results_package_example/output/insured_loss/by_geography/elt', hdf)
    add_csv_to_store(
        './oasis_output/output/il_S2_leccalc_full_uncertainty_aep.csv', 'results_package_example/output/insured_loss/by_geography/aep_full_uncertainty', hdf)
    add_csv_to_store(
        './oasis_output/output/il_S2_leccalc_full_uncertainty_oep.csv', 'results_package_example/output/insured_loss/by_geography/oep_full_uncertainty', hdf)
    add_csv_to_store(
        './oasis_output/output/il_S2_pltcalc.csv', 'results_package_example/output/insured_loss/by_geography/plt', hdf)
    add_csv_to_store(
        './oasis_output/output/il_S2_summary-info.csv', 'results_package_example/output/insured_loss/by_geography/summary_info', hdf)

