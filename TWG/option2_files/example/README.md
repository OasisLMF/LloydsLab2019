# Example

Creates an example results data package, based on running a GEM Dominican Republic example model in Oasis.

To generate the example run the following command. This assumes that Python 3 and pip are installed.

```
./create_example.sh
```

An example results package will be created as example_results_package.tar with the following structure:

```
.
├── analysis_settings.json
├── exposure
│   ├── dom-rep-146-oed-account.csv
│   └── dom-rep-146-oed-location.csv
├── output
│   ├── groundup_loss
│   │   ├── all
│   │   │   ├── aal.csv
│   │   │   ├── aep_full_uncertainty.csv
│   │   │   ├── elt.csv
│   │   │   ├── oep_full_uncertainty.csv
│   │   │   ├── plt.csv
│   │   │   └── summary_info.csv
│   │   └── by_geography
│   │       ├── aal.csv
│   │       ├── aep_full_uncertainty.csv
│   │       ├── elt.csv
│   │       ├── oep_full_uncertainty.csv
│   │       ├── plt.csv
│   │       └── summary_info.csv
│   └── insured_loss
│       ├── all
│       │   ├── aal.csv
│       │   ├── aep_full_uncertainty.csv
│       │   ├── elt.csv
│       │   ├── oep_full_uncertainty.csv
│       │   ├── plt.csv
│       │   └── summary_info.csv
│       └── by_geography
│           ├── aal.csv
│           ├── aep_full_uncertainty.csv
│           ├── elt.csv
│           ├── oep_full_uncertainty.csv
│           ├── plt.csv
│           └── summary_info.csv
└── results_package.json

```
