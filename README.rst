flexpart-ifs-preprocessor
========

Container application which pre-processes IFS data for use with `Flexpart <https://www.flexpart.eu/>`_, a Lagrangian particle dispersion model. The `flexprep <https://gitlab.phaidra.org/flexpart/flexprep>`_ library which itself relies on ECMWF's earthkit-data is used for the GRIB data processing.

Use at EWC
---------------

For detailed instructions on how flexpart-ifs-preprocessor is configured and deployed on the `European Weather Cloud (EWC) https://europeanweather.cloud/`_, and how it is automatically triggered by the dissemination of IFS forecasts through the `Aviso Notification System <https://confluence.ecmwf.int/display/EWCLOUDKB/Aviso+Notification+System+on+EWC>`_, please refer to the `orchestration repository <https://github.com/MeteoSwiss/flex-container-orchestrator>`_.
