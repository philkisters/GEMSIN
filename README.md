# Imputation Test Suite

This repository focuses on exploring multivariate time series imputation, with plans to extend into geospatial imputation in the future.

Primarily, it serves as a backup for local development, but it may also provide a useful starting point for others interested in this topic. For a more comprehensive introduction, consider visiting [Awesome Imputation](https://github.com/WenjieDu/Awesome_Imputation).

The goal is to build upon [PyPOTS](https://github.com/WenjieDu/PyPOTS) and evaluate various imputation methods using official [DWD (German Weather Service) datasets](https://cdc.dwd.de/portal/), as well as incorporating additional sensor data from crowdsourced networks such as [Netatmo](https://weathermap.netatmo.com/) and [Sensor.Community](https://sensor.community).

## Data Storage

Currently, the data is cached in my *ciweda* database ([ciweda repository](https://github.com/philkisters/ciweda)), but any other database schema can be used. If you choose a different database, ensure that both the database and the DWD inserter are updated accordingly.

## Run Geoserver Locally
If you want to use a GeoServer as a backend, you can set up a local GeoServer instance with the following command. The "netcdf" extension is installed automatically, allowing you to work with MODIS data.

> **Note:** As mentioned above, future work will incorporate satellite data (e.g., MODIS, Landsat) to enrich environmental context and potentially improve the precision of imputation results.

`docker run -it -p 8080:8080 --env INSTALL_EXTENSIONS=true --env STABLE_EXTENSIONS="netcdf" docker.osgeo.org/geoserver:2.28.x`

### Example WCS Request
Below you will find two example REST requests that can be used to access the WCS in order to obtain subsets of the Landsat or MODIS data:

`{geoserverurl}/geoserver/wcs?version=2.0.1&request=GetCoverage&service=WCS&CoverageID=Landsat9%3AL9-20240906&crs=EPSG%3A32632&format=image%2Ftiff&width=70&height=50&subset=E(560600,562700)&subset=N(5938500,5940000)`

`{geoserverurl}/geoserver/wcs?version=2.0.1&request=GetCoverage&service=WCS&CoverageID=modis_D1%3A20240906_lst_day&crs=EPSG%3A32632&format=image%2Ftiff&width=70&height=50&subset=Lat(53.591585,605044)&subset=Long(9.915843,9.947162)`

## Hamburg Coordinates
The following coordinates define the bounding box for Hamburg, Germany:

- **Northwest corner:** 53.605044, 9.915843
- **Southeast corner:** 53.591585, 9.947162

These can be used as the `subset` parameters in WCS requests to specify the area of interest.
