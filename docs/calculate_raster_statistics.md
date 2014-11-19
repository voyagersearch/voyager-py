#Calculate Raster Statistics

Calculates statistics for raster datasets and mosaic datasets.

###Usage Tips
  - RECOMMENDED: See the ArcGIS help topics below for information about calculating statistics.
  - Statistics are required for your raster and mosaic datasets to perform tasks such as reclassification.
  - Statistics allows ArcGIS to properly stretch and symbolize raster data for display.
  - The Processing Extent is not supported for ArcGIS 10.0. It will be ignored and the full extent is used.
  - A skip factor is not used for all raster formats. The supported raster formats include TIFF, IMG, NITF, DTED, RAW, ADRG, CIB, CADRG, DIGEST, GIS, LAN, CIT, COT, ERMapper, ENVI DAT, BIL, BIP, BSQ, and geodatabase.
  - The skip factors control the portion of the raster dataset that is used when calculating the statistics, where a value of 1 will use each pixel and a value of 2 will use every second pixel. 
  - The skip factor values can only range from 1 to the number of columns/rows in the raster.
  - For mosaic datasets, the statistics are calculated for the top-level mosaicked image and not for every raster contained within the mosaic dataset.
  - Mosaic datasets tend to be very large and specifying a skip factor is highly recommended. 
  - The Esri Grid and the RADARSAT2 formats always use a skip factor of 1. 
  
###Requirements
    - ArcGIS 10.x

### See Also
[Raster Data Statistics]: http://resources.arcgis.com/en/help/main/10.2/index.html#//009t0000001s000000 "Raster Data Statistics"
[Calculate Statistics]: http://resources.arcgis.com/en/help/main/10.2/index.html#/Calculate_Statistics/0017000000m3000000/ "Calculate Statistics"
- [Raster Data Statistics] (Source: ArcGIS)
- [Calculate Statistics] (Source: Arcgis)

[Voyager Search]:http://voyagersearch.com/
[@VoyagerGIS]:https://twitter.com/voyagergis
[github]:https://github.com/voyagersearch/tasks

