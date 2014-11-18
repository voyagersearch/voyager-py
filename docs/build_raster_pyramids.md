#Build Raster Pyramids

Builds pyramids for raster datasets, raster catalogs and mosaic datasets.

###Usage Tips
  - Building raster pyramids provides better performance when displaying raster datasets.
  - Pyramids will only be built if they do not exist.
  - Pyramids are not built for raster datasets having less than 1024 pixels in the row or column.
  - The default resampling method is NEAREST_NEIGHBOR (the value of the closest cell is assigned to the output cell).
  - The Advanced Options can be used to change the compression method and quality. 
  - See the ArcGIS help topics below for information about resampling methods and compression methods.

###Requirements
    - ArcGIS 10.x

### See Also
[What are Raster Pyramids?]: http://resources.arcgis.com/en/help/main/10.2/index.html#//009t00000019000000/ "What are Raster Pyramids?"
[Build Pyramids]: http://resources.arcgis.com/en/help/main/10.2/index.html#//0017000000m1000000/ "Build Pyramids"
- [What are Raster Pyramids?] (Source: ArcGIS)
- [Build Pyramids] (Source: Arcgis)

[Voyager Search]:http://voyagersearch.com/
[@VoyagerGIS]:https://twitter.com/voyagergis
[github]:https://github.com/voyagersearch/tasks

