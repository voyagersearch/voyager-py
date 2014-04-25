#Convert to KML

Converts feature or raster data into Keyhole Markup Language (KML). The output KML files can be read by any KML client including ArcGIS Explorer, ArcGlobe, and Google Earth.

###Usage Tips
  - Each output result is converted to a compressed file with a .kmz extension where geometries and symbology is maintained.
  - Converting a single result produces a single .kmz file that can be downloaded. If there are multiple results, the .kmz files are added to a zip file that can be downloaded.
  - A processing extent can be specified to limit the geographic area being exported.
  - If a processing extent is not specified, the full extent of inputs is converted. 
  - All output KML files are created in the WGS84 coordinate system. 
  

###Screen Captures
![Convert to KML] (imgs/convert_to_kml_0.png "Convert to KML example")

###Requirements
    - ArcGIS 10.x

### See Also
[ArcGIS tools for conversion to KML]: http://resources.arcgis.com/en/help/main/10.2/#/An_overview_of_the_To_KML_toolset/00120000002n000000/ "Esri tools for conversion to KML"
- Learn more about the [ArcGIS tools for conversion to KML] used by this task

[Voyager Search]:http://voyagersearch.com/
[@VoyagerGIS]:https://twitter.com/voyagergis
[github]:https://github.com/voyagersearch/tasks

    