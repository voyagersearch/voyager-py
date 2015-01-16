#Mosaic

Mosaic selected raster datasets to a new raster where the result is to be downloaded.

###Usage Tips
  - The result is compressed into a zip file named output.zip that can be downloaded.
  - The number of bands for each input must be the same or the task will fail.
  - The clip region is used to clip the output mosaic dataset.
  - The clip region is ignored when the output format is a MosaicDataset.
  - The projection is used to project the output results. The default is WGS84.
  - Outputs can be saved to BIL, BIP, BMP, BSQ, DAT, Esri Grid, GIF, IMG, JPEG, JPEG 2000, PNG, TIFF, or any geodatabase raster dataset including a mosaic dataset.
  - If the raster format is 'MosaicDataset', a mosaic dataset is created in a File Geodatabase and the results are added to it. **This option requires an ArcGIS Standard or Advanced license.**
  - The most common pixel type of the inputs is used.
  - For raster formats JPG, JP2, and FileGDB, the advanced settings for setting compression can be used.
  - The GIF format only supports single-band raster datasets.
  - Large input rasters will require longer processing times. The status can be checked on the Task History page. 

###Screen Captures
![Mosaic] (imgs/mosaic_0.png "Mosaic example")

###Requirements
    - ArcGIS 10.x

### See Also
[What is a Mosaic?]: http://resources.arcgis.com/en/help/main/10.2/index.html#//009t000000n6000000 "What is a Mosaic?"
[Mosaic To New Raster]: http://resources.arcgis.com/en/help/main/10.2/#/Mosaic_To_New_Raster/001700000098000000/ "Mosaic To New Raster"
- [What is a Mosaic?]
- [Mosaic To New Raster] (ArcGIS tool used by this task)

[Voyager Search]:http://voyagersearch.com/
[@VoyagerGIS]:https://twitter.com/voyagergis
[github]:https://github.com/voyagersearch/tasks

