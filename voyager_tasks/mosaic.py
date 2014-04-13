"""Mosaic input rasters into a new raster dataset."""
import os
import sys
import collections
import shutil
import arcpy
from voyager_tasks.utils import status
from voyager_tasks.utils import task_utils


pixel_types = {"U1": "1_BIT",
               "U2": "2_BIT",
               "U4": "4_BIT",
               "U8": "8_BIT_UNSIGNED",
               "S8": "8_BIT_SIGNED",
               "U16": "16_BIT_UNSIGNED",
               "S16": "16_BIT_SIGNED",
               "U32": "32_BIT_UNSIGNED",
               "S32": "32_BIT_SIGNED",
               "F32": "32_BIT_FLOAT",
               "F64": "64_BIT"}


def execute(request):
    """Mosaics input raster datasets into a new raster dataset.
    :param request: json as a dict.
    """
    parameters = request['params']
    input_items = task_utils.get_parameter_value(parameters, 'input_items')
    out_coordinate_system = int(task_utils.get_parameter_value(parameters, 'output_projection', 'code'))
    # Get the clip region as an extent object.
    try:
        clip_area = task_utils.get_parameter_value(parameters, 'processing_extent', 'wkt')
        # WKT coordinates for each task are always WGS84.
        gcs_sr = task_utils.get_spatial_reference(4326)
        clip_area = task_utils.from_wkt(clip_area, gcs_sr)
        if not clip_area.area > 0:
            clip_area = task_utils.from_wkt('POLYGON ((-180 -90, -180 90, 180 90, 180 -90, -180 -90))', gcs_sr)
        if out_coordinate_system:
            out_sr = task_utils.get_spatial_reference(out_coordinate_system)
            if not out_sr.name == gcs_sr.name:
                try:
                    geo_transformation = arcpy.ListTransformations(gcs_sr, out_sr)[0]
                    clip_area = clip_area.projectAs(out_sr, geo_transformation)
                except AttributeError:
                    clip_area = clip_area.projectAs(out_sr)
        clip_area = clip_area.extent
        arcpy.env.outputCoordinateSystem = out_sr
    except KeyError:
        try:
            clip_area = task_utils.get_parameter_value(parameters, 'processing_extent', 'feature')
            clip_area = arcpy.Describe(clip_area).extent
        except KeyError:
            clip_area = None

    # Advanced options
    output_raster_format = task_utils.get_parameter_value(parameters, 'raster_format', 'value')
    compression_method = task_utils.get_parameter_value(parameters, 'compression_method', 'value')
    compression_quality = task_utils.get_parameter_value(parameters, 'compression_quality', 'value')
    arcpy.env.compression = '{0} {1}'.format(compression_method, compression_quality)

    out_workspace = os.path.join(request['folder'], 'temp')
    if not os.path.exists(out_workspace):
        os.makedirs(out_workspace)

    if output_raster_format == 'FileGDB':
        out_workspace = arcpy.management.CreateFileGDB(out_workspace, 'output.gdb').getOutput(0)
    arcpy.env.workspace = out_workspace

    status_writer = status.Writer()
    pixels = []
    raster_items = []
    bands = collections.defaultdict(int)
    skipped = 0
    for item in input_items:
        # Number of bands for each item should be the same.
        dsc = arcpy.Describe(item)
        if dsc.datasettype == 'RasterDataset':
            raster_items.append(item)
            pixels.append(dsc.pixeltype)
            bands[dsc.bandcount] = 1
            if len(bands) > 1:
                status_writer.send_state(status.STAT_FAILED, 'Input rasters must have the same number of bands.')
                return
        else:
            status_writer.send_status('{0} is not a raster dataset and will not be processed.'.format(item))
            skipped += 1

    # Get most common pixel type.
    pixel_type = pixel_types[max(set(pixels), key=pixels.count)]
    if output_raster_format in ('FileGDB', 'GRID'):
        output_name = arcpy.ValidateTableName('mosaic', out_workspace)
    else:
        output_name = '{0}.{1}'.format(arcpy.ValidateTableName('mosaic', out_workspace), output_raster_format.lower())

    try:
        if clip_area is not None:
            ext = '{0} {1} {2} {3}'.format(clip_area.XMin, clip_area.YMin, clip_area.XMax, clip_area.YMax)
            status_writer.send_status('Running mosaic to new raster...')
            tmp_mosaic = arcpy.MosaicToNewRaster_management(
                raster_items,
                out_workspace,
                'tmpMosaic',
                out_coordinate_system,
                pixel_type,
                number_of_bands=bands.keys()[0]
            )
            status_writer.send_status('Clipping output mosaic...')
            arcpy.Clip_management(tmp_mosaic, ext, output_name)
            arcpy.Delete_management(tmp_mosaic)
        else:
            status_writer.send_status('Running mosaic to new raster...')
            arcpy.MosaicToNewRaster_management(raster_items,
                                               out_workspace,
                                               output_name,
                                               out_coordinate_system,
                                               pixel_type, number_of_bands=bands.keys()[0])
    except arcpy.ExecuteError:
        status_writer.send_state(status.STAT_FAILED, arcpy.GetMessages(2))
        task_utils.report(os.path.join(request['folder'], '_report.md'), request['task'], 0, len(raster_items))
        sys.exit(1)

    if arcpy.env.workspace.endswith('.gdb'):
        out_workspace = os.path.dirname(arcpy.env.workspace)
    zip_file = task_utils.zip_data(out_workspace, 'output.zip')
    status_writer.send_status('Created the output zip file.')
    shutil.move(zip_file, os.path.join(os.path.dirname(out_workspace), os.path.basename(zip_file)))
    shutil.copyfile(
        os.path.join(os.path.dirname(__file__), r'supportfiles\_thumb.png'),
        os.path.join(request['folder'], '_thumb.png')
    )
    task_utils.report(os.path.join(request['folder'], '_report.md'), request['task'], len(raster_items), skipped)
