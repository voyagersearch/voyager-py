"""Creates or updates existing metadata."""
import os
import sys
import re
import tempfile
import xml.etree.cElementTree as eTree
import shutil
import arcpy
from voyager_tasks.utils import status
from voyager_tasks.utils import task_utils


def execute(request):
    """Writes existing metadata for summary, description and tags.
    If overwrite is false, existing metadata is untouched unless any
    field is empty or does not exist, then it is created.

    :param request: json as a dict.
    """
    parameters = request['params']
    input_items = task_utils.get_input_items(parameters)
    summary = task_utils.get_parameter_value(parameters, 'summary', 'value')
    description = task_utils.get_parameter_value(parameters, 'description', 'value')
    tags = task_utils.get_parameter_value(parameters, 'tags', 'value')
    # Handle commas, spaces, and/or new line separators.
    tags = [tag for tag in re.split(' |,|\n', tags) if not tag == '']
    try:
        overwrite = task_utils.get_parameter_value(parameters, 'overwrite', 'value')
    except KeyError:
        overwrite = False

    if not os.path.exists(request['folder']):
        os.makedirs(request['folder'])

    # Stylesheet
    xslt_file = os.path.join(arcpy.GetInstallInfo()['InstallDir'], 'Metadata/Stylesheets/gpTools/exact copy of.xslt')

    # Template metadata file.
    template_xml = os.path.join(os.path.dirname(__file__), 'supportfiles/{0}'.format('metadata_template.xml'))

    i = 1.
    updated = 0
    skipped = 0
    item_count = len(input_items)
    status_writer = status.Writer()

    for item in input_items:
        try:
            # Temporary XML file
            temp_xml = tempfile.NamedTemporaryFile(suffix='.xml', delete=True).name

            # Export xml
            try:
                arcpy.conversion.XSLTransform(item, xslt_file, temp_xml)
            except arcpy.ExecuteError:
                src_xml = os.path.join(arcpy.Describe(item).path, '{0}.xml'.format(os.path.basename(item)))
                shutil.copyfile(template_xml, src_xml)
                arcpy.conversion.XSLTransform(src_xml, xslt_file, temp_xml)

            # Read in XML
            tree = eTree.parse(temp_xml)
            root = tree.getroot()
            changes = 0

            # ISO allows many dataIdInfo groups; ArcGIS generally supports only one.
            data_id_elements = root.findall(".//dataIdInfo")
            if not data_id_elements:
                data_id_elements = [eTree.SubElement(root, 'dataIdInfo')]

            for data_id_element in data_id_elements:

                # Write summary.
                summary_element = root.findall(".//idPurp")
                if not summary_element:
                    summary_element = eTree.SubElement(data_id_element, 'idPurp')
                    summary_element.text = summary
                    changes += 1
                else:
                    for element in summary_element:
                        if overwrite or element.text is None:
                            element.text = summary
                            changes += 1

                # Write description.
                description_element = root.findall(".//idAbs")
                if not description_element:
                    description_element = eTree.SubElement(data_id_element, 'idAbs')
                    description_element.text = description
                    changes += 1
                else:
                    for element in description_element:
                        if overwrite or element.text is None:
                            element.text = description
                            changes += 1

                # Write tags.
                search_keys = root.findall(".//searchKeys")
                if not search_keys:
                    search_element = eTree.SubElement(data_id_element, 'searchKeys')
                    for tag in tags:
                        new_tag = eTree.SubElement(search_element, "keyword")
                        new_tag.text = tag
                        changes += 1
                elif not overwrite:
                    for search_element in search_keys:
                        keyword_elements = search_element.findall('.//keyword')
                        if not keyword_elements:
                            for tag in tags:
                                new_tag = eTree.SubElement(search_keys[0], "keyword")
                                new_tag.text = tag
                                changes += 1
                else:
                    for search_element in search_keys:
                        [search_element.remove(e) for e in search_element.findall('.//keyword')]
                        for tag in tags:
                            new_tag = eTree.SubElement(search_element, "keyword")
                            new_tag.text = tag
                            changes += 1

            if changes > 0:
                # Save modifications to the temporary XML file.
                tree.write(temp_xml)
                # Import the XML file to the item; existing metadata is replaced.
                arcpy.conversion.MetadataImporter(temp_xml, item)
                status_writer.send_percent(i/item_count, 'Metadata changed for: {0}.'.format(item), 'write_metadata')
                updated += 1
            else:
                status_writer.send_percent(i/item_count, 'No Metadata changes for: {0}.'.format(item), 'write_metadata')
                skipped += 1

        except Exception as ex:
            status_writer.send_percent(
                i/item_count,
                'Failed to write metadata: {0}. {1}.'.format(item, repr(ex)),
                'write_metadata'
            )
            skipped += 1
            pass

    try:
        shutil.copy2(os.path.join(os.path.dirname(__file__), 'supportfiles', '_thumb.png'), request['folder'])
    except IOError:
        status_writer.send_status('Could not copy thumbnail.')
        pass

    # Update state if necessary.
    if updated == 0:
        status_writer.send_state(status.STAT_FAILED, 'Could not write metadata for all results.')
        task_utils.report(os.path.join(request['folder'], '_report.md'), updated, skipped, skipped)
        sys.exit(1)
    if skipped > 0:
        status_writer.send_state(status.STAT_WARNING, 'Could not write metadata for {0} results.'.format(skipped))
        task_utils.report(os.path.join(request['folder'], '_report.md'), updated, skipped, skipped)