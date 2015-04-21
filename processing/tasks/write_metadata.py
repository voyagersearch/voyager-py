# -*- coding: utf-8 -*-
# (C) Copyright 2014 Voyager Search
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import os
import re
import sys
import tempfile
import urllib2
import xml.etree.cElementTree as eTree
import shutil
from tasks.utils import status
from tasks.utils import task_utils
from tasks import _


status_writer = status.Writer()
import arcpy
if arcpy.GetInstallInfo()['Version'] == '10.0':
    raise ImportError('write_metadata not available with ArcGIS 10.0.')


def index_item(id):
    """Re-indexes an item.
    :param id: Item's index ID
    """
    try:
        solr_url = "{0}/flags?op=add&flag=__to_extract&fq=id:({1})&fl=*,[true]".format(sys.argv[2].split('=')[1], id)
        request = urllib2.Request(solr_url, headers={'Content-type': 'application/json'})
        response = urllib2.urlopen(request)
        if not response.code == 200:
            status_writer.send_state(status.STAT_FAILED, 'Error sending {0}: {1}'.format(id, response.code))
            return
    except urllib2.HTTPError as http_error:
        status_writer.send_state(status.STAT_FAILED, http_error.message)
        return
    except urllib2.URLError as url_error:
        status_writer.send_state(status.STAT_FAILED, url_error.message)
        return


def execute(request):
    """Writes existing metadata for summary, description and tags.
    If overwrite is false, existing metadata is untouched unless any
    field is empty or does not exist, then it is created.

    :param request: json as a dict.
    """
    updated = 0
    errors = 0
    skipped = 0
    parameters = request['params']
    summary = task_utils.get_parameter_value(parameters, 'summary', 'value')
    description = task_utils.get_parameter_value(parameters, 'description', 'value')
    tags = task_utils.get_parameter_value(parameters, 'tags', 'value')
    # Handle commas, spaces, and/or new line separators.
    tags = [tag for tag in re.split(' |,|\n', tags) if not tag == '']
    overwrite = task_utils.get_parameter_value(parameters, 'overwrite', 'value')
    if not overwrite:
        overwrite = False
    if not os.path.exists(request['folder']):
        os.makedirs(request['folder'])

    # Stylesheet
    xslt_file = os.path.join(arcpy.GetInstallInfo()['InstallDir'], 'Metadata/Stylesheets/gpTools/exact copy of.xslt')

    # Template metadata file.
    template_xml = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'supportfiles', 'metadata_template.xml')

    num_results, response_index = task_utils.get_result_count(parameters)
    if num_results > task_utils.CHUNK_SIZE:
        # Query the index for results in groups of 25.
        query_index = task_utils.QueryIndex(parameters[response_index])
        fl = query_index.fl

        #query = '{0}{1}{2}'.format("http://localhost:8888/solr/v0", '/select?&wt=json', fl)
        query = '{0}{1}{2}'.format(sys.argv[2].split('=')[1], '/select?&wt=json', fl)
        fq = query_index.get_fq()
        if fq:
            groups = task_utils.grouper(range(0, num_results), task_utils.CHUNK_SIZE, '')
            query += fq
        else:
            groups = task_utils.grouper(list(parameters[response_index]['ids']), task_utils.CHUNK_SIZE, '')

        status_writer.send_percent(0.0, _('Starting to process...'), 'write_metadata')
        i = 0.
        for group in groups:
            i += len(group) - group.count('')
            if fq:
                results = urllib2.urlopen(query + "&rows={0}&start={1}".format(task_utils.CHUNK_SIZE, group[0]))
            else:
                results = urllib2.urlopen(query + '{0}&ids={1}'.format(fl, ','.join(group)))

            input_items = task_utils.get_input_items(eval(results.read())['response']['docs'], True)
            result = write_metadata(input_items, template_xml, xslt_file, summary, description, tags, overwrite)
            updated += result[0]
            errors += result[1]
            skipped += result[2]
            status_writer.send_percent(i / num_results, '{0}: {1:%}'.format("Processed", i / num_results), 'write_metadata')
    else:
        input_items = task_utils.get_input_items(parameters[response_index]['response']['docs'], True)
        updated, errors, skipped = write_metadata(input_items, template_xml, xslt_file,
                                                  summary, description, tags, overwrite, True)

    try:
        shutil.copy2(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'supportfiles', '_thumb.png'), request['folder'])
    except IOError:
        pass

    # Report state.
    if skipped > 0 or errors > 0:
        status_writer.send_state(status.STAT_WARNING, _('{0} results could not be processed').format(skipped + errors))
    else:
        status_writer.send_state(status.STAT_SUCCESS)
    task_utils.report(os.path.join(request['folder'], '_report.json'), updated, skipped, errors)


def write_metadata(input_items, template_xml, xslt_file, summary, description, tags, overwrite, show_progress=False):
    """Writes metadata."""
    updated = 0
    errors = 0
    skipped = 0
    if show_progress:
        item_count = len(input_items)
        i = 1.

    for item in input_items:
        try:
            # Temporary XML file
            temp_xml = tempfile.NamedTemporaryFile(suffix='.xml', delete=True).name

            # Export xml
            try:
                arcpy.XSLTransform_conversion(item, xslt_file, temp_xml)
            except arcpy.ExecuteError:
                src_xml = os.path.join(arcpy.Describe(item).path, '{0}.xml'.format(os.path.basename(item)))
                shutil.copyfile(template_xml, src_xml)
                arcpy.XSLTransform_conversion(src_xml, xslt_file, temp_xml)

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
                        if summary and (overwrite or element.text is None):
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
                        if description and (overwrite or element.text is None):
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
                    # Still add any new tags.
                    for search_element in search_keys:
                        #keyword_elements = search_element.findall('.//keyword')
                        if tags:
                            for tag in tags:
                                new_tag = eTree.SubElement(search_keys[0], "keyword")
                                new_tag.text = tag
                                changes += 1
                else:
                    if tags:
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
                arcpy.MetadataImporter_conversion(temp_xml, item)
                if show_progress:
                    status_writer.send_percent(i / item_count, _('Metadata updated for: {0}').format(item), 'write_metadata')
                    i += 1
                else:
                    status_writer.send_status(_('Metadata updated for: {0}').format(item))
                index_item(input_items[item][1])
                updated += 1
            else:
                if show_progress:
                    status_writer.send_percent(i / item_count, _('No metadata changes for: {0}').format(item), 'write_metadata')
                    i += 1
                else:
                    status_writer.send_status(_('No metadata changes for: {0}').format(item))
                skipped += 1
        except Exception as ex:
            if show_progress:
                status_writer.send_percent(i / item_count, _('Skipped: {0}').format(item), 'write_metadata')
                i += 1
            status_writer.send_status(_('FAIL: {0}').format(repr(ex)))
            errors += 1
            pass

    return updated, errors, skipped
