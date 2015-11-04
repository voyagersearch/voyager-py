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
import requests
from utils import status
from utils import task_utils


status_writer = status.Writer()
import arcpy
if arcpy.GetInstallInfo()['Version'] == '10.0':
    raise ImportError('write_metadata not available with ArcGIS 10.0.')

result_count = 0
processed_count = 0.
errors_reasons = {}
skipped_reasons = {}


def index_item(id, header):
    """Re-indexes an item.
    :param id: Item's index ID
    """
    solr_url = "{0}/flags?op=add&flag=__to_extract&fq=id:({1})&fl=*,[true]".format(sys.argv[2].split('=')[1], id)
    request = urllib2.Request(solr_url, headers=header)
    urllib2.urlopen(request)


def execute(request):
    """Writes existing metadata for summary, description and tags.
    If overwrite is false, existing metadata is untouched unless any
    field is empty or does not exist, then it is created.

    :param request: json as a dict.
    """
    updated = 0
    errors = 0
    skipped = 0
    global result_count
    parameters = request['params']
    summary = task_utils.get_parameter_value(parameters, 'summary', 'value')
    description = task_utils.get_parameter_value(parameters, 'description', 'value')
    tags = task_utils.get_parameter_value(parameters, 'tags', 'value')
    data_credits = task_utils.get_parameter_value(parameters, 'credits', 'value')
    constraints = task_utils.get_parameter_value(parameters, 'constraints', 'value')

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

    result_count, response_index = task_utils.get_result_count(parameters)
    headers = {'x-access-token': task_utils.get_security_token(request['owner'])}
    # Query the index for results in groups of 25.
    query_index = task_utils.QueryIndex(parameters[response_index])
    fl = query_index.fl + ',links'
    query = '{0}{1}{2}'.format(sys.argv[2].split('=')[1], '/select?&wt=json', fl)
    fq = query_index.get_fq()
    if fq:
        groups = task_utils.grouper(range(0, result_count), task_utils.CHUNK_SIZE, '')
        query += fq
    elif 'ids' in parameters[response_index]:
        groups = task_utils.grouper(list(parameters[response_index]['ids']), task_utils.CHUNK_SIZE, '')
    else:
        groups = task_utils.grouper(range(0, result_count), task_utils.CHUNK_SIZE, '')

    status_writer.send_percent(0.0, _('Starting to process...'), 'write_metadata')
    i = 0.
    for group in groups:
        i += len(group) - group.count('')
        if fq:
            results = requests.get(query + "&rows={0}&start={1}".format(task_utils.CHUNK_SIZE, group[0]), headers=headers)
        elif 'ids' in parameters[response_index]:
            results = requests.get(query + '{0}&ids={1}'.format(fl, ','.join(group)), headers=headers)
        else:
            results = requests.get(query + "&rows={0}&start={1}".format(task_utils.CHUNK_SIZE, group[0]), headers=headers)

        docs = results.json()['response']['docs']
        if not docs:
            docs = parameters[response_index]['response']['docs']

        input_items = []
        for doc in docs:
            if 'path' in doc:
                if 'links' in doc:
                    links = eval(doc['links'])
                    input_items.append((doc['path'], links['links'][0]['link'][0]['id']))
                else:
                    input_items.append((doc['path'], doc['id']))
        result = write_metadata(input_items, template_xml, xslt_file, summary, description, tags, data_credits, constraints, overwrite, headers)
        updated += result[0]
        errors += result[1]
        skipped += result[2]
        status_writer.send_percent(i / result_count, '{0}: {1:%}'.format("Processed", i / result_count), 'write_metadata')

    # Report state.
    if skipped > 0 or errors > 0:
        status_writer.send_state(status.STAT_WARNING, _('{0} results could not be processed').format(skipped + errors))
    else:
        status_writer.send_state(status.STAT_SUCCESS)
    task_utils.report(os.path.join(request['folder'], '__report.json'), updated, skipped, errors, errors_reasons, skipped_reasons)


def write_metadata(input_items, template_xml, xslt_file, summary, description, tags, data_credits, use_constraints, overwrite, token_header):
    """Writes metadata."""
    updated = 0
    errors = 0
    skipped = 0
    global processed_count

    for item in input_items:
        try:
            id = item[1]
            path = item[0]
            # Temporary XML file
            temp_xml = tempfile.NamedTemporaryFile(suffix='.xml', delete=True).name

            # Export xml
            try:
                arcpy.XSLTransform_conversion(path, xslt_file, temp_xml)
            except arcpy.ExecuteError:
                src_xml = os.path.join(arcpy.Describe(path).path, '{0}.xml'.format(os.path.basename(path)))
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
                tags = task_utils.get_unique_strings(tags)
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
                        if tags:
                            for tag in tags:
                                if tag.lower() not in [se.text.lower() for se in search_element.findall('.//keyword')]:
                                    new_tag = eTree.SubElement(search_element, "keyword")
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

                # Write credits.
                credits_element = root.findall(".//idCredit")
                if not credits_element:
                    credits_element = eTree.SubElement(data_id_element, 'idCredit')
                    credits_element.text = data_credits
                    changes += 1
                else:
                    for element in credits_element:
                        if data_credits and (overwrite or element.text is None):
                            element.text = data_credits
                            changes += 1

                # Write use constraints.
                res_constraints = root.findall(".//resConst")
                if not res_constraints:
                    res_constraint_element = eTree.SubElement(data_id_element, 'resConst')
                    const_element = eTree.SubElement(res_constraint_element, 'Consts')
                    new_constraint = eTree.SubElement(const_element, 'useLimit')
                    new_constraint.text = use_constraints
                    changes += 1
                elif not overwrite:
                    constraint_elements = root.findall('.//Consts')
                    for element in constraint_elements:
                        if use_constraints:
                            new_constraint = eTree.SubElement(element, 'useLimit')
                            new_constraint.text = use_constraints
                            changes += 1
                else:
                    if use_constraints:
                        constraint_elements = root.findall('.//Consts')
                        if constraint_elements:
                            [constraint_elements[0].remove(e) for e in constraint_elements[0].findall('.//useLimit')]
                            new_constraint = eTree.SubElement(constraint_elements[0], 'useLimit')
                            new_constraint.text = use_constraints
                            changes += 1

            if changes > 0:
                # Save modifications to the temporary XML file.
                tree.write(temp_xml)
                # Import the XML file to the item; existing metadata is replaced.
                arcpy.MetadataImporter_conversion(temp_xml, path)
                status_writer.send_percent(processed_count / result_count, _('Metadata updated for: {0}').format(path), 'write_metadata')
                processed_count += 1

                try:
                    index_item(id, token_header)
                except (IndexError, urllib2.HTTPError, urllib2.URLError) as e:
                    status_writer.send_status(e.message)
                    pass
                updated += 1
            else:
                processed_count += 1
                status_writer.send_percent(processed_count / result_count, _('No metadata changes for: {0}').format(path), 'write_metadata')
                skipped_reasons[path] = _('No metadata changes for: {0}').format(path)
                skipped += 1
        except Exception as ex:
            processed_count += 1
            status_writer.send_percent(processed_count / result_count, _('Skipped: {0}').format(path), 'write_metadata')
            status_writer.send_status(_('FAIL: {0}').format(repr(ex)))
            errors_reasons[path] = repr(ex)
            errors += 1
            pass

    return updated, errors, skipped
