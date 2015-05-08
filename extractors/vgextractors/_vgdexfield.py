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
class VgDexField:
    # ID  type: STRING
    ID = "id";

    # NAME  type: TEXT
    NAME = "name";

    # NAME_ALIAS  type: STRING
    NAME_ALIAS = "name_alias";

    # TITLE  type: TEXT
    TITLE = "title";

    # SUBTITLE  type: TEXT
    SUBTITLE = "subtitle";

    # PATH  type: TEXT
    PATH = "path";

    # URI  type: STRING
    URI = "uri";

    # FORMAT  type: LOWER
    FORMAT = "format";

    # LANGUAGE  type: STRING
    LANGUAGE = "lang";

    # LANGUAGES  type: STRING
    LANGUAGES = "langs";

    # TYPE  type: STRING
    TYPE = "type";

    # IS_MISSING  type: PROP
    IS_MISSING = "is_missing";

    # UNITS  type: LOWER
    UNITS = "units";

    # COMPONENT_FILE  type: STRING
    COMPONENT_FILE = "component_files";

    # EMBEDDED_FILE_COUNT  type: INT
    EMBEDDED_FILE_COUNT = "embedded_file_count";

    # EMBEDDED_MIME  type: LOWER
    EMBEDDED_MIME = "embedded_mime";

    # EMBEDDED_NAME  type: TEXT
    EMBEDDED_NAME = "embedded_name";

    # MAX_DEPTH  type: INT
    MAX_DEPTH = "max_depth";

    # FOLDER  type: STRING
    FOLDER = "folder";

    # GEOGRAPIC_COVERAGE  type: TEXT
    GEOGRAPIC_COVERAGE = "geographic_coverage";

    # TEMPORAL_COVERAGE  type: TEXT
    TEMPORAL_COVERAGE = "temporal_coverage";

    # GFID  type: STRING
    GFID = "gfid";

    # BITRATE  type: STRING
    BITRATE = "bitrate";

    # GPS_TIME  type: DATE
    GPS_TIME = "gps_time";

    # HOST  type: STRING
    HOST = "host";

    # SERVICE  type: STRING
    SERVICE = "service";

    # SERVICE_URL  type: HREF
    SERVICE_URL = "service_url";

    # SERVICE_ROOT  type: HREF
    SERVICE_ROOT = "service_root";

    # SERVICE_HOST  type: HREF
    SERVICE_HOST = "service_host";

    # ABSTRACT  type: TEXT
    ABSTRACT = "abstract";

    # PURPOSE  type: TEXT
    PURPOSE = "purpose";

    # ANNOTATION  type: TEXT
    ANNOTATION = "annotation";

    # ENCODING  type: STRING
    ENCODING = "encoding";

    # VERSION  type: STRING
    VERSION = "version";

    # META_LOCATOR  type: STRING
    META_LOCATOR = "meta_locator";

    # META_MIME  type: STRING
    META_MIME = "meta_mime";

    # META_NAMESPACES  type: STRING
    META_NAMESPACES = "meta_namespaces";

    # META_DEFAULT_NS  type: STRING
    META_DEFAULT_NS = "meta_default_ns";

    # META_SCHEMA_LOC  type: STRING
    META_SCHEMA_LOC = "meta_schema_loc";

    # PARAMETERS  type: STRING
    PARAMETERS = "parameters";

    # PARAMETER_TYPE  type: STRING
    PARAMETER_TYPE = "parameter_type";

    # HREFS  type: HREF
    HREFS = "hrefs";

    # OUTLINKS  type: HREF
    OUTLINKS = "outlinks";

    # SECURITY_CLASSIFICATION  type: STRING
    SECURITY_CLASSIFICATION = "sec_class";

    # SECURITY_CLASSIFICATION_META  type: STRING
    SECURITY_CLASSIFICATION_META = "sec_class_meta";

    # SECURITY_CLASSIFICATION_SYS_ID  type: STRING
    SECURITY_CLASSIFICATION_SYS_ID = "sec_class_sys_id";

    # SECURITY_CLASSIFICATION_BY  type: STRING
    SECURITY_CLASSIFICATION_BY = "sec_class_by";

    # SECURITY_CLASSIFICATION_REASON  type: TEXT
    SECURITY_CLASSIFICATION_REASON = "sec_class_reason";

    # SECURITY_OWNER_PRODUCER  type: STRING
    SECURITY_OWNER_PRODUCER = "sec_owner_producer";

    # SECURITY_RELEASABLE_TO  type: STRING
    SECURITY_RELEASABLE_TO = "sec_releasable_to";

    # SECURITY_ATOMIC_ENERGY  type: STRING
    SECURITY_ATOMIC_ENERGY = "sec_atomic_energy";

    # SECURITY_DECLASS_DATE  type: DATE
    SECURITY_DECLASS_DATE = "sec_declass_date";

    # SECURITY_CLASSIFICATION_ORIG  type: STRING
    SECURITY_CLASSIFICATION_ORIG = "sec_class_orig";

    # SECURITY_CLASSIFICATIONS_LIST  type: STRING
    SECURITY_CLASSIFICATIONS_LIST = "sec_class_list";

    # AGS_NUM_COMMENTS  type: INT
    AGS_NUM_COMMENTS = "ags_num_comments";

    # AGS_NUM_RATINGS  type: INT
    AGS_NUM_RATINGS = "ags_num_ratings";

    # AGS_NUM_VIEWS  type: INT
    AGS_NUM_VIEWS = "ags_num_views";

    # AGS_AVG_RATING  type: FLOAT
    AGS_AVG_RATING = "ags_avg_rating";

    # AGS_THUMBNAIL  type: STRING
    AGS_THUMBNAIL = "ags_thumbnail";

    # AGS_TYPE  type: STRING
    AGS_TYPE = "ags_type";

    # AGS_HAS_DATA  type: PROP
    AGS_HAS_DATA = "ags_has_data";

    # AGS_FORMAT_KEYWORD  type: STRING
    AGS_FORMAT_KEYWORD = "ags_format_keyword";

    # GEOCORTEX_HAS_FEATURE  type: STRING
    GEOCORTEX_HAS_FEATURE = "geocortex_has_feature";

    # GEOCORTEX_HAS_PROPERTY  type: STRING
    GEOCORTEX_HAS_PROPERTY = "geocortex_has_property";

    # SERVICE_TYPE  type: STRING
    SERVICE_TYPE = "service_type";

    # SERVICE_FUNCTION  type: STRING
    SERVICE_FUNCTION = "service_function";

    # SERVICE_PROPS  type: STRING
    SERVICE_PROPS = "service_properties";

    # MAP_PROPERTIES  type: STRING
    MAP_PROPERTIES = "map_properties";

    # EXPRESSION  type: STRING
    EXPRESSION = "expression";

    # VIDEO_CODEC  type: STRING
    VIDEO_CODEC = "audio_codec";

    # AUDIO_CODEC  type: STRING
    AUDIO_CODEC = "video_codec";

    # COMMENTS_ENABLED  type: BOOLEAN
    COMMENTS_ENABLED = "comments_enabled";

    # THUMB_SUPPORTED  type: BOOLEAN
    THUMB_SUPPORTED = "thumbSupported";

    # FILE_CREATION_DAY  type: STRING
    FILE_CREATION_DAY = "file_creation_day";

    # OFFSET_X  type: DOUBLE
    OFFSET_X = "offset_x";

    # OFFSET_Y  type: DOUBLE
    OFFSET_Y = "offset_y";

    # OFFSET_Z  type: DOUBLE
    OFFSET_Z = "offset_z";

    # SCALE_FACTOR_X  type: DOUBLE
    SCALE_FACTOR_X = "scale_factor_x";

    # SCALE_FACTOR_Y  type: DOUBLE
    SCALE_FACTOR_Y = "scale_factor_y";

    # SCALE_FACTOR_Z  type: DOUBLE
    SCALE_FACTOR_Z = "scale_factor_z";

    # NUMBER_OF_POINTS  type: LONG
    NUMBER_OF_POINTS = "number_of_points";

    # NUMBER_OF_POINTS_BY_RETURN  type: STRING
    NUMBER_OF_POINTS_BY_RETURN = "number_of_points_by_return";

    # OFFSET_TO_POINT_DATA  type: LONG
    OFFSET_TO_POINT_DATA = "offset_to_point_data";

    # POINT_DATA_FORMAT  type: STRING
    POINT_DATA_FORMAT = "point_data_format";

    # FILE_SOURCE_ID  type: STRING
    FILE_SOURCE_ID = "file_source_id";

    # SYSTEM_IDENTIFIER  type: STRING
    SYSTEM_IDENTIFIER = "system_identifier";

    # MODEL_COUNT  type: INT
    MODEL_COUNT = "model_count";

    # LAYER_COUNT  type: INT
    LAYER_COUNT = "layer_count";

    # LAYOUT_COUNT  type: INT
    LAYOUT_COUNT = "layout_count";

    # WMS_CAPABILITIES  type: STRING
    WMS_CAPABILITIES = "wms_capabilities";

    # WMS_MAP_FORMATS  type: STRING
    WMS_MAP_FORMATS = "wms_map_formats";

    # WMS_LEGEND_FORMATS  type: STRING
    WMS_LEGEND_FORMATS = "wms_legend_formats";

    # WMS_DESCRIBE_FORMATS  type: STRING
    WMS_DESCRIBE_FORMATS = "wms_describe_formats";

    # WMS_FEATURE_FORMATS  type: STRING
    WMS_FEATURE_FORMATS = "wms_feature_formats";

    # WMS_STYLE_FORMATS  type: STRING
    WMS_STYLE_FORMATS = "wms_style_formats";

    # WMS_COMMON_SRS  type: STRING
    WMS_COMMON_SRS = "wms_common_srs";

    # WMS_STYLES  type: STRING
    WMS_STYLES = "wms_styles";

    # WMS_LAYER_NAME  type: STRING
    WMS_LAYER_NAME = "wms_layer_name";

    # SUPPORTS_VERSION  type: STRING
    SUPPORTS_VERSION = "supports_version";

    # SUPPORTED_CRS  type: STRING
    SUPPORTED_CRS = "supported_crs";

    # GUID  type: STRING
    GUID = "guid";

    # BE_NUMBER  type: STRING
    BE_NUMBER = "be_number";

    # AUTHOR  type: STRING
    AUTHOR = "author";

    # ORGANIZATION  type: STRING
    ORGANIZATION = "organization";

    # PUBLISHER  type: STRING
    PUBLISHER = "publisher";

    # DESCRIPTION  type: TEXT
    DESCRIPTION = "description";

    # DOCUMENTATION  type: TEXT
    DOCUMENTATION = "documentation";

    # SNIPPIT  type: TEXT
    SNIPPIT = "snippit";

    # SUBJECT  type: STRING
    SUBJECT = "subject";

    # KEYWORDS  type: STRING
    KEYWORDS = "keywords";

    # CATEGORY  type: STRING
    CATEGORY = "category";

    # CREDITS  type: TEXT
    CREDITS = "credits";

    # RIGHTS  type: TEXT
    RIGHTS = "rights";

    # THEME  type: STRING
    THEME = "theme";

    # CULTURE  type: STRING
    CULTURE = "culture";

    # COPYRIGHT  type: STRING
    COPYRIGHT = "copyright";

    # FEES  type: STRING
    FEES = "fees";

    # ORIGINATOR  type: STRING
    ORIGINATOR = "originator";

    # ORIGINATING_SOURCE  type: STRING
    ORIGINATING_SOURCE = "originating_source";

    # CONTENT_REPOSITORY  type: STRING
    CONTENT_REPOSITORY = "content_repository";

    # FORMAT_TYPE  type: STRING
    FORMAT_TYPE = "format_type";

    # FORMAT_CATEGORY  type: STRING
    FORMAT_CATEGORY = "format_category";

    # FORMAT_APP  type: STRING
    FORMAT_APP = "format_app";

    # FORMAT_KEYWORD  type: STRING
    FORMAT_KEYWORD = "format_keyword";

    # FORMAT_COMPANY  type: STRING
    FORMAT_COMPANY = "format_company";

    # RESOURCE_FORMAT  type: STRING
    RESOURCE_FORMAT = "resource_format";

    # DISTRIBUTION_FORMAT  type: STRING
    DISTRIBUTION_FORMAT = "distribution_format";

    # DC_RELATION  type: STRING
    DC_RELATION = "dc_relation";

    # ACCESS_CONSTRAINTS  type: TEXT
    ACCESS_CONSTRAINTS = "access_constraints";

    # USE_CONSTRAINTS  type: TEXT
    USE_CONSTRAINTS = "use_constraints";

    # RESPONSIBLE_PARTY  type: TEXT
    RESPONSIBLE_PARTY = "responsibleParty";

    # LICENSE_INFO  type: TEXT
    LICENSE_INFO = "license_info";

    # LICENSE_ID  type: STRING
    LICENSE_ID = "license_id";

    # IMAGE_URL  type: STRING
    IMAGE_URL = "image_url";

    # AGS_VIEW_IN  type: STRING
    AGS_VIEW_IN = "ags_view_in";

    # AGS_INTERFACE  type: STRING
    AGS_INTERFACE = "ags_interface";

    # AGS_OPERATION  type: STRING
    AGS_OPERATION = "ags_operation";

    # AGS_FORMATS  type: STRING
    AGS_FORMATS = "ags_formats";

    # AGS_FUSED_CACHE  type: PROP
    AGS_FUSED_CACHE = "ags_fused_cache";

    # AGS_STATUS  type: STRING
    AGS_STATUS = "ags_status";

    # AGS_ACCESS  type: STRING
    AGS_ACCESS = "ags_access";

    # AGS_HAS_TILE_SERVER  type: PROP
    AGS_HAS_TILE_SERVER = "ags_has_tile_server";

    # FME_SERVICE  type: STRING
    FME_SERVICE = "fme_service";

    # FME_TRANSFORMER  type: STRING
    FME_TRANSFORMER = "fme_transformer";

    # FME_CMD_LINE  type: STRING
    FME_CMD_LINE = "fme_cmdline";

    # FME_ROLE  type: STRING
    FME_ROLE = "fme_role";

    # FME_FORMAT  type: STRING
    FME_FORMAT = "fme_format";

    # EXECUTION_TYPE  type: STRING
    EXECUTION_TYPE = "execution_type";

    # RESULT_MAP_SERVER_NAME  type: STRING
    RESULT_MAP_SERVER_NAME = "result_map_server_name";

    # IMPORTS  type: STRING
    IMPORTS = "imports";

    # CITY  type: STRING
    CITY = "city";

    # STATE  type: STRING
    STATE = "state";

    # COUNTY  type: STRING
    COUNTY = "county";

    # COUNTRY  type: STRING
    COUNTRY = "country";

    # PROVINCE  type: STRING
    PROVINCE = "province";

    # API  type: STRING
    API = "API";

    # UWI  type: STRING
    UWI = "UWI";

    # SERVICE_COMPANY  type: STRING
    SERVICE_COMPANY = "service_company";

    # SECTION  type: STRING
    SECTION = "section";

    # WELL_LOCATION  type: STRING
    WELL_LOCATION = "well_location";

    # WELL_FIELD  type: STRING
    WELL_FIELD = "well_field";

    # WELL_NAME  type: STRING
    WELL_NAME = "well_name";

    # TEXT  type: TEXT
    TEXT = "text";

    # NOTES  type: TEXT
    NOTES = "notes";

    # MANAGER  type: STRING
    MANAGER = "manager";

    # COMPANY  type: STRING
    COMPANY = "company";

    # HPFS_SECURITY  type: STRING
    HPFS_SECURITY = "hpfsSecurity";

    # LAST_AUTHOR  type: STRING
    LAST_AUTHOR = "lastAuthor";

    # COMMENTS  type: TEXT
    COMMENTS = "comments";

    # TEMPLATE  type: STRING
    TEMPLATE = "template";

    # APPLICATION_NAME  type: STRING
    APPLICATION_NAME = "applicationName";

    # REVISION_NUMBER  type: STRING
    REVISION_NUMBER = "revisionNumber";

    # ACCESS_TIME  type: DATE
    ACCESS_TIME = "accessTime";

    # FILE_ATTRIBUTE  type: STRING
    FILE_ATTRIBUTE = "file_attribute";

    # CHARACTER_COUNT  type: INT
    CHARACTER_COUNT = "characterCount";

    # LAST_SAVED  type: DATE
    LAST_SAVED = "last_saved";

    # LAST_PRINTED  type: DATE
    LAST_PRINTED = "last_printed";

    # LAST_EXPORTED  type: DATE
    LAST_EXPORTED = "last_exported";

    # PAGE_COUNT  type: INT
    PAGE_COUNT = "pageCount";

    # WORD_COUNT  type: INT
    WORD_COUNT = "wordCount";

    # LINE_COUNT  type: INT
    LINE_COUNT = "lineCount";

    # SLIDE_COUNT  type: INT
    SLIDE_COUNT = "slideCount";

    # PARAGRAPH_COUNT  type: INT
    PARAGRAPH_COUNT = "paragraphCount";

    # PRESENTATION_FORMAT  type: STRING
    PRESENTATION_FORMAT = "presentationFormat";

    # CONTENT_STATUS  type: STRING
    CONTENT_STATUS = "contentStatus";

    # CONTENT_TYPE  type: STRING
    CONTENT_TYPE = "contentType";

    # CONTENT_ENCODING  type: STRING
    CONTENT_ENCODING = "contentEncoding";

    # ENCRYPTED  type: PROP
    ENCRYPTED = "encrypted";

    # MAIL_FROM  type: STRING
    MAIL_FROM = "mail_from";

    # MAIL_TO  type: STRING
    MAIL_TO = "mail_to";

    # MAIL_CC  type: STRING
    MAIL_CC = "mail_cc";

    # MAIL_BCC  type: STRING
    MAIL_BCC = "mail_bcc";

    # GROUP  type: STRING
    GROUP = "group";

    # ACL_ALLOW  type: STRING
    ACL_ALLOW = "acl_allow";

    # ACL_DENY  type: STRING
    ACL_DENY = "acl_deny";

    # ACL_AUDIT  type: STRING
    ACL_AUDIT = "acl_audit";

    # ACL_ALARM  type: STRING
    ACL_ALARM = "acl_alarm";

    # PERM  type: STRING
    PERM = "perm";

    # ALLOW_TOKEN_DOCUMENT  type: STRING
    ALLOW_TOKEN_DOCUMENT = "allow_token_document";

    # ALLOW_TOKEN_PARENT  type: STRING
    ALLOW_TOKEN_PARENT = "allow_token_parent";

    # ALLOW_TOKEN_SHARE  type: STRING
    ALLOW_TOKEN_SHARE = "allow_token_share";

    # DENY_TOKEN_DOCUMENT  type: STRING
    DENY_TOKEN_DOCUMENT = "deny_token_document";

    # DENY_TOKEN_PARENT  type: STRING
    DENY_TOKEN_PARENT = "deny_token_parent";

    # DENY_TOKEN_SHARE  type: STRING
    DENY_TOKEN_SHARE = "deny_token_share";

    # GEO  type: STRING
    GEO = "geo";

    # BBOX  type: STRING
    BBOX = "bbox";

    # EXTENT_ASPECT  type: DOUBLE
    EXTENT_ASPECT = "extent_aspect";

    # AREA  type: DOUBLE
    AREA = "area";

    # SPATIAL_REFERENCE  type: STRING
    SPATIAL_REFERENCE = "srs";

    # SPATIAL_CODE  type: STRING
    SPATIAL_CODE = "srs_code";

    # LOCATION  type: STRING
    LOCATION = "location";

    # FEATURE_TYPE  type: STRING
    FEATURE_TYPE = "feature_type";

    # DATASET_TYPE  type: STRING
    DATASET_TYPE = "dataset_type";

    # GEOMETRY_TYPE  type: STRING
    GEOMETRY_TYPE = "geometry_type";

    # COMPRESSION_TYPE  type: STRING
    COMPRESSION_TYPE = "compression_type";

    # SENSOR_TYPE  type: STRING
    SENSOR_TYPE = "sensor_type";

    # PROPERTIES  type: STRING
    PROPERTIES = "properties";

    # DEBUG_PROPERTIES  type: STRING
    DEBUG_PROPERTIES = "debug_properties";

    # METABYTES  type: BYTES
    METABYTES = "metaBytes";

    # METATEXT  type: TEXT
    METATEXT = "metaText";

    # METADATA_STANDARD_NAME  type: STRING
    METADATA_STANDARD_NAME = "metadata_standard_name";

    # METADATA_STANDARD_VERSION  type: STRING
    METADATA_STANDARD_VERSION = "metadata_standard_version";

    # MODIFIED  type: DATE
    MODIFIED = "modified";

    # UPLOADED  type: DATE
    UPLOADED = "uploaded";

    # CREATED  type: DATE
    CREATED = "created";

    # PUBLISHED  type: DATE
    PUBLISHED = "published";

    # DATE_STAMP  type: DATE
    DATE_STAMP = "date_stamp";

    # EVENT_START  type: DATE
    EVENT_START = "event_start";

    # EVENT_END  type: DATE
    EVENT_END = "event_end";

    # CONTACT  type: STRING
    CONTACT = "contact";

    # CONTACT_ROLE  type: STRING
    CONTACT_ROLE = "contact_role";

    # CONTACT_PHONE  type: STRING
    CONTACT_PHONE = "contact_phone";

    # CONTACT_EMAIL  type: STRING
    CONTACT_EMAIL = "contact_email";

    # CONTACT_CITY  type: STRING
    CONTACT_CITY = "contact_city";

    # CONTACT_STATE  type: STRING
    CONTACT_STATE = "contact_state";

    # CONTACT_ZIP  type: STRING
    CONTACT_ZIP = "contact_zip";

    # CONTACT_COUNTRY  type: STRING
    CONTACT_COUNTRY = "contact_country";

    # OWNER  type: STRING
    OWNER = "owner";

    # OWNER_ROLE  type: STRING
    OWNER_ROLE = "owner_role";

    # OWNER_PHONE  type: STRING
    OWNER_PHONE = "owner_phone";

    # OWNER_EMAIL  type: STRING
    OWNER_EMAIL = "owner_email";

    # OWNER_CITY  type: STRING
    OWNER_CITY = "owner_city";

    # OWNER_STATE  type: STRING
    OWNER_STATE = "owner_state";

    # OWNER_ZIP  type: STRING
    OWNER_ZIP = "owner_zip";

    # OWNER_COUNTRY  type: STRING
    OWNER_COUNTRY = "owner_country";

    # PRODUCER  type: STRING
    PRODUCER = "producer";

    # PRODUCER_ROLE  type: STRING
    PRODUCER_ROLE = "producer_role";

    # PRODUCER_PHONE  type: STRING
    PRODUCER_PHONE = "producer_phone";

    # PRODUCER_EMAIL  type: STRING
    PRODUCER_EMAIL = "producer_email";

    # PRODUCER_CITY  type: STRING
    PRODUCER_CITY = "producer_city";

    # PRODUCER_STATE  type: STRING
    PRODUCER_STATE = "producer_state";

    # PRODUCER_ZIP  type: STRING
    PRODUCER_ZIP = "producer_zip";

    # PRODUCER_COUNTRY  type: STRING
    PRODUCER_COUNTRY = "producer_country";

    # INDEXING_WARNING  type: STRING
    INDEXING_WARNING = "indexingWarning";

    # INDEXING_ERROR  type: STRING
    INDEXING_ERROR = "indexingError";

    # INDEXING_ERROR_CODE  type: INT
    INDEXING_ERROR_CODE = "indexingErrorCode";

    # INDEXING_ERROR_TRACE  type: TEXT
    INDEXING_ERROR_TRACE = "indexingErrorTrace";

    # EXTENT_DELTA  type: FLOAT
    EXTENT_DELTA = "extentDelta";

    # DISCOVERY_ID  type: STRING
    DISCOVERY_ID = "_discoveryID";

    # TO_EXTRACT  type: FLAG
    TO_EXTRACT = "__to_extract";

    # RASTER_TYPE  type: STRING
    RASTER_TYPE = "rasterType";

    # PIXEL_TYPE  type: STRING
    PIXEL_TYPE = "pixelType";

    # FILE_EXTENSION  type: LOWER
    FILE_EXTENSION = "fileExtension";

    # PRODUCT  type: STRING
    PRODUCT = "product";

    # SERIES  type: STRING
    SERIES = "series";

    # SCALE  type: STRING
    SCALE = "scale";

    # SOURCE  type: STRING
    SOURCE = "source";

    # NUM_SEGMENTS  type: INT
    NUM_SEGMENTS = "numSegments";

    # START_TIME  type: DATE
    START_TIME = "startTime";

    # END_TIME  type: DATE
    END_TIME = "endTime";

    # DURATION  type: MILLISECONDS
    DURATION = "duration";

    # MD5_HASH  type: STRING
    MD5_HASH = "md5";

    # MD5_HASH_TIME  type: MILLISECONDS
    MD5_HASH_TIME = "md5_time";

    # SCHEMA_HASH  type: STRING
    SCHEMA_HASH = "schemaHash";

    # CONTENT_HASH  type: STRING
    CONTENT_HASH = "contentHash";

    # TRANSPARENCY  type: INT
    TRANSPARENCY = "transparency";

    # RASTER_SIZE  type: STRING
    RASTER_SIZE = "rasterSize";

    # BRIGHTNESS  type: INT
    BRIGHTNESS = "brightness";

    # CONTRAST  type: INT
    CONTRAST = "contrast";

    # MAX_SCALE  type: INT
    MAX_SCALE = "maxScale";

    # MIN_SCALE  type: INT
    MIN_SCALE = "minScale";

    # MAX_WIDTH  type: INT
    MAX_WIDTH = "maxWidth";

    # MAX_HEIGHT  type: INT
    MAX_HEIGHT = "maxHeight";

    # LAYER_LIMIT  type: INT
    LAYER_LIMIT = "layerLimit";

    # PIX_WIDTH  type: FLOAT
    PIX_WIDTH = "pixWidth";

    # PIX_HEIGHT  type: FLOAT
    PIX_HEIGHT = "pixHeight";

    # PIX_DEPTH  type: FLOAT
    PIX_DEPTH = "pixDepth";

    # DRIVER  type: STRING
    DRIVER = "driver";

    # NUM_BANDS  type: INT
    NUM_BANDS = "numBands";

    # ROW_COUNT  type: INT
    ROW_COUNT = "rowCount";

    # COLUMN_COUNT  type: INT
    COLUMN_COUNT = "columnCount";

    # ITEM_COUNT  type: INT
    ITEM_COUNT = "item_count";

    # OBJECT_COUNT  type: INT
    OBJECT_COUNT = "object_count";

    # TABLE_JOIN_COUNT  type: INT
    TABLE_JOIN_COUNT = "tableJoinCount";

    # HOT_LINK_FIELD  type: STRING
    HOT_LINK_FIELD = "hot_link_field";

    # WORKER  type: STRING
    WORKER = "worker";

    # EXTRACTOR  type: STRING
    EXTRACTOR = "extractor";

    # HAS_EXIF  type: PROP
    HAS_EXIF = "has_EXIF";

    # HAS_NITF  type: PROP
    HAS_NITF = "has_NITF";

    # HAS_XMP  type: PROP
    HAS_XMP = "has_XMP";

    # HAS_M  type: PROP
    HAS_M = "has_M";

    # HAS_Z  type: PROP
    HAS_Z = "has_Z";

    # HAS_PYRAMIDS  type: PROP
    HAS_PYRAMIDS = "has_pyramids";

    # HAS_STATISTICS  type: PROP
    HAS_STATISTICS = "has_statistics";

    # HAS_COLORMAP  type: PROP
    HAS_COLORMAP = "has_colormap";

    # GEOTAGGED_FROM_METADATA  type: PROP
    GEOTAGGED_FROM_METADATA = "geotagged_from_metadata";

    # GEOTAGGED_FROM_METADATA_SOURCE  type: STRING
    GEOTAGGED_FROM_METADATA_SOURCE = "geotagged_from_metadata_source";

    # AVG_NUM_POINTS_PER_FEATURE  type: INT
    AVG_NUM_POINTS_PER_FEATURE = "avg_num_points_per_feature";

    # SCHEMA  type: STRING
    SCHEMA = "schema";

    # FIELD_NAME  type: STRING
    FIELD_NAME = "field_name";

    # FIELD_HAS_ALIAS  type: PROP
    FIELD_HAS_ALIAS = "field_has_alias";

    # FIELD_ALIAS  type: STRING
    FIELD_ALIAS = "field_alias";

    # FIELD_TYPE  type: STRING
    FIELD_TYPE = "field_type";

    # FIELD_COUNT  type: INT
    FIELD_COUNT = "field_count";

    # BYTES  type: BYTES
    BYTES = "bytes";

    # BYTES_UNCOMPRESSED  type: BYTES
    BYTES_UNCOMPRESSED = "bytes_uncompressed";

    # COMPRESSION_RATIO  type: FLOAT
    COMPRESSION_RATIO = "compression_ratio";

    # RATING  type: DOUBLE
    RATING = "rating";

    # FEATURED  type: STRING
    FEATURED = "tag_flags";

    # HIGHLIGHT  type: STRING
    HIGHLIGHT = "highlight";

    # lat  type: DOUBLE
    GEO_LAT = "lat";

    # lon  type: DOUBLE
    GEO_LON = "lon";

    # altitude  type: DOUBLE
    GEO_ALTITUDE = "altitude";

    # xmin  type: DOUBLE
    GEO_XMIN = "xmin";

    # ymin  type: DOUBLE
    GEO_YMIN = "ymin";

    # zmin  type: DOUBLE
    GEO_ZMIN = "zmin";

    # xmax  type: DOUBLE
    GEO_XMAX = "xmax";

    # ymax  type: DOUBLE
    GEO_YMAX = "ymax";

    # zmax  type: DOUBLE
    GEO_ZMAX = "zmax";

    # raw  type: STRING
    GEO_RAW = "raw";

    # wkt  type: STRING
    GEO_WKT = "wkt";

    # code  type: STRING
    GEO_CODE = "code";

    # spatialReference  type: STRING
    GEO_SPATIALREFERENCE = "spatialReference";

    # area  type: DOUBLE
    GEO_AREA = "area";

    # inWGS84  type: BOOLEAN
    GEO_INWGS84 = "inWGS84";

