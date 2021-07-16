__all__ = ['to_zarr']

import logging
import os.path as op
import requests
import xml.etree.ElementTree as ElementTree

logger = logging.getLogger(__name__)

# Environment variables

""" Namespaces used in DMRPP XML Files """
NS = {
    'dpp': 'http://xml.opendap.org/dap/dmrpp/1.0.0#',
    'd': 'http://xml.opendap.org/ns/DAP/4.0#'
}

""" Default compression level """
UNKNOWN_COMPRESSION_LEVEL = 4

""" Data type mappings """
TYPE_INFO = {
    'Int8': (int, '|i1'),
    'Int16': (int, '<i2'),
    'Int32': (int, '<i4'),
    'Int64': (int, '<i8'),
    'Byte': (int, '|u1'),
    'UInt8': (int, '|u1'),
    'UInt16': (int, '<u2'),
    'UInt32': (int, '<u4'),
    'UInt64': (int, '<u8'),
    'Float32': (float, '<f4'),
    'Float64': (float, '<f8'),
    'String': (str, '|s'),
    'URI': (str, '|s')
}


def find_child(node, name):
    """Return child node with matching name (this function primarily used for testing)

    Args:
        node (XML Element): XML Node to search children
        name (string): Name of child

    Returns:
        XML Element: XML Child Element
    """
    return node.find(".//d:*[@name='%s']" % (name), NS)


def get_attribute_values(node):
    """Get value for a node

    Args:
        node (XML Element): An XML Element, presumably of Attribute type

    Returns:
        str or [str]: Single value or a list
    """
    t = TYPE_INFO[node.attrib['type']][0]
    vals = [t(val.text) for val in node]
    return vals[0] if len(vals) == 1 else vals


def get_attributes(node, exclude=[]):
    """Get all children from a node that are Attributes

    Args:
        node (XML Element): An XML Element containing Attribute children
        exclude (list[str], optional): List of attribute names to exclude. Defaults to [].

    Returns:
        dict: Dictionary of Atribute values
    """
    zattrs = {}
    for child in node :
        tag = child.tag.split('}')[-1]
        if tag == 'Attribute' and child.attrib['name'] not in exclude:
            zattrs[child.attrib['name']] = get_attribute_values(child)
    return zattrs


def get_dimensions(root, group=None):
    """Get dictionary of dimension info from the root of the DMRPP XML

    Args:
        root (XML Element): XML Element for the DMRPP root
        group (str, optional): Group name to get dimensions from

    Returns:
        dict: Dictionary containing dimension names, sizes, and full paths
    """
    #, group=None): #, path='/'):
    if group is None:
        group = root

    #dimensions = {}
    dim_infos = { '/' + dim.attrib['name']: {'size': int(dim.attrib['size'])} for dim in group.findall('d:Dimension', NS)}
    for name in dim_infos:
        basename = name.split('/')[-1]
        dim_node = root.find(".//d:*[@name='%s']/d:Dim[@name='%s']/.." % (basename, name), NS)
        if dim_node is None:
            logger.warning(f"Could not find details for dimension {name}")
            continue
        #result = node.find(f"./d:Attribute[@name='{name}']/d:Value", NS)
        #return result.text.lstrip('/')
        node = dim_node.find(f"./d:Attribute[@name='fullnamepath']/d:Value", NS)
        if node:
            dim_infos[name]['path'] = node.text
        else:
            dim_infos[name]['path'] = name

    # TODO - HARMONY-530, don't think this works as originally intended. Need test files with nested groups
    #for child in group.findall('d:Group', NS):
    #    dim_infos.update(get_dimensions(root, child)) #, path + child.attrib['name'] + '/'))
    return dim_infos


def chunks_to_zarr(node):
    """Convert DMRPP 'Chunks' Element into Zarr metadata

    Args:
        node (XML Element): XML Element of type dmrpp:chunks

    Returns:
        dict: Zarr metadata for chunks
    """
    chunks = None
    zarray = {}
    zchunkstore = {}
    for child in node:
        tag = child.tag.split('}')[-1]
        if tag == 'chunkDimensionSizes':
            chunks = [int(v) for v in child.text.split(' ')]
        elif tag == 'chunk':
            offset = int(child.attrib['offset'])
            nbytes = int(child.attrib['nBytes'])
            positions_in_array = child.get('chunkPositionInArray')
            if positions_in_array:
                positions_str = positions_in_array[1:-1].split(',')
                positions = [int(p) for p in positions_str]
                indexes = [ int(p / c) for p, c in zip(positions, chunks) ]
            else:
                indexes = [0]
            key = '.'.join([ str(i) for i in indexes ])
            zchunkstore[key] = { 'offset': offset, 'size': nbytes }
    zarray['chunks'] = chunks
    return {
        'zarray': zarray,
        'zchunkstore': zchunkstore
    }


def array_to_zarr(node, dims, prefix=''):
    """Convert a DMRPP Array into Zarr metadata

    Args:
        node (XML Element): XML Element of a DMRPP array
        dims (dict): Dimension info from DMRPP XML root
        prefix (str, optional): Prefix to prepend to array in Zarr metadata. Defaults to ''.

    Raises:
        Exception: Unrecognized compression type

    Returns:
        dict: Zarr metadata for this DMRPP array
    """
    datatype = node.tag.split('}')[-1]
    dtype = TYPE_INFO[datatype][1]
    pathnode = node.find(f"./d:Attribute[@name='fullnamepath']/d:Value", NS)
    if pathnode is not None:
        prefix = op.join(prefix, pathnode.text).lstrip('/')
    else:
        prefix = op.join(prefix, node.attrib['name']).lstrip('/')

    zarray = {
        "zarr_format": 2,
        "filters": None,
        "order": "C",
        "dtype": dtype,
        "shape": []
    }
    zattrs = get_attributes(node, exclude=['fullnamepath', 'origname'])
    zattrs.update({
        "_ARRAY_DIMENSIONS": []
    })
    zchunkstore = None

    for child in node:
        tag = child.tag.split('}')[-1]
        if tag == 'Dim' and 'name' in child.attrib:
            dim = dims[child.attrib['name']]
            zattrs['_ARRAY_DIMENSIONS'].append(child.attrib['name'].lstrip('/'))
            zarray['shape'].append(dim['size'])
        elif tag == 'Dim':
            # anonymous Dimensions still have size
            zarray['shape'].append(int(child.attrib['size']))
        elif tag == 'chunks':
            compression = child.attrib.get('compressionType')
            if compression == 'deflate':
                zarray['compressor'] = { "id": "zlib", "level": UNKNOWN_COMPRESSION_LEVEL }
            elif compression == 'deflate shuffle':
                zarray['compressor'] = {"id": "zlib", "level": UNKNOWN_COMPRESSION_LEVEL}
                size = int(dtype[2:])
                zarray['filters'] = [{"id": "shuffle", "elementsize": size}]
            elif compression is None:
                zarray['compressor'] = None
            else:
                raise Exception('Unrecognized compressionType: ' + compression)
            chunks = chunks_to_zarr(child)
            zarray.update(chunks['zarray'])
            zchunkstore = chunks['zchunkstore']
    # NOTE - this is null in test file
    zarray['fill_value'] = zattrs.get('_FillValue')

    # HARMONY-896: Automatic scale factor and offset filter.  Not yet working with all data types
    # if zattrs.get('scale_factor') or zattrs.get('add_offset'):
    #     zarray['filters'].append({
    #         'id': 'fixedscaleoffset',
    #         'offset': zattrs.get('add_offset', 0.0),
    #         'scale': zattrs.get('scale_factor', 1.0),
    #         'dtype': '<f8',
    #     })

    if zarray.get('chunks') is None:
        zarray['chunks'] = zarray['shape']

    zarr = {
        op.join(prefix, '.zarray'): zarray,
        op.join(prefix, '.zattrs'): zattrs,
        op.join(prefix, '.zchunkstore'): zchunkstore
    }
    return zarr


def group_to_zarr(node, dims, prefix=''):
    """Convert DMRPP grouping into a Zarr group

    Args:
        node (XML Element): XML Element representing DMRPP group
        dims (dict): Dimension info retrieved from DMRPP root XML
        prefix (str, optional): Prefix to prepend to Zarr metadata keys. Defaults to ''.

    Returns:
        dict: Zarr metadata
    """
    zarr = {}
    if prefix == '':
        zarr['.zgroup'] = {
            'zarr_format': 2
        }

    for child in node:
        tag = child.tag.split('}')[-1]
        # if this is an array, convert to zarr array
        if tag in TYPE_INFO:
            zarr_array = array_to_zarr(child, dims, prefix=prefix)
            zarr.update(zarr_array)
        # otherwise, if this is group or a Container Attribute - this has not been tested
        elif tag == 'Group' or (tag == 'Attribute' and child.attrib.get('type', '') == 'Container'):
            name = child.attrib['name']
            # use for global .zattrs
            if name == 'HDF5_GLOBAL':
                zarr['.zattrs'] = get_attributes(child)
            elif name != 'DODS_EXTRA' and len(child):
                zarr_child = group_to_zarr(child, dims, prefix=op.join(prefix, name))
                zarr.update(zarr_child)
        # if attribute
        elif tag == 'Attribute':
            # put at current level
            key = op.join(prefix, '.zattrs')
            if key not in zarr:
                zarr[key] = {}
            zarr[key][child.attrib['name']] = get_attribute_values(child)
    return zarr


def to_zarr(root):
    """Convert DMRPP metadata to Zarr metadata

    Args:
        root (XML Element): Root XML Element of DMRPP XML

    Returns:
        dict: Zarr metadata
    """
    zarr = {}
    dims = get_dimensions(root)
    zarr = group_to_zarr(root, dims)
    return zarr

