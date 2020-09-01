ns = {'dpp': 'http://xml.opendap.org/dap/dmrpp/1.0.0#', 'd': 'http://xml.opendap.org/ns/DAP/4.0#'}

UNKNOWN_COMPRESSION_LEVEL = 4

type_info = {
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

def build_groups(fullname, zarr):
    parts = fullname.split('/')
    while len(parts) > 0:
        parts.pop()
        key = '/'.join(parts) + '/.zgroup'
        if key != '/.zgroup' and not key in zarr:
            zarr[key] = { 'zarr_format': 2 }

def get_fullname(node, default, zarr):
    fullname_node = node.find("./d:Attribute[@name='fullnamepath']/d:Value", ns)
    if fullname_node is None:
        result = default.lstrip('/')
    else:
        result = fullname_node.text.lstrip('/')
    if zarr:
        build_groups(result, zarr)
    return result

def array_to_zarr(dmr, zarr, dims):
    datatype = dmr.tag.split('}')[-1]
    dtype = type_info[datatype][1]
    zarray = { "zarr_format": 2, "filters": None, "order": "C", "dtype": dtype }
    zattrs = {}
    prefix = get_fullname(dmr, dmr.attrib['name'], zarr) + '/'
    zarr[prefix + '.zarray'] = zarray
    zarr[prefix + '.zattrs'] = zattrs

    for child in dmr:
        tag = child.tag.split('}')[-1]
        if tag == 'Dim':
            if 'name' in child.attrib:
                dim = dims[child.attrib['name']]
                if '_ARRAY_DIMENSIONS' not in zattrs:
                    zattrs['_ARRAY_DIMENSIONS'] = []
                zattrs['_ARRAY_DIMENSIONS'].append(dim['path'])
            else:
                dim = { 'size': int(child.attrib['size']) }
            if 'shape' not in zarray:
                zarray['shape'] = []
            zarray['shape'].append(dim['size'])

        elif tag == 'Attribute':
            name = child.attrib['name']
            if name != 'fullnamepath' and name != 'origname':
                attribute_to_zarr(prefix, child, zarr)
        elif tag == 'chunks':
            compression = child.attrib.get('compressionType')
            if compression == 'deflate':
                zarray['compressor'] = { "id": "zlib", "level": UNKNOWN_COMPRESSION_LEVEL }
            elif not compression:
                zarray['compressor'] = None
            else:
                raise Exception('Unrecognized compressionType: ' + compression)
            chunks_to_zarr(prefix, child, zarr, zarray)
    zarray['fill_value'] = zattrs.get('_FillValue')

def get_attribute_values(dmr):
    t = type_info[dmr.attrib['type']][0]
    vals = [t(val.text) for val in dmr]
    return vals[0] if len(vals) == 1 else vals

def attribute_to_zarr(prefix, dmr, zarr):
    key = prefix + '.zattrs'
    if key not in zarr:
        zarr[key] = {}
    zarr[key][dmr.attrib['name']] = get_attribute_values(dmr)

def chunks_to_zarr(prefix, dmr, zarr, zarray):
    chunks = None
    zchunkstore = zarr[prefix + '.zchunkstore'] = {}
    for child in dmr:
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
    if chunks:
        zarray['chunks'] = chunks
    elif 'shape' in zarray:
        zarray['chunks'] = zarray['shape']

def group_to_zarr(prefix, dmr, zarr, dims):
    zarr[prefix + '.zgroup'] = { 'zarr_format': 2 }
    for child in dmr:
        tag = child.tag.split('}')[-1]
        if tag in type_info:
            name = child.attrib['name']
            array_to_zarr(child, zarr, dims)
        elif tag == 'Group' or (tag == 'Attribute' and child.attrib['type'] == 'Container'):
            name = child.attrib['name'] + '/'
            if name == 'HDF5_GLOBAL/':
                name = ''
            if name != 'DODS_EXTRA/' and len(child):
                fullname = get_fullname(child, prefix + name, zarr)
                group_to_zarr(fullname, child, zarr, dims)
        elif tag == 'Attribute':
            attribute_to_zarr(prefix, child, zarr)

def parse_dims(root, group=None, path='/'):
    if group is None:
        group = root
    dim_infos = { '/' + dim.attrib['name']: {'size': int(dim.attrib['size'])} for dim in group.findall('d:Dimension', ns)}
    for name in dim_infos:
        basename = name.split('/')[-1]
        dim_node = root.find(".//d:*[@name='%s']/d:Dim[@name='%s']/.." % (basename, name), ns)

        if dim_node is None:
            print('Could not find details for dimension', name)
        else:
            dim_infos[name]['path'] = get_fullname(dim_node, name, None)

    for child in group.findall('d:Group', ns):
        dim_infos.update(parse_dims(root, child, path + child.attrib['name'] + '/'))
    return dim_infos

def dmrpp_to_zarr(root):
    zarr = {}
    dims = parse_dims(root)
    group_to_zarr('', root, zarr, dims)
    return zarr
