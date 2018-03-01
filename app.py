"""
dos-indexd-lambda
This lambda proxies requests to the necessary indexd endpoints and converts them
to GA4GH messages.

"""

import requests
import yaml
import urlparse
import logging

from chalice import Chalice, Response

DSS_URL = "https://commons-dss.ucsc-cgp-dev.org/v1"

app = Chalice(app_name='dos-indexd-lambda', debug=True)
app.log.setLevel(logging.DEBUG)


def dss_file_to_dos(data_object_id, dss_file):
    """
    Converts a DSS file header into a Data Object.

    List of headers

    ['Content-Type', 'Content-Length', 'Connection', 'Date',
     'x-amzn-RequestId', 'X-DSS-SHA1',
     'Access-Control-Allow-Origin', 'X-DSS-S3-ETAG',
     'X-DSS-SHA256', 'X-DSS-BUNDLE-UUID',
     'Access-Control-Allow-Headers', 'X-DSS-CONTENT-TYPE',
     'X-DSS-CRC32C', 'X-DSS-CREATOR-UID', 'X-DSS-VERSION',
     'X-Amzn-Trace-Id', 'X-DSS-SIZE', 'X-Cache', 'Via',
     'X-Amz-Cf-Id']

    :param data_object_id:
    :param dss_file:
    :return:
    """

    data_object = {}
    data_object['id'] = data_object_id
    sha256 = {'checksum': dss_file.get('X-DSS-SHA256', None), 'type': 'sha256'}
    etag = {'checksum': dss_file.get('X-DSS-S3-ETAG', None), 'type': 'etag'}
    sha1 = {'checksum': dss_file.get('X-DSS-SHA1', None), 'type': 'sha1'}
    crc32c = {'checksum': dss_file.get('X-DSS-CRC32C', None), 'type': 'crc32c'}
    checksums = [sha256, etag, sha1, crc32c]
    data_object['checksums'] = checksums
    data_object['version'] = dss_file.get('X-DSS-VERSION', None)
    data_object['content_type'] = dss_file.get('X-DSS-CONTENT-TYPE', None)
    data_object['urls'] = make_urls(data_object_id, 'files')
    return data_object


def dss_list_bundle_to_dos(dss_bundle):
    """
    Converts a DSS bundle to DOS bundle messages by splitting the ID.

    :param bundle_list:
    :return:
    """
    dos_bundle = {}
    dos_bundle['id'] = dss_bundle['bundle_fqid'].split('.')[0]
    dos_bundle['version'] = dss_bundle['bundle_fqid'].split('.')[1]
    # full_bundle = requests.get(
    #     "{}/bundles/{}?replica=aws&version={}".format(
    #         DSS_URL, dos_bundle['id'], dos_bundle['version'])).json()
    # if full_bundle and full_bundle.get('files', None):
    #     for file in full_bundle.get('files'):
    #         dos_bundle['data_object_ids'].append(file['uuid'])
    return dos_bundle

def dss_bundle_to_dos(dss_bundle):
    """
    Converts a fully formatted DSS bundle into a DOS bundle.

    :param dss_bundle:
    :return:
    """
    dos_bundle = {}
    dos_bundle['id'] = dss_bundle['uuid']
    dos_bundle['version'] = dss_bundle['version']
    dos_bundle['data_object_ids'] = [x['uuid'] for x in dss_bundle['files']]
    return dos_bundle


@app.route('/swagger.json', cors=True)
def swagger():
    """
    An endpoint for returning the swagger api description.

    :return:
    """
    # FIXME replace with one hosted here
    req = requests.get("https://ga4gh.github.io/data-object-service-schemas/swagger/data_object_service.swagger.yaml")
    swagger_dict = yaml.load(req.content)

    swagger_dict['basePath'] = '/api/ga4gh/dos/v1'
    return swagger_dict


def make_urls(object_id, path):
    """
    Makes a list of URLs for each replica for a DOS message.
    :param object_id:
    :param path:
    :return:
    """
    replicas = ['aws', 'azure', 'gcp']
    urls = map(
        lambda replica: {'url' : '{}/{}/{}?replica={}'.format(
            DSS_URL, path, object_id, replica)},
        replicas)
    return urls


def convert_reference_json(reference_json, data_object):
    """
    Converts the reference JSON download from DSS into the DOS message
    and returns the DOS message.
    :param reference_json:
    :param data_object:
    :return:
    """
    # {u'content-type': u'application/octet-stream',
    # u'crc32c': u'e2a2bc04',
    # u'size': 25955827488,
    # u'url': [u'gs://topmed-irc-share/genomes/NWD145710.b38.irc.v1.cram',
    # u's3://nih-nhlbi-datacommons/NWD145710.b38.irc.v1.cram']}
    data_object['size'] = reference_json['size']
    data_object['urls'] = map(lambda x: {'url': x}, reference_json['url'])
    data_object['checksums'] = [{'checksum': reference_json['crc32c'], 'type': 'crc32c'}]
    data_object['content_type'] = reference_json['content-type']
    return data_object

@app.route('/ga4gh/dos/v1/dataobjects/{data_object_id}', methods=['GET'], cors=True)
def get_data_object(data_object_id):
    """
    This endpoint returns DataObjects by their identifier by proxying the
    request to files in DSS.
    :param data_object_id:
    :return:
    """
    dss_response = requests.head("{}/files/{}?replica=aws".format(DSS_URL, data_object_id))
    dss_file = dss_response.headers
    if not dss_response.status_code == 200:
        return Response({'msg': 'Data Object with data_object_id {} was not found.'.format(data_object_id)}, status_code=404)
    data_object = dss_file_to_dos(data_object_id, dss_file)
    # FIXME download the extra metadata if its a file by reference
    content_key = 'fileref'
    if data_object['content_type'].find(content_key) != -1:
        reference_json = requests.get('{}/files/{}?replica=aws'.format(DSS_URL, data_object_id)).json()
        try:
            data_object = convert_reference_json(reference_json, data_object)
        except Exception as e:
            return Response({'msg': 'Data Object with data_object_id {} was not found. {}'.format(data_object_id, str(e))},
                            status_code=404)
    else:
        replicas = ['aws', 'azure', 'gcp']
        for replica in replicas:
            # TODO make async
            try:
                data_bundle = requests.get(
                    "{}/bundles/{}?replica={}&directurls=true".format(DSS_URL, dss_file['X-DSS-BUNDLE-UUID'], replica))
                url = filter(lambda x: x['uuid'] == data_object_id, data_bundle.json()['bundle']['files'])[0]['url']
                data_object['urls'].append({'url': url})
            except Exception as e:
                pass
    return {'data_object': data_object}

@app.route('/ga4gh/dos/v1/dataobjects/list', methods=['POST'], cors=True)
def list_data_objects():
    """
    This endpoint translates DOS List requests into requests against DSS
    and converts the responses into GA4GH messages.

    :return:
    """
    return Response(status_code=405)


@app.route('/ga4gh/dos/v1/databundles/list', methods=['POST'], cors=True)
def list_data_bundles():
    """
    This endpoint translates DOS List requests into requests against DSS
    and converts the responses into GA4GH messages.

    :return:
    """
    req_body = app.current_request.json_body
    per_page = 10
    page_token = None
    next_page_token = None
    if req_body and (req_body.get('page_size', None)):
        per_page = req_body.get('page_size')
    if req_body and (req_body.get('page_token', None)):
        page_token = req_body.get('page_token')
    if page_token:
        res = requests.post(
            "{}/search?replica=aws&per_page={}&_scroll_id={}".format(
                DSS_URL, per_page, page_token), json={'es_query': {}})
    else:
        res = requests.post(
            "{}/search?replica=aws&per_page={}".format(
                DSS_URL, per_page), json={'es_query': {}})
    # We need to page using the github style
    if res.links.get('next', None):
        try:
            # first _scroll_id item of the query string in the link
            # header of the response
            next_page_token = urlparse.parse_qs(
                urlparse.urlparse(
                    res.links['next']['url']).query)['_scroll_id'][0]
        except Exception as e:
            print(e)
    # And convert the fqid message into a DOS id and version
    response = {}
    response['next_page_token'] = next_page_token
    try:
        response['data_bundles'] = map(dss_list_bundle_to_dos, res.json()['results'])
    except Exception as e:
        response = e
    finally:
        return response

@app.route('/ga4gh/dos/v1/databundles/{data_bundle_id}', methods=['GET'], cors=True)
def get_data_bundle(data_bundle_id):
    """
    This endpoint translates DOS List requests into requests against DSS
    and converts the responses into GA4GH messages.

    :return:
    """
    version = None
    if app.current_request.query_params:
        version = app.current_request.query_params.get('version', None)
    if version:
        res = requests.get("{}/bundles/{}?replica=aws&version={}".format(
            DSS_URL, data_bundle_id, version)).json()
    else:
        res = requests.get(
            "{}/bundles/{}?replica=aws".format(DSS_URL, data_bundle_id)).json()
    return {'data_bundle': dss_bundle_to_dos(res['bundle'])}



@app.route('/')
def index():
    message = "<h1>Welcome to the DOS lambda, send requests to /ga4gh/dos/v1/</h1>"
    return Response(body=message,
                    status_code=200,
                    headers={'Content-Type': 'text/html'})