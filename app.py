"""
dos-indexd-lambda
This lambda proxies requests to the necessary indexd endpoints and converts them
to GA4GH messages.

"""

import requests
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
    req = requests.get("https://gist.githubusercontent.com/david4096/6dad2ea6a4ebcff8e0fe24c2210ae8ef/raw/55bf72546923c7bd9f63f3ea72d7441b0a506a76/data_object_service.gdc.swagger.json")
    swagger_dict = req.json()
    swagger_dict['basePath'] = '/api'
    return swagger_dict


def make_urls(object_id, path):
    """
    Makes a list of URLs for each replica for a DOS message.
    :param object_id:
    :param path:
    :return:
    """
    replicas = ['aws', 'azure']
    urls = map(
        lambda replica: {'url' : '{}/{}/{}?replica={}'.format(
            DSS_URL, path, object_id, replica)},
        replicas)
    return urls

@app.route('/ga4gh/dos/v1/dataobjects/{data_object_id}', methods=['GET'], cors=True)
def get_data_object(data_object_id):
    """
    This endpoint returns DataObjects by their identifier by proxying the
    request to files in DSS.
    :param data_object_id:
    :return:
    """
    dss_file = requests.head("{}/files/{}?replica=aws".format(DSS_URL, data_object_id)).headers
    data_object = dss_file_to_dos(data_object_id, dss_file)
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