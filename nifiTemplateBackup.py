# https://community.hortonworks.com/articles/87160/creating-nifi-template-via-rest-api.html
# https://community.hortonworks.com/questions/85475/nifi-createbackup-template-with-nifi-api.html

# this code creates a template from the top level of a NiFi project, then downloads it and
# writes it out in a compressed format

import contextlib
import OpenSSL.crypto
import requests
import tempfile
import json
import datetime
import gzip
import argparse


@contextlib.contextmanager
def pfx_to_pem(pfx_path, pfx_password):
    """ Decrypts the .pfx file to be used with requests.
    https://gist.github.com/erikbern/756b1d8df2d1487497d29b90e81f8068 """
    with tempfile.NamedTemporaryFile(suffix='.pem') as t_pem:
        f_pem = open(t_pem.name, 'wb')
        pfx = open(pfx_path, 'rb').read()
        p12 = OpenSSL.crypto.load_pkcs12(pfx, pfx_password)
        f_pem.write(OpenSSL.crypto.dump_privatekey(OpenSSL.crypto.FILETYPE_PEM, p12.get_privatekey()))
        f_pem.write(OpenSSL.crypto.dump_certificate(OpenSSL.crypto.FILETYPE_PEM, p12.get_certificate()))
        ca = p12.get_ca_certificates()
        if ca is not None:
            for cert in ca:
                f_pem.write(OpenSSL.crypto.dump_certificate(OpenSSL.crypto.FILETYPE_PEM, cert))
        f_pem.close()
        yield t_pem.name


def build_snippet_payload(root, client, groupInfo):
    # these are all the things the snippet can include
    payload = {
        "snippet": {
            "parentGroupId": root,
            "processors": {},
            "funnels": {},
            "inputPorts": {},
            "outputPorts": {},
            "remoteProcessGroups": {},
            "processGroups": {},
            "connections": {},
            "labels": {}
            }
        }

    # loop over the categories of flow information and add the names, versions to the payload (we want everything)
    for category in groupInfo["processGroupFlow"]["flow"]:
        for item in groupInfo["processGroupFlow"]["flow"][category]:
            payload["snippet"][category][item["id"]] = \
                {
                    "clientId": client,
                    "version": item["revision"]["version"]
                }

    return payload


def main(savePath, nifiAddress, clientCert, clientPass, caBundle):
    with pfx_to_pem(clientCert, clientPass) as cert:
        s = requests.Session()
        s.cert = cert

        # query for root (top level) flow info
        rootFlowId = json.loads(
            s.get("https://{0}/nifi-api/process-groups/root".format(nifiAddress), verify=caBundle).text
        )["id"]

        # query for client id
        clientId = s.get("https://{0}/nifi-api/flow/client-id".format(nifiAddress), verify=caBundle).text

        # query for process group information
        processGroup = json.loads(
            s.get("https://{0}/nifi-api/flow/process-groups/{1}".format(nifiAddress, rootFlowId), verify=caBundle).text
        )

        # create the payload to build the snippet with
        snippetPayload = build_snippet_payload(rootFlowId, clientId, processGroup)

        # create the snippet
        snippetId = json.loads(
            s.post("https://{0}/nifi-api/snippets".format(nifiAddress), json=snippetPayload, verify=caBundle).text
        )["snippet"]["id"]

        # create the template
        now = datetime.datetime.now()
        templateCreationPayload = {
            "name": "python generated ({0}) - {1}".format(nifiAddress, now.strftime('%Y-%m-%d %H:%M:%S')),
            "description": "",
            "snippetId": snippetId
        }

        try:
            templateCreationResponse = s.post(
                "https://{0}/nifi-api/process-groups/{1}/templates".format(nifiAddress, rootFlowId),
                json=templateCreationPayload,
                verify=caBundle
            )
            
            # grab id of newly created template
            templateId = json.loads(templateCreationResponse.text)["template"]["id"]

        except json.decoder.JSONDecodeError:
            print(templateCreationResponse.text)
            exit(1)
        
        # download the template
        template = s.get("https://{0}/nifi-api/templates/{1}/download".format(nifiAddress, templateId)).content

        # write it out compressed, templates are XML so they compress very well
        with gzip.open(savePath, "wb") as f:
            f.write(template)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--save-path", help="full path and file name of where to save the compressed (.gz) template", required=True)
    parser.add_argument("--nifi-address", help="FQDN of the NiFi server, with port (my-nifi.my-domain.com:9443)", required=True)
    parser.add_argument("--client-cert", help="path to the client certificate (.p12) that allows access to NiFi", required=True)
    parser.add_argument("--client-pass", help="pass phrase (import password) for the client cert", required=True)
    parser.add_argument("--ca-bundle", help="path to pem file containing the certificate chain of the NiFi server", required=True)
    args = parser.parse_args()

    main(args.save_path, args.nifi_address, args.client_cert, args.client_pass, args.ca_bundle)
