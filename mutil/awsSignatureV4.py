#!/usr/bin/env python3

# This is a modified version of the zero-dependency signature algorithm from:
# https://docs.aws.amazon.com/general/latest/gr/sigv4-signed-request-examples.html
# (captured and modified/improved December 2020)

# Copyright 2010-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# This file is licensed under the Apache License, Version 2.0 (the "License").
# You may not use this file except in compliance with the License. A copy of the
# License is located at
#
# http://aws.amazon.com/apache2.0/
#
# This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS
# OF ANY KIND, either express or implied. See the License for the specific
# language governing permissions and limitations under the License.
#
# ABOUT THIS PYTHON SAMPLE: This sample is part of the AWS General Reference
# Signing AWS API Requests top available at
# https://docs.aws.amazon.com/general/latest/gr/sigv4-signed-request-examples.html
#

# AWS Version 4 signing example

# EC2 API (DescribeRegions)

# See: http://docs.aws.amazon.com/general/latest/gr/sigv4_signing.html
# This version makes a GET request and passes the signature
# in the Authorization header.
import urllib.parse
from dataclasses import dataclass
import sys, os, base64, datetime, hashlib, hmac
from typing import Any, Dict


@dataclass
class SignatureV4:
    access_key: str
    secret_key: str
    host: str  # full host: my-bucket.s3.us-west-001.amazonaws.com

    def __post_init__(self) -> None:
        # automatically de-construct bucket host to get all components
        # host looks like: [bucket].[service].[region].[company]
        parts = self.host.split(".")
        self.bucket = parts[0]  # bucket name
        self.service = parts[1]  # s3
        self.region = parts[2]  # us-west-2
        self.endpoint = ".".join(parts[1:])  # s3.us-west-2.amazonaws.com

        # use empty sha256 hash for body of GET requests
        self.empty_payload = hashlib.sha256("".encode()).hexdigest()

    # Key derivation functions. See:
    # http://docs.aws.amazon.com/general/latest/gr/signature-v4-examples.html#signature-v4-examples-python
    def sign(self, key: bytes, msg: str) -> bytes:
        return hmac.new(key, msg.encode(), hashlib.sha256).digest()

    def getSignatureKey(
        self, key: str, dateStamp: str, regionName: str, serviceName: str
    ) -> bytes:
        kDate = self.sign(("AWS4" + key).encode(), dateStamp)
        kRegion = self.sign(kDate, regionName)
        kService = self.sign(kRegion, serviceName)
        kSigning = self.sign(kService, "aws4_request")
        return kSigning

    def getHeaders(self, method: str, uri: str) -> Dict[str, str]:
        # Read AWS access key from env. variables or configuration file. Best practice is NOT
        # to embed credentials in code.

        # Create a date for headers and the credential string
        # TODO: we could cache headers every day by making another method
        #       getHeadersForDay(datestamp, method, uri) then putting a cache
        #       decorator on it. We just need to update the signatures once per day.
        t = datetime.datetime.utcnow()
        amzdate = t.strftime("%Y%m%dT%H%M%SZ")
        datestamp = t.strftime("%Y%m%d")  # Date w/o time, used in credential scope

        # ************* TASK 1: CREATE A CANONICAL REQUEST *************
        # http://docs.aws.amazon.com/general/latest/gr/sigv4-create-canonical-request.html

        # Step 1 is to define the verb (GET, POST, etc.)--already done.

        # Step 2: Create canonical URI--the part of the URI from domain to query
        # string (use '/' if no path)

        # we first "unquote" to prevent double quoting
        # (e.g. if the URL is already %xx encoded, we need to un-convert before
        #       running conversion again, or else the %xx gets double encoded)
        unconverted = urllib.parse.unquote(uri)
        canonical_uri = urllib.parse.quote(unconverted, safe="/~")
        # print(f"converted uri from: {uri} unconverted to {unconverted} to {canonical_uri}")

        # Step 3: Empty query string since we don't pass params back to S3.
        canonical_querystring = ""

        # Step 4: Create the canonical headers and signed headers. Header names
        # must be trimmed and lowercase, and sorted in code point order from
        # low to high. Note that there is a trailing \n.
        canonical_headers = "host:" + self.host + "\n" + "x-amz-date:" + amzdate + "\n"

        # Step 5: Create the list of signed headers. This lists the headers
        # in the canonical_headers list, delimited with ";" and in alpha order.
        # Note: The request can include any headers; canonical_headers and
        # signed_headers lists those that you want to be included in the
        # hash of the request. "Host" and "x-amz-date" are always required.
        signed_headers = "host;x-amz-date"

        # Step 6: Create payload hash (hash of the request body content). For GET
        # requests, the payload is an empty string ("").
        payload_hash = self.empty_payload

        # Step 7: Combine elements to create canonical request
        canonical_request = (
            method
            + "\n"
            + canonical_uri
            + "\n"
            + canonical_querystring
            + "\n"
            + canonical_headers
            + "\n"
            + signed_headers
            + "\n"
            + payload_hash
        )

        # ************* TASK 2: CREATE THE STRING TO SIGN*************
        # Match the algorithm to the hashing algorithm you use, either SHA-1 or
        # SHA-256 (recommended)
        algorithm = "AWS4-HMAC-SHA256"
        credential_scope = (
            datestamp + "/" + self.region + "/" + self.service + "/" + "aws4_request"
        )
        string_to_sign = (
            algorithm
            + "\n"
            + amzdate
            + "\n"
            + credential_scope
            + "\n"
            + hashlib.sha256(canonical_request.encode()).hexdigest()
        )

        # ************* TASK 3: CALCULATE THE SIGNATURE *************
        # Create the signing key using the function defined above.
        signing_key = self.getSignatureKey(
            self.secret_key, datestamp, self.region, self.service
        )

        # Sign the string_to_sign using the signing_key
        signature = hmac.new(
            signing_key, string_to_sign.encode(), hashlib.sha256
        ).hexdigest()

        # ************* TASK 4: ADD SIGNING INFORMATION TO THE REQUEST *************
        # The signing information can be either in a query string value or in
        # a header named Authorization. This code shows how to use a header.
        # Create authorization header and add to request headers
        authorization_header = (
            algorithm
            + " "
            + "Credential="
            + self.access_key
            + "/"
            + credential_scope
            + ", "
            + "SignedHeaders="
            + signed_headers
            + ", "
            + "Signature="
            + signature
        )

        # The request can include any headers, but MUST include "host", "x-amz-date",
        # and (for this scenario) "Authorization". "host" and "x-amz-date" must
        # be included in the canonical_headers and signed_headers, as noted
        # earlier. Order here is not significant.
        # Python note: The 'host' header is added automatically by the Python 'requests' library.
        headers = {"x-amz-date": amzdate, "Authorization": authorization_header}
        return headers


if __name__ == "__main__":
    # ************* SEND THE REQUEST *************
    # Run test as:
    #   python -m mutil.awsSignatureV4 access_key secret_key FetchURL

    import time
    import sys

    from loguru import logger
    import requests
    import yarl

    from .timer import Timer

    try:
        access, secret, url = sys.argv[1:]
    except:
        logger.error(f"Run as:\n{sys.argv[0]}: access secret url")
        sys.exit(1)

    u = yarl.URL(url)
    method = "GET"
    scheme = u.scheme
    bucketHost = u.host or ""
    path = u.path

    # host looks like: [bucket].[service].[region].[company]
    parts = bucketHost.split(".")
    bucket = parts[0]
    service = parts[1]
    region = parts[2]
    splitURL = url.split("/")

    with Timer("Signing..."):
        sig = SignatureV4(access, secret, bucketHost)
        headers = sig.getHeaders(method, path)

    request_url = url

    useChunked = True
    chunkedChunkSize = 1 << 20
    print("\nBEGIN REQUEST++++++++++++++++++++++++++++++++++++")
    print("Request URL = " + request_url)
    with Timer("Requesting..."):
        r = requests.get(request_url, headers=headers, stream=useChunked)

    print("\nRESPONSE++++++++++++++++++++++++++++++++++++")
    print("Response code: %d\n" % r.status_code)

    with Timer("Writing..."):
        with open("got.out", "wb") as f:
            if useChunked:
                totalBytes = 0
                for chunk in r.iter_content(chunk_size=chunkedChunkSize):
                    if chunk:
                        # totalBytes += chunkedChunkSize
                        # print("Downloaded", totalBytes / 1024, "KB...")
                        f.write(chunk)
            else:
                f.write(r.content)
