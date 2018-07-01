import asyncio
from datetime import datetime
import email.utils
import logging
from types import MappingProxyType
from typing import Union
from pathlib import PosixPath
import json

# Third Party
import aiohttp.helpers
import aiobotocore.session
import aiobotocore.config
import yarl
import backoff


_empty_dict = MappingProxyType({})
_empty_list = tuple()

_DEFAULT_BATCH_SIZE = 100


def _parse_rfc822(dt: str) -> datetime:
    ts = email.utils.mktime_tz(email.utils.parsedate_tz(dt))
    dt = datetime.utcfromtimestamp(ts)
    return dt


class _Pager:
    def __init__(self, session: aiohttp.ClientSession,  url: yarl.URL, batch_size: int, response_key: str):
        self._session = session
        self._url = url
        self._batch_size = batch_size
        self._batch = None
        self._response_key = response_key
        self._next = None

    def __aiter__(self):
        self._batch = None
        self._next = None
        return self

    @backoff.on_exception(backoff.expo, (aiohttp.ClientError, asyncio.TimeoutError), max_tries=2)
    async def _get_next_batch(self, url: Union[yarl.URL, str]):
        with aiohttp.helpers.CeilTimeout(30):
            async with self._session.get(url) as response:
                self._next = response.links.get('next', _empty_dict).get('url')
                self._batch = await response.json()

    async def __anext__(self) -> str:
        if self._batch is None:
            url = self._url.with_query(dict(n=self._batch_size))
            await self._get_next_batch(url)
        elif self._next and not self._batch[self._response_key]:
            await self._get_next_batch(self._next)

        try:
            errors = self._batch.get('errors')
            if errors and errors[0].get('code') == 'NAME_UNKNOWN' and self._url.path.endswith('/tags/list'):
                raise StopAsyncIteration

            return self._batch[self._response_key].pop(0)
        except IndexError:
            raise StopAsyncIteration


class RegistryClient:
    def __init__(self, url: str):
        """
        Creates docker registry client instance based on V2 docker registry REST API
        :param url: base url to docker registry endpoint
        """
        self._url = yarl.URL(url)
        self._session: aiohttp.ClientSession = None
        self._logger = logging.getLogger('RegistryClient')

        boto_session = aiobotocore.session.get_session()
        config = aiobotocore.config.AioConfig(connect_timeout=10, read_timeout=10, max_pool_connections=100)
        self._s3_client = boto_session.create_client('s3', config=config)

    async def __aenter__(self):
        timeout = aiohttp.ClientTimeout(sock_read=15, sock_connect=15)
        self._session = await aiohttp.ClientSession(timeout=timeout).__aenter__()
        self._s3_client = await self._s3_client.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._session.__aexit__(exc_type, exc_val, exc_tb)
        await self._s3_client.__aexit__(exc_type, exc_val, exc_tb)

    def catalog_pager(self, batch_size: int=_DEFAULT_BATCH_SIZE):
        return _Pager(self._session, self._url / 'v2/_catalog', batch_size, 'repositories')

    def image_tag_pager(self, image_name: str, batch_size: int=_DEFAULT_BATCH_SIZE):
        return _Pager(self._session, self._url / f'v2/{image_name}/tags/list', batch_size, 'tags')

    @backoff.on_exception(backoff.expo, (aiohttp.ClientError, asyncio.TimeoutError), max_tries=2)
    async def get_image_manifest(self, image_name: str, tag: str):
        with aiohttp.helpers.CeilTimeout(30):
            async with self._session.get(self._url / 'v2' / image_name / 'manifests' / tag) as response:
                data = await response.json(content_type=None)
        return data

    @backoff.on_exception(backoff.expo, (aiohttp.ClientError, asyncio.TimeoutError), max_tries=2)
    async def get_blob_info(self, image_name: str, blob_sum: str):
        info = dict()
        with aiohttp.helpers.CeilTimeout(30):
            async with self._session.head(self._url / 'v2' / image_name / 'blobs' / blob_sum) as response:
                location = response.headers.get("Location")
                if location:
                    location = yarl.URL(location)

                if location and location.host.startswith("s3-") and location.host.endswith(".amazonaws.com"):
                    region = location.host[3:].split(".", 1)[0]
                    bucket, key = yarl.URL(location).path[1:].split("/", 1)
                    info["s3location"] = dict(region=region, bucket=bucket, key=key)

                    response = await self._s3_client.head_object(Bucket=bucket, Key=key)
                    info["size"] = response['ContentLength']
                    info['modified'] = response['LastModified']

        if 's3location' not in info:
            with aiohttp.helpers.CeilTimeout(30):
                async with self._session.get(self._url / 'v2' / image_name / 'blobs' / blob_sum, read_until_eof=False) as response:
                    info["size"] = int(response.headers["Content-Length"])
                    info["modified"] = _parse_rfc822(response.headers["Last-Modified"])
                    response.close()

        return info


class _S3Pager:
    def __init__(self, async_iter):
        self._async_iter = async_iter
        self._prefix_len = None
        self._next = None

    async def _get_next_batch(self):
        response = await self._async_iter.__anext__()
        self._prefix_len = len(response['Prefix'])
        self._next = response.get('CommonPrefixes', [])

    def __aiter__(self):
        self._next = None
        self._prefix_len = None
        return self

    async def __anext__(self):
        if not self._next:
            await self._get_next_batch()

        try:
            item = self._next.pop(0)
            return item['Prefix'][self._prefix_len:-1]
        except IndexError:
            raise StopAsyncIteration


class S3RegistryClient:
    def __init__(self, bucket: str, prefix: str):
        """
        Creates docker registry client instance based on S3 registry backend
        :param bucket: S3 bucket of registry
        :param prefix: S3 bucket prefix, ex: docker/v2/dev/docker/registry/v2, where "blobs" and "repositories" folders exist
        """
        self._bucket = bucket
        self._prefix = PosixPath(prefix)

        boto_session = aiobotocore.session.get_session()
        config = aiobotocore.config.AioConfig(connect_timeout=10, read_timeout=10, max_pool_connections=100)
        self._s3_client = boto_session.create_client('s3', config=config)

    async def __aenter__(self):
        self._s3_client = await self._s3_client.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._s3_client.__aexit__(exc_type, exc_val, exc_tb)

    def catalog_pager(self):
        paginator = self._s3_client.get_paginator('list_objects')
        prefix = str(self._prefix / "repositories") + '/'
        return _S3Pager(paginator.paginate(Bucket=self._bucket, Prefix=prefix, Delimiter='/'))

    def image_tag_pager(self, image_name: str):
        # TODO: validate against repository API
        paginator = self._s3_client.get_paginator('list_objects')
        prefix = str(self._prefix / "repositories" / image_name / "_manifests" / "tags") + '/'
        return _S3Pager(paginator.paginate(Bucket=self._bucket, Prefix=prefix, Delimiter='/'))

    async def get_image_manifest(self, image_name: str, tag: str):
        # TODO: this is not correct, not matching repository API
        # get pointer to current version of this tag
        response = await self._s3_client.get_object(Bucket=self._bucket, Key=str(self._prefix / "repositories" / image_name / "_manifests" / "tags" / tag / "current" / "link"))
        async with response["Body"] as stream:
            data = await stream.read()
            sha_prefix, sha256 = data.decode('utf-8').split(':', 1)

        # now get the manifest link
        response = await self._s3_client.get_object(Bucket=self._bucket, Key=str(self._prefix / "repositories" / image_name / "_manifests" / "tags" / tag / "index" / "sha256" / sha256 / "link"))
        async with response["Body"] as stream:
            data = await stream.read()
            sha_prefix, sha256 = data.decode('utf-8').split(':', 1)

        # now get the manifest
        response = await self._s3_client.get_object(Bucket=self._bucket, Key=str(self._prefix / "blobs" / "sha256" / sha256[:2] / sha256 / "data"))
        async with response["Body"] as stream:
            manifest = await stream.read()

        return json.loads(manifest)

    async def get_blob_info(self, image_name: str, blob_sum: str):
        pass
