from datetime import datetime
import email.utils
from types import MappingProxyType
from typing import Union

# Third Party
import aiohttp
import aiobotocore.session
import aiobotocore.config
import yarl


_empty_dict = MappingProxyType({})

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

    async def _get_next_batch(self, url: Union[yarl.URL, str]):
        async with self._session.get(url) as response:
            self._next = response.links.get('next', _empty_dict).get('url')
            self._batch = await response.json()

    async def __anext__(self) -> str:
        if self._batch is None:
            url = self._url.with_query(dict(n=self._batch_size))
            await self._get_next_batch(url)

        try:
            errors = self._batch.get('errors')
            if errors and errors[0].get('code') == 'NAME_UNKNOWN' and self._url.path.endswith('/tags/list'):
                raise StopAsyncIteration

            return self._batch[self._response_key].pop(0)
        except IndexError:
            if self._next:
                await self._get_next_batch(self._next)

                try:
                    return self._batch[self._response_key].pop(0)
                except IndexError:
                    pass  # fall through

            raise StopAsyncIteration


class RegistryClient:
    def __init__(self, url: str):
        self._url = yarl.URL(url)
        self._session: aiohttp.ClientSession = None

        boto_session = aiobotocore.session.get_session()
        config = aiobotocore.config.AioConfig(connect_timeout=15, read_timeout=15, max_pool_connections=100)
        self._s3_client = boto_session.create_client('s3', config=config)

    async def __aenter__(self):
        self._session = await aiohttp.ClientSession().__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._session.__aexit__(exc_type, exc_val, exc_tb)

    def catalog_pager(self, batch_size: int=_DEFAULT_BATCH_SIZE):
        return _Pager(self._session, self._url / 'v2/_catalog', batch_size, 'repositories')

    def image_tag_pager(self, image_name: str, batch_size: int=_DEFAULT_BATCH_SIZE):
        return _Pager(self._session, self._url / f'v2/{image_name}/tags/list', batch_size, 'tags')

    async def get_image_manifest(self, image_name: str, tag: str):
        async with self._session.get(self._url / 'v2' / image_name / 'manifests' / tag) as response:
            data = await response.json(content_type=None)
            return data

    async def get_blob_info(self, image_name: str, blob_sum: str):
        info = dict()
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
            async with self._session.get(self._url / 'v2' / image_name / 'blobs' / blob_sum, read_until_eof=False) as response:
                info["size"] = int(response.headers["Content-Length"])
                info["modified"] = _parse_rfc822(response.headers["Last-Modified"])
                response.close()

        return info
