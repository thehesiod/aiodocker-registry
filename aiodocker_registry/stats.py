import asyncio
from collections import defaultdict
import logging
from typing import Dict

from registry_client import RegistryClient

import asyncpool


# Gouping by blobs that only have one reference to a single tag


class RepoStats:
    def __init__(self):
        self._blob_to_image_tags = defaultdict(lambda: {"info": None, "usage": defaultdict(set)})  # {blob_sum: {"info":, "usage": {image_name: {tag, ...}}}}
        self._logger = logging.getLogger()
        self._total_blob_size = 0
        self._blob_groups = dict()  # {group_id: {"blobs": {blob_sum, ...}, "image_
        self._client: RegistryClient = None
        self._locks: Dict[str, asyncio.Lock] = dict()  # {blob_sum: Lock}
        self._image_info = defaultdict(lambda: {"total_shared_size": 0, 'tags': set()})

        self._blob_pool: asyncpool.AsyncPool = None

    async def __aenter__(self):
        self._client = await RegistryClient("https://repos.fbn.org").__aenter__()
        self._blob_pool = await asyncpool.AsyncPool(None, 100, 'blob_pool', self._logger, self._process_blob, raise_on_join=True, log_every_n=250).__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._blob_pool.__aexit__(exc_type, exc_val, exc_tb)
        await self._client.__aexit__(exc_type, exc_val, exc_tb)

    async def get_stats(self):
        async with asyncpool.AsyncPool(None, 10, 'img_pool', self._logger, self._process_image, raise_on_join=True, log_every_n=10) as pool:
            async for image_name in self._client.catalog_pager():
                await pool.push(self._client, image_name)
                if pool.total_queued == 10:
                    break

        self._logger.info(f"Total num blobs: {len(self._blob_to_image_tags)} size: {self._total_blob_size:,}")

    async def _process_blob(self, img_name: str, blob_sum: str):
        try:
            blob_info = await self._client.get_blob_info(img_name, blob_sum)
            self._blob_to_image_tags[blob_sum]["info"] = blob_info
            self._total_blob_size += blob_info["size"]
            self._image_info[img_name]["total_shared_size"] += blob_info["size"]
        finally:
            self._locks[blob_sum].release()
            del self._locks[blob_sum]

    async def _process_image(self, client: RegistryClient, image_name: str):
        tag_to_blobs = defaultdict(set)  # tag: {blob_sum, ...}
        tags = list()
        blobs = set()

        self._logger.info(f"Processing image: {image_name}")
        tag = None

        try:
            async for tag in client.image_tag_pager(image_name):
                tags.append(tag)

                data = await client.get_image_manifest(image_name, tag)
                for layer in data['fsLayers']:
                    blob_sum = layer["blobSum"]

                    blobs.add(blob_sum)
                    tag_to_blobs[tag].add(blob_sum)

                    # add global reference
                    blob_entry = self._blob_to_image_tags[blob_sum]
                    blob_entry["usage"][image_name].add(tag)

                    if blob_sum in self._locks:
                        await self._locks[blob_sum].acquire()
                        self._image_info[image_name]["total_shared_size"] += blob_entry["info"]["size"]
                    elif not blob_entry["info"]:
                        lock = self._locks[blob_sum] = asyncio.Lock()
                        await lock.acquire()
                        await self._blob_pool.push(image_name, blob_sum)
                    else:
                        self._image_info[image_name]["total_shared_size"] += blob_entry["info"]["size"]

            # wait until all the data is available
            for blob_sum in blobs:
                if blob_sum in self._locks:
                    await self._locks[blob_sum].acquire()

            self._logger.info(f"image: {image_name} tags: {tags} total_shared_size: {self._image_info[image_name]['total_shared_size']:,} blobs: {len(blobs)}")
        except:
            self._logger.exception(f"Error processing image: {image_name} tag: {tag}")
            raise


async def main():
    import logging
    logging.basicConfig(level=logging.INFO)

    async with RepoStats() as stats:
        await stats.get_stats()


if __name__ == '__main__':
    import asyncio
    asyncio.get_event_loop().run_until_complete(main())
