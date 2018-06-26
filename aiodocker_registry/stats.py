import asyncio
from collections import defaultdict
import logging
from typing import Dict
import shelve
import pickle

# Module
from registry_client import RegistryClient
from draw_chart import get_treemap

# Third Party
import asyncpool


# TODO: untracked blobs (GC)
# TODO: remove shelfed manifest history

class RepoStats:
    def __init__(self):
        self._logger = logging.getLogger()
        self._total_blob_size = 0
        self._client: RegistryClient = None
        self._locks: Dict[str, asyncio.Lock] = dict()  # {blob_sum: Lock}
        self._blob_pool: asyncpool.AsyncPool = None
        self._shelf = None  # for debugging

        # TODO: reduce data duplication after layout finalized
        self._blob_to_image_tags = defaultdict(lambda: {"info": None, "usage": defaultdict(set)})  # {blob_sum: {"info":, "usage": {image_name: {tag, ...}}}}
        self._image_info = defaultdict(lambda: defaultdict(set))  # {image_name: {blob_group_key: {tag_name, ...}}}}

        # {blob_group_key: {'blobs': [blob_sum, ...]}, 'size':, 'images: {image_name: {tag_name, ...}}}}
        self._blob_groups = defaultdict(lambda: {'blobs': None, 'size': None, 'images': defaultdict(set)})

    async def __aenter__(self):
        self._shelf = shelve.open('/tmp/aiodocker_registry.shelf', protocol=pickle.HIGHEST_PROTOCOL)
        self._client = await RegistryClient("https://repos.fbn.org").__aenter__()
        self._blob_pool = await asyncpool.AsyncPool(None, 100, 'blob_pool', self._logger, self._process_blob, raise_on_join=True, log_every_n=250).__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._blob_pool.__aexit__(exc_type, exc_val, exc_tb)
        await self._client.__aexit__(exc_type, exc_val, exc_tb)
        self._shelf.close()

    def _get_blob_group_size(self, blob_group_key: int):
        entry = self._blob_groups[blob_group_key]
        if entry["size"] is None:
            entry["size"] = sum(self._blob_to_image_tags[blob_sum]["info"]["size"] for blob_sum in entry["blobs"])
        return entry["size"]

    def _get_blob_group_name(self, blob_group_key: int):
        name = ", ".join(f"{image_name}:{tags}" for image_name, tags in self._blob_groups[blob_group_key]['images'].items())
        return name

    async def get_stats(self, max_image_names: int=None):
        async with asyncpool.AsyncPool(None, 10, 'img_pool', self._logger, self._process_image, raise_on_join=True, log_every_n=10) as pool:
            async for image_name in self._client.catalog_pager():
                await pool.push(image_name)
                if max_image_names is not None and pool.total_queued == max_image_names:
                    break

        description = [
            ('Group Name', 'string'),
            ('Parent', 'string'),
            ('Size (size)', 'number'),
        ]

        data = []

        # This needs to be done from parent to child
        group_instances = defaultdict(int)
        for image_name, blob_groups in self._image_info.items():
            image_shared_size = 0
            for blob_group_key, tags in blob_groups.items():
                blob_group_size = self._get_blob_group_size(blob_group_key)
                blob_group_name = self._get_blob_group_name(blob_group_key)
                image_shared_size += blob_group_size

                g_inst = group_instances[blob_group_name]
                group_instances[blob_group_name] += 1

                data.append((f"{blob_group_name}_{g_inst}", image_name, blob_group_size))  # unfortunately you can't have two nodes point to this

            data.append((image_name, 'root', image_shared_size))

        data.append(("root", None, self._total_blob_size))

        self._logger.info(f"Total num blobs: {len(self._blob_to_image_tags)} size: {self._total_blob_size:,}")

        return get_treemap(description, data)

    async def _process_blob(self, image_name: str, blob_sum: str):
        try:
            blob_info = self._shelf.get(blob_sum) if self._shelf is not None else None
            if not blob_info:
                blob_info = await self._client.get_blob_info(image_name, blob_sum)

                if self._shelf is not None:  # cache it
                    self._shelf[blob_sum] = blob_info

            self._blob_to_image_tags[blob_sum]["info"] = blob_info
            self._total_blob_size += blob_info["size"]
        finally:
            self._locks[blob_sum].release()
            del self._locks[blob_sum]

    async def _process_image(self, image_name: str):
        tags = list()
        blobs = set()

        self._logger.info(f"Processing image: {image_name}")
        tag = None

        try:
            async for tag in self._client.image_tag_pager(image_name):
                tags.append(tag)

                manifest_key = f"{image_name}:{tag}"
                blob_group = self._shelf.get(manifest_key) if self._shelf is not None else None
                if blob_group is None:
                    manifest = await self._client.get_image_manifest(image_name, tag)
                    if manifest is None:
                        manifest = await self._client.get_image_manifest(image_name, tag)
                    blob_group = frozenset(layer['blobSum'] for layer in manifest['fsLayers'])

                    if self._shelf is not None:
                        self._shelf[manifest_key] = blob_group

                blob_group_key = hash(blob_group)

                blob_group_entry = self._blob_groups[blob_group_key]
                if blob_group_entry['blobs'] is None:  # avoid object churn
                    blob_group_entry['blobs'] = blob_group
                blob_group_entry['images'][image_name].add(tag)

                self._image_info[image_name][blob_group_key].add(tag)

                for blob_sum in blob_group:
                    blobs.add(blob_sum)

                    # add global reference
                    blob_entry = self._blob_to_image_tags[blob_sum]
                    blob_entry["usage"][image_name].add(tag)

                    if not blob_entry["info"] and blob_sum not in self._locks:
                        lock = self._locks[blob_sum] = asyncio.Lock()
                        await lock.acquire()
                        await self._blob_pool.push(image_name, blob_sum)

                    # NOTE: at this point we may not have the blob info

            # wait until all the data is available
            image_shared_size = 0
            for blob_sum in blobs:
                if blob_sum in self._locks:
                    await self._locks[blob_sum].acquire()

                image_shared_size += self._blob_to_image_tags[blob_sum]["info"]["size"]

            self._logger.info(f"image: {image_name} tags: {tags} total_shared_size: {image_shared_size:,} blobs: {len(blobs)}")
        except:
            self._logger.exception(f"Error processing image: {image_name} tag: {tag}")
            raise


async def main():
    import logging
    logging.basicConfig(level=logging.INFO)

    async with RepoStats() as stats:
        data = await stats.get_stats(30)

        with open('/tmp/graph.html', 'w') as f:
            f.write(data)


if __name__ == '__main__':
    import asyncio
    asyncio.get_event_loop().run_until_complete(main())
