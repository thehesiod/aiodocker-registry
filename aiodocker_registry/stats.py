#!/usr/bin/env python3
import asyncio
import argparse
import os
from collections import defaultdict
import logging
from typing import Dict, Tuple, Union, Set, List
import shelve
import pickle
import hashlib

# Module
from registry_client import RegistryClient, S3RegistryClient
from draw_chart import get_treemap

# Third Party
import asyncpool


# TODO: untracked blobs (GC)
class _BlobGroupInstanceHelper:
    def __init__(self):
        self._instances = defaultdict(int)

    def new_instance(self, blob_group_name: str):
        g_inst = self._instances[blob_group_name]
        self._instances[blob_group_name] += 1
        return f"{blob_group_name}_{g_inst}"


_sentinel = object()


# ordering is important
def _get_blob_group_key(blob_group: List[str]):
    return hashlib.sha256(".".join(blob_group).encode('utf-8')).digest()


class RepoStats:
    def __init__(self, client: Union[RegistryClient, S3RegistryClient], shelf_path: str):
        """
        Docker Repository Statistics Helper

        # :param url: base url to docker repository
        :param shelf_path: path to file to cache data to
        """

        self._logger = logging.getLogger()
        self._total_blob_size = 0
        self._client: RegistryClient = None
        self._inprogress_blobs: Set[str] = set()  # {blob_sum, ...}
        self._shelf_path = shelf_path
        self._shelf = None
        # self._repo_url = url

        self._client = client

        # TODO: reduce data duplication after layout finalized
        self._blob_to_image_tags = defaultdict(lambda: {"info": None, "usage": defaultdict(set)})  # {blob_sum: {"info":, "usage": {image_name: {tag, ...}}}}
        self._image_info = defaultdict(lambda: defaultdict(set))  # {image_name: {blob_group_key: {tag_name, ...}}}}
        # {blob_group_key: {'blobs': [blob_sum, ...]}, 'size':, 'images: {image_name: {tag_name, ...}}}}
        self._blob_groups = defaultdict(lambda: {'blobs': None, 'size': None, 'images': defaultdict(set)})

    async def __aenter__(self):
        if self._shelf_path:
            self._shelf = shelve.open(self._shelf_path, protocol=pickle.HIGHEST_PROTOCOL).__enter__()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._shelf_path:
            self._shelf.__exit__(exc_type, exc_val, exc_tb)

    def _get_blob_group_name(self, blob_group_key: int):
        name = ", ".join(f"{image_name}:{tags}" for image_name, tags in self._blob_groups[blob_group_key]['images'].items())
        return name

    # TODO: clean return up
    def _get_blob_group_info(self, blob_group_key: int) -> Tuple[int, Union[None, int]]:
        """
        Returns tuple: (group_unique_size, parent_blob_group_key)
        :param blob_group_key:
        :return: tuple: (group_unique_size, parent_blob_group_key)
        """
        # NOTE: unique size does not account for base-images which come from other repositories
        unique_size = 0
        # docker images that rely on base images have all the layers from the parent image and then their
        # own after
        blob_group = self._blob_groups[blob_group_key]
        for end_idx in range(len(blob_group['blobs']), -1, -1):
            parent_blob_group_key = _get_blob_group_key(blob_group['blobs'][:end_idx])
            if parent_blob_group_key and parent_blob_group_key != blob_group_key and parent_blob_group_key in self._blob_groups:
                return unique_size, parent_blob_group_key
            else:
                blob_sum = blob_group['blobs'][end_idx - 1]
                unique_size += self._blob_to_image_tags[blob_sum]["info"]["size"]

        return unique_size, None

    async def get_stats(self, root_path: str, max_image_names: int=None):
        async with asyncpool.AsyncPool(None, 100, 'blob_pool', self._logger, self._process_blob, raise_on_join=True, log_every_n=1000) as blob_pool, \
                asyncpool.AsyncPool(None, 100, 'img_pool', self._logger, self._process_image_tag, raise_on_join=True, log_every_n=1000) as image_tag_pool:
            image_num = 0
            async for image_name in self._client.catalog_pager():
                num_tags = 0
                async for tag in self._client.image_tag_pager(image_name):
                    num_tags += 1
                    await image_tag_pool.push(blob_pool, image_name, tag)

                image_num += 1
                self._logger.info(f"{image_name} pushed num tags: {num_tags}")
                if max_image_names is not None and image_num == max_image_names:
                    break

        description = [
            ('Group Name', 'string'),
            ('Parent', 'string'),
            ('Size (size)', 'number'),
        ]

        g_data = []

        # This needs to be done from parent to child
        g_instances = _BlobGroupInstanceHelper()

        for image_name, blob_groups in self._image_info.items():
            image_unique_size = 0

            img_data = []

            for blob_group_key, tags in blob_groups.items():
                blob_group_unique_size, parent_blob_group_key = self._get_blob_group_info(blob_group_key)
                image_unique_size += blob_group_unique_size
                orig_blob_group_name = blob_group_name = g_instances.new_instance(self._get_blob_group_name(blob_group_key))

                while parent_blob_group_key:
                    parent_blob_group_name = g_instances.new_instance(self._get_blob_group_name(parent_blob_group_key))
                    blob_group_unique_size, parent_blob_group_key = self._get_blob_group_info(parent_blob_group_key)

                    img_data.append((parent_blob_group_name, blob_group_name, blob_group_unique_size))
                    blob_group_name = parent_blob_group_name
                    break  # TODO: find better way to show large multi-level trees

                img_data.append((orig_blob_group_name, "root", blob_group_unique_size))  # unfortunately you can't have two nodes point to this

            g_data.append((image_name, 'root', image_unique_size))
            img_data.append(("root", None, image_unique_size))
            get_treemap(description, img_data, os.path.join(root_path, f"{image_name}.html"), False)

        g_data.append(("root", None, self._total_blob_size))

        self._logger.info(f"Total num blobs: {len(self._blob_to_image_tags)} size: {self._total_blob_size:,}")

        get_treemap(description, g_data, os.path.join(root_path, "root.html"), True)

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
            self._inprogress_blobs.remove(blob_sum)

    async def _process_image_tag(self, blob_pool: asyncpool.AsyncPool, image_name: str, tag: str):
        manifest_key = f"{image_name}:{tag}"
        manifest = self._shelf.get(manifest_key) if self._shelf is not None else None
        if manifest is None:
            manifest = await self._client.get_image_manifest(image_name, tag)
            if manifest is None:
                manifest = await self._client.get_image_manifest(image_name, tag)

            assert manifest
            if self._shelf is not None:
                if manifest['schemaVersion'] == 1:
                    if 'history' in manifest:
                        del manifest['history']
                    if 'signatures' in manifest:
                        del manifest['signatures']

                self._shelf[manifest_key] = manifest

        if manifest['schemaVersion'] == 1:
            # TODO: validate v1 fsLayers == reversed v2 layers
            layers = list(reversed([layer for layer in manifest['fsLayers']]))
            blob_group = [layer['blobSum'] for layer in layers]
        else:
            assert manifest['schemaVersion'] == 2
            layers = [layer for layer in manifest['layers']]
            blob_group = [layer['digest'] for layer in layers]

        # TODO: validate if we can use manifest["config"]["digest"]
        blob_group_key = _get_blob_group_key(blob_group)
        blob_group_entry = self._blob_groups[blob_group_key]
        if blob_group_entry['blobs'] is None:  # avoid object churn
            blob_group_entry['blobs'] = blob_group
        blob_group_entry['images'][image_name].add(tag)

        self._image_info[image_name][blob_group_key].add(tag)

        for layer in layers:
            blob_sum = layer["blobSum"] if manifest['schemaVersion'] == 1 else layer['digest']

            # add global reference
            blob_entry = self._blob_to_image_tags[blob_sum]
            blob_entry["usage"][image_name].add(tag)

            if manifest['schemaVersion'] == 1:
                if not blob_entry["info"] and blob_sum not in self._inprogress_blobs:
                    self._inprogress_blobs.add(blob_sum)
                    await blob_pool.push(image_name, blob_sum)
            else:
                if not blob_entry["info"]:
                    self._total_blob_size += layer["size"]
                    self._blob_to_image_tags[blob_sum]["info"] = layer


async def main():
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser(description='Docker Repository Statistics Tool')
    parser.add_argument('-num', type=int, help="Number of images to query")
    # parser.add_argument('-url', required=True, type=str, help='Repository Base URL')
    parser.add_argument('-bucket', required=True, help="S3 Bucket of Repository")
    parser.add_argument('-prefix', required=True, help="S3 Bucket Prefix of Repository")
    parser.add_argument('-graph_path', required=True, type=str, help="Path to graph html to")
    parser.add_argument('-shelf_path', type=str, help="Path to file to cache repository info to")
    app_args = parser.parse_args()

    if hasattr(app_args, 'url'):
        rclient = RegistryClient(app_args.url, max_connections=150)
    else:
        rclient = S3RegistryClient(app_args.bucket, app_args.prefix, max_connections=150)

    async with rclient, RepoStats(rclient, app_args.shelf_path) as stats:
        await stats.get_stats(app_args.graph_path, app_args.num)


if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(main())
