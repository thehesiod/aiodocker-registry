#!/usr/bin/env python3
import asyncio
import argparse
from pprint import pprint

import registry_client


async def main():
    parser = argparse.ArgumentParser(description="Docker Registry Utilities")
    # parser.add_argument('-url', required=True, help="Registry Base URL")
    parser.add_argument('-bucket', required=True, help="S3 Bucket of Repository")
    parser.add_argument('-prefix', required=True, help="S3 Bucket Prefix of Repository")
    subparsers = parser.add_subparsers(title="command", dest="command")
    subparsers.required = True

    parser_images = subparsers.add_parser('images')

    parser_tags = subparsers.add_parser('tags')
    parser_tags.add_argument('-image_name', required=True, help="Image name of manifest")

    parser_manifest = subparsers.add_parser('manifest')
    parser_manifest.add_argument('-image_name', required=True, help="Image name of manifest")
    parser_manifest.add_argument('-tag', required=True, help="Tag name of manifest")

    app_args = parser.parse_args()

    if hasattr(app_args, 'url'):
        rclient = registry_client.RegistryClient(app_args.url)
    else:
        rclient = registry_client.S3RegistryClient(app_args.bucket, app_args.prefix)

    async with rclient as client:
        if app_args.command == "manifest":
            manifest = await client.get_image_manifest(app_args.image_name, app_args.tag)
            pprint(manifest, width=200)
        elif app_args.command == "images":
            async for image in client.catalog_pager():
                print(image)
        elif app_args.command == 'tags':
            async for tag in client.image_tag_pager(app_args.image_name):
                print(tag)


if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(main())
