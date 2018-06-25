rate-limiter
============

Async and Sync rate-limiter implementations.

Install
-------
::

    $ pip install rate-limiter


Basic Example
-------------

async def main():
    async with RegistryClient("https://repos.fbn.org") as client:
        async for image_name in client.catalog_pager():
            async for tag in client.image_tag_pager(image_name):
                data = await client.get_image_manifest(image_name, tag)


if __name__ == '__main__':
    import asyncio
    asyncio.get_event_loop().run_until_complete(main())


Requirements
------------
* Python_ 3.6+