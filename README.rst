Asyncio Docker Registry Client
============

asyncio docker registry client

Install
-------
::

    $ pip install aiodocker-registry


Basic Example
-------------

```python
async def main():
    async with RegistryClient("https://repos.fbn.org") as client:
        async for image_name in client.catalog_pager():
            async for tag in client.image_tag_pager(image_name):
                data = await client.get_image_manifest(image_name, tag)


if __name__ == '__main__':
    import asyncio
    asyncio.get_event_loop().run_until_complete(main())
```

Requirements
------------
* Python_ 3.6+