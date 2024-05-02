# slack-sdk-oauth-mongodb

This repository contains MongoDB implementations of Python Slack SDK's `InstallationStore` and `OAuthStateStore`.

Although the implementations support the `asyncio` interfaces, they only call the regular blocking
methods, which use [`pymongo`](https://pymongo.readthedocs.io/en/stable/index.html). A proper async
implementation can be easily adapted to use [`motor`](https://www.mongodb.com/docs/drivers/motor/).