Pyrogram asyncpg storage
========================

Usage
-----

```python
import asyncpg
from pyrogram import Client
from pyrogram_asyncpg_storage import PostgreSQLStorage


app = Client(...)


async def set_session():
	pool = await asyncpg.create_pool()
	session = PostgreSQLStorage(name=..., pool=pool)
	app.storage = session


app.run(set_session())
app.run()
```
