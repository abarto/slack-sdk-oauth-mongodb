import logging

from uuid import uuid4

from pymongo import ASCENDING
from pymongo.database import Database
from time import time

from slack_sdk.oauth.state_store import OAuthStateStore
from slack_sdk.oauth.state_store.async_state_store import AsyncOAuthStateStore


class MongoDBAsyncOAuthStateStore(OAuthStateStore, AsyncOAuthStateStore):
    def __init__(
        self,
        db: Database,
        expiration_seconds: int,
        logger: logging.Logger = logging.getLogger(__name__),
        oauth_states_collection_name: str = "oauth_states",
    ):
        self.slack_oauth_states_collection = db[oauth_states_collection_name]
        self.expiration_seconds = expiration_seconds
        self._logger = logger

    @property
    def logger(self) -> logging.Logger:
        if self._logger is None:
            self._logger = logging.getLogger(__name__)
        return self._logger

    def init(self):
        """Initialize database store by ensuring indexes exist on the proper fields"""
        self.slack_oauth_states_collection.create_index(
            [
                ("state", ASCENDING),
            ],
            background=True,
        )

    def issue(self, *args, **kwargs) -> str:
        self.logger.debug("args: %s, kwargs: %s", args, kwargs)
        
        state = str(uuid4())
        
        self.logger.debug("state: %s", state)
        
        self.slack_oauth_states_collection.insert_one(
            {"state": state, "expire_at": time() + self.expiration_seconds}
        )
        return state

    def consume(self, state: str) -> bool:
        self.logger.debug("state: %s", state)
        
        delete_result = self.slack_oauth_states_collection.delete_many(
            {"state": state, "expire_at": {"$gt": time()}}
        )
        
        self.logger.debug("delete_result: %s", delete_result)
        
        return delete_result.deleted_count > 0

    async def async_issue(self, *args, **kwargs) -> str:
        return self.issue(*args, **kwargs)

    async def async_consume(self, state: str) -> bool:
        return self.consume(state)
