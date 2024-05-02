import logging

from dataclasses import asdict, dataclass, fields
from typing import Optional

from pymongo import ASCENDING, DESCENDING
from pymongo.database import Database
from slack_sdk.oauth.installation_store import InstallationStore
from slack_sdk.oauth.installation_store.async_installation_store import (
    AsyncInstallationStore,
)
from slack_sdk.oauth.installation_store.models.bot import Bot
from slack_sdk.oauth.installation_store.models.installation import Installation


class InstallationDocument(dataclass(Installation)):
    """
    Helper dataclass that wraps the Installation class to
    make it easier to convert to and from JSON.
    """

    @classmethod
    def from_installation(cls, installation: Installation):
        return InstallationDocument(
            **{field.name: getattr(installation, field.name) for field in fields(cls)},
        )


class BotDocument(dataclass(Bot)):
    """
    Helper dataclass that wraps the Bot class to
    make it easier to convert to and from JSON.
    """

    @classmethod
    def from_bot(cls, bot: Bot):
        return BotDocument(
            **{field.name: getattr(bot, field.name) for field in fields(cls)},
        )


class MongoDBInstallationStore(AsyncInstallationStore, InstallationStore):
    def __init__(
        self,
        db: Database,
        client_id: str,
        logger: logging.addLevelNameLogger = logging.getLogger(__name__),
        installations_collection_name: str = "slack_installations",
        bots_collection_name: str = "slack_bots",
    ):
        self.slack_installations_collection = db[installations_collection_name]
        self.slack_bots_collection = db[bots_collection_name]
        self.client_id = client_id
        self._logger = logger

    @property
    def logger(self) -> logging.Logger:
        if self._logger is None:
            self._logger = logging.getLogger(__name__)
        return self._logger

    def init(self):
        """Initialize database store by ensuring indexes exist on the proper fields"""
        self.slack_installations_collection.create_index(
            [
                ("client_id", ASCENDING),
                ("enterprise_id", ASCENDING),
                ("team_id", ASCENDING),
                ("user_id", ASCENDING),
                ("installed_at", ASCENDING),
            ],
            background=True,
        )
        self.slack_installations_collection.create_index(
            [
                ("client_id", ASCENDING),
                ("enterprise_id", ASCENDING),
                ("team_id", ASCENDING),
                ("installed_at", ASCENDING),
            ],
            background=True,
        )

    def save(self, installation: Installation):
        """Saves an installation data"""
        self.logger.debug("installation: %s", installation)

        installation_document = InstallationDocument.from_installation(
            installation=installation
        )

        self.logger.debug("installation_document: %s", installation_document)

        insert_result = self.slack_installations_collection.insert_one(
            {"client_id": self.client_id, **asdict(installation_document)}
        )
        self.logger.debug("insert_result: %s", insert_result)

        self.save_bot(installation.to_bot())

    def save_bot(self, bot: Bot):
        """Saves a bot installation data"""
        self.logger.debug("bot: %s", bot)

        bot_document = BotDocument.from_bot(bot=bot)

        self.logger.debug("bot_document: %s", bot_document)

        insert_result = self.slack_bots_collection.insert_one(
            {"client_id": self.client_id, **asdict(bot_document)}
        )

        self.logger.debug("insert_result: %s", insert_result)

    def find_bot(
        self,
        *,
        enterprise_id: Optional[str],
        team_id: Optional[str],
        is_enterprise_install: Optional[bool] = False,
    ) -> Optional[Bot]:
        """Finds a bot scope installation per workspace / org"""
        self.logger.debug(
            "enterprise_id: %s, team_id: %s, is_enterprise_install: %s",
            enterprise_id,
            team_id,
            is_enterprise_install,
        )

        doc = self.slack_bots_collection.find_one(
            {
                "client_id": self.client_id,
                "enterprise_id": enterprise_id,
                "team_id": (
                    None if team_id is None or is_enterprise_install else team_id
                ),
            },
            {"_id": 0, "client_id": 0},
            sort=[("installed_at", DESCENDING)],
        )

        self.logger.debug("doc: %s", doc)

        if doc is None:
            return None

        bot = BotDocument(**doc)

        self.logger.debug("bot: %s", bot)

        return bot

    def find_installation(
        self,
        *,
        enterprise_id: Optional[str],
        team_id: Optional[str],
        user_id: Optional[str] = None,
        is_enterprise_install: Optional[bool] = False,
    ) -> Optional[Installation]:
        """Finds a relevant installation for the given IDs.
        If the user_id is absent, this method may return the latest installation in the workspace / org.
        """
        self.logger.debug(
            "enterprise_id: %s, team_id: %s, user_id: %s, is_enterprise_install: %s",
            enterprise_id,
            team_id,
            user_id,
            is_enterprise_install,
        )
        query = {
            "client_id": self.client_id,
            "enterprise_id": enterprise_id,
            "team_id": None if team_id is None or is_enterprise_install else team_id,
        }
        if user_id is not None:
            query["user_id"] = user_id

        doc = self.slack_installations_collection.find_one(
            query, {"_id": 0, "client_id": 0}, sort=[("installed_at", DESCENDING)]
        )

        self.logger.debug("doc: %s", doc)

        if doc is None:
            return None

        if user_id is not None:
            latest_bot_token_doc = self.slack_installations_collection.find_one(
                {**query, "bot_token": {"$ne": None}},
                {"_id": 0, "client_id": 0},
                sort=[("installed_at", DESCENDING)],
            )

            self.logger.info("latest_bot_token_doc: %s", latest_bot_token_doc)

            if latest_bot_token_doc is not None:
                doc.update(
                    {
                        field: latest_bot_token_doc[field]
                        for field in (
                            "bot_token",
                            "bot_id",
                            "bot_user_id",
                            "bot_scopes",
                            "bot_refresh_token",
                            "bot_token_expires_at",
                        )
                    }
                )

        installation = InstallationDocument(**doc)

        self.logger.debug("installation: %s", installation)

        return installation

    def delete_bot(
        self,
        *,
        enterprise_id: Optional[str],
        team_id: Optional[str],
    ) -> None:
        """Deletes a bot scope installation per workspace / org"""
        self.logger.debug("enterprise_id: %s, team_id: %s", enterprise_id, team_id)
        delete_result = self.slack_bots_collection.delete_many(
            {
                "client_id": self.client_id,
                "enterprise_id": enterprise_id,
                "team_id": team_id,
            }
        )
        self.logger.debug("delete_result: %s", delete_result)

    def delete_installation(
        self,
        *,
        enterprise_id: Optional[str],
        team_id: Optional[str],
        user_id: Optional[str] = None,
    ) -> None:
        """Deletes an installation that matches the given IDs"""
        self.logger.info(
            "enterprise_id: %s, team_id: %s, user_id: %s",
            enterprise_id,
            team_id,
            user_id,
        )
        query = {
            "client_id": self.client_id,
            "enterprise_id": enterprise_id,
            "team_id": team_id,
        }
        if user_id is not None:
            query["user_id"] = user_id

        delete_result = self.slack_bots_collection.delete_many(query)

        self.logger.info("delete_result: %s", delete_result)

    async def async_save(self, installation: Installation):
        """Saves an installation data"""
        self.save(installation)

    async def async_save_bot(self, bot: Bot):
        """Saves a bot installation data"""
        self.save_bot(bot)

    async def async_find_bot(
        self,
        *,
        enterprise_id: Optional[str],
        team_id: Optional[str],
        is_enterprise_install: Optional[bool] = False,
    ) -> Optional[Bot]:
        """Finds a bot scope installation per workspace / org"""
        return self.find_bot(
            enterprise_id, team_id, is_enterprise_install=is_enterprise_install
        )

    async def async_find_installation(
        self,
        *,
        enterprise_id: Optional[str],
        team_id: Optional[str],
        user_id: Optional[str] = None,
        is_enterprise_install: Optional[bool] = False,
    ) -> Optional[Installation]:
        """Finds a relevant installation for the given IDs.
        If the user_id is absent, this method may return the latest installation in the workspace / org.
        """
        return self.find_installation(
            enterprise_id,
            team_id,
            user_id=user_id,
            is_enterprise_install=is_enterprise_install,
        )

    async def async_delete_bot(
        self,
        *,
        enterprise_id: Optional[str],
        team_id: Optional[str],
    ) -> None:
        """Deletes a bot scope installation per workspace / org"""
        self.delete_bot(enterprise_id, team_id)

    async def async_delete_installation(
        self,
        *,
        enterprise_id: Optional[str],
        team_id: Optional[str],
        user_id: Optional[str] = None,
    ) -> None:
        """Deletes an installation that matches the given IDs"""
        self.delete_installation(enterprise_id, team_id, user_id=user_id)
