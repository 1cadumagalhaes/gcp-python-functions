from google.cloud import datastore
import logging


class DatastoreUtils:
    def __init__(self, project: str = None, namespace=None, logger=None):
        """
        Initialize the DatastoreUtils class.

        :param project: The ID of the Google Cloud project.
        :param namespace: The namespace of the datastore entities. If not provided, the default namespace will be used.
        :param logger: A custom logger instance to use for logging. If not provided, the default logging module will be used.
        """
        self.project = project
        self.namespace = namespace
        if logger is None:
            self.logger = logging
        else:
            self.logger = logger

        self.client = self.__create_client()

    def __create_client(self):
        """
        Create the Datastore client.

        :return: The Datastore client.
        """
        if self.namespace:
            return datastore.Client(project=self.project, namespace=self.namespace)
        else:
            return datastore.Client(project=self.project)

    def query_entity(self, kind: str, filters=[], namespace=None) -> list:
        """
        Query entities from the Datastore.

        :param kind: The kind of entities to query.
        :param filters: The list of filters to apply to the query. Each filter is represented as a dictionary with "field", "condition", and "value" keys.
        :param namespace: The namespace of the entities to query. If not provided, the default namespace will be used.
        :return: A list of matching entities.
        """
        namespace = namespace if namespace is not None else self.namespace
        query = self.client.query(namespace=namespace, kind=kind)
        self.logger.debug("[DatastoreUtils.query_entity] creating query filter")
        for filter in filters:
            query.add_filter(
                filter.get("field"), filter.get("condition", "="), filter.get("value")
            )
        try:
            self.logger.debug("[DatastoreUtils.query_entity] fetching results")
            results = list(query.fetch())

            self.logger.info(
                f"[DatastoreUtils.query_entity] found {len(results)} entities"
            )
            return results
        except Exception as e:
            self.logger.error("[DatastoreUtils.query_entity]", e)

    def add_entity(self, kind: str, data: dict, key_name: str = None):
        """
        Add an entity to the Datastore.

        :param kind: The kind of the entity.
        :param data: The data of the entity as a dictionary.
        :param key_name: The key name of the entity. If not provided, an auto-generated key will be used.
        :return: The inserted entity.
        """
        if key_name is None:
            key = self.client.key(kind)
        else:
            key = self.client.key(kind, f"{data.get('dataset')}.{data.get('tabela')}")

        entity = datastore.Entity(key=key)
        entity.update(data)
        try:
            self.logger.debug("[DatastoreUtils.add_entity] upserting entity")
            self.client.put(entity)
            self.logger.info("[DatastoreUtils.add_entity] inserted entity")
        except Exception as e:
            self.logger.error("[DatastoreUtils.add_entity]", e)

        return entity

    def batch_add_entities(self, kind: str, data: list):
        """
        Batch add entities to the Datastore.

        :param kind: The kind of the entities.
        :param data: The list of entities to insert. Each entity should be a dictionary.
        :return: The last inserted entity.
        """
        entities = []
        for row in data:
            key = self.client.key(kind, f"{row.get('dataset')}.{row.get('tabela')}")
            entity = datastore.Entity(key=key)
            entity.update(row)
            entities.append(entity)

        try:
            self.logger.debug(
                f"[DatastoreUtils.batch_add_entities] upserting {len(entities)} entities to kind {kind}"
            )
            self.client.put_multi(entities=entities)
            self.logger.info(
                f"[DatastoreUtils.batch_add_entities] inserted {len(entities)} entities to kind {kind}"
            )
        except Exception as e:
            self.logger.error("[DatastoreUtils.batch_add_entities]", e)

        return entity
