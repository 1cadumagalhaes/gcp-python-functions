## DatastoreUtils

The `DatastoreUtils` class provides utility functions to interact with Google Cloud Datastore.

### Initialization

TODO

### Methods

#### `query_entity(kind: str, filters=[], namespace=None) -> list`

Query entities from the Datastore.

- `kind` (str): The kind of entities to query.
- `filters` (list): The list of filters to apply to the query. Each filter is represented as a dictionary with "field", "condition", and "value" keys.
- `namespace` (optional, str): The namespace of the entities to query. If not provided, the default namespace will be used.

Returns a list of matching entities.

#### `add_entity(kind: str, data: dict, key_name: str = None)`

Add an entity to the Datastore.

- `kind` (str): The kind of the entity.
- `data` (dict): The data of the entity as a dictionary.
- `key_name` (optional, str): The key name of the entity. If not provided, an auto-generated key will be used.

Returns the inserted entity.

#### `batch_add_entities(kind: str, data: list)`

Batch add entities to the Datastore.

- `kind` (str): The kind of the entities.
- `data` (list): The list of entities to insert. Each entity should be a dictionary.

Returns the last inserted entity.
