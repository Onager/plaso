# -*- coding: utf-8 -*-
"""Redis store.

Only supports task storage at the moment.
"""
from __future__ import unicode_literals

import uuid

import redis

from plaso.lib import definitions
from plaso.lib import errors
from plaso.storage import identifiers
from plaso.storage import interface
from plaso.storage import logger


class RedisStore(interface.BaseStore):
  """Redis store."""

  _FORMAT_VERSION = '20181013'
  _EVENT_INDEX_NAME = 'sorted_event_identifier'
  _FINALIZED_KEY_NAME = 'finalized'
  _FINALIZED_BYTES = b'finalized'
  _MERGING_KEY_NAME = 'merging'
  _MERGING_BYTES = b'merging'

  def __init__(
      self, storage_type=definitions.STORAGE_TYPE_TASK,
      session_identifier=None, task_identifier=None):
    """Initializes a redis store.

    Args:
      storage_type (Optional[str]): storage type.

    Raises:
      ValueError: if the storage type is not supported.
    """
    if storage_type != definitions.STORAGE_TYPE_TASK:
      raise ValueError('Unsupported storage type: {0:s}.'.format(
          storage_type))
    super(RedisStore, self).__init__()
    if not session_identifier:
      session_identifier = str(uuid.uuid4())
    self._session_identifier = session_identifier
    if not task_identifier:
      task_identifier = str(uuid.uuid4())
    self._task_identifier = task_identifier
    self._redis_client = None
    self.serialization_format = definitions.SERIALIZER_FORMAT_JSON

  def _AddAttributeContainer(self, container_type, container):
    """Adds an attribute container to the store.

   Args:
      container_type (Type): container type attribute of the container being
          added.
      container (AttributeContainer): unserialized attribute container.
    """
    self._RaiseIfNotWritable()

    identifier = identifiers.RedisKeyIdentifier()
    container.SetIdentifier(identifier)

    serialized_data = self._SerializeAttributeContainer(container)

    container_key = self._GenerateRedisKey(container_type)
    string_identifier = identifier.CopyToString()
    self._redis_client.hset(container_key, string_identifier, serialized_data)

  def _GenerateRedisKey(self, subkey):
    """Generates a redis key inside the appropriate namespace.

    Args:
      subkey (str): redis key to be prefixed with the namespace value.

    Returns:
      str: a redis key name.
    """
    return '{0:s}-{1:s}-{2:s}'.format(
        self._session_identifier, self._task_identifier, subkey)
  def _GetAttributeContainers(self, container_type):
    """Yields attribute containers

    Args:
      container_type (str): container type attribute of the container being
          added.

    Yields:
      AttributeContainer: attribute container.
    """
    container_key = self._GenerateRedisKey(container_type)
    for identifier, serialized_data in self._redis_client.hscan_iter(
        container_key):
      attribute_container = self._DeserializeAttributeContainer(
          container_type, serialized_data)
      identifier = identifiers.RedisKeyIdentifier(identifier)
      attribute_container.SetIdentifier(identifier)
      yield attribute_container

  def _GetAttributeContainerByIdentifier(self, container_type, identifier):
    """Retrieves the container with a specific identifier.

    Args:
      container_type (str): container type.
      identifier (AttributeContainerIdentifier): event data identifier.

    Returns:
      AttributeContainer: attribute container or None if not available.
    """
    container_key = self._GenerateRedisKey(container_type)
    string_identifier = identifier.CopyToString()

    serialized_data = self._redis_client.hget(
        container_key, string_identifier)

    if not serialized_data:
      return None

    attribute_container = self._DeserializeAttributeContainer(
        container_type, serialized_data)

    attribute_container.SetIdentifier(identifier)
    return attribute_container

  def _GetNumberOfAttributeContainers(self, container_type):
    """Determines the number of containers of a type in the store.

    Args:
      container_type (str): attribute container type.

    Returns:
      int: the number of containers in the store of the specified type.
    """
    container_key = self._GenerateRedisKey(container_type)
    return self._redis_client.hlen(container_key)

  def _GetFinalizationKey(self):
    """Generates the finalized key for the store."""
    return '{0:s}-{1:s}'.format(
        self._session_identifier, self._FINALIZED_KEY_NAME)

  def _HasAttributeContainers(self, container_type):
    """Determines if the store contains a specific type of attribute container.

    Args:
      container_type (str): attribute container type.

    Returns:
      bool: True if the store contains the specified type of attribute
          containers.
    """
    container_key = self._GenerateRedisKey(container_type)
    return self._redis_client.hlen(container_key) > 0

  def _RaiseIfNotReadable(self):
    """Checks that the store is ready to for reading.

     Raises:
       StorageNotReadableError: if the store cannot be read from.
    """
    if not self._redis_client:
      raise errors.StorageError(
          'Unable to read, client not connected.')

  def _RaiseIfNotWritable(self):
    """Checks that the store is ready to for writing.

     Raises:
       StorageNotWritableError: if the store cannot be written to.
    """
    if not self._redis_client:
      raise errors.StorageError('Unable to write, client not connected.')

  def _WriteStorageMetadata(self):
    """Writes the storage metadata."""
    metadata = {
      'format_version': self._FORMAT_VERSION,
       'storage_type': definitions.STORAGE_TYPE_TASK,
       'serialization_format': self.serialization_format}
    metadata_key = self._GenerateRedisKey('metadata')

    self._redis_client.hmset(metadata_key, metadata)

  def AddEvent(self, event):
    """Adds an event.

    Args:
      event (EventObject): event.
    """
    super(RedisStore, self).AddEvent(event)
    event_index_name = self._GenerateRedisKey(self._EVENT_INDEX_NAME)
    identifier = event.GetIdentifier()
    string_identifier = identifier.CopyToString()
    self._redis_client.zincrby(
        event_index_name, string_identifier, event.timestamp)

  def Close(self):
    """Closes the store."""
    self._redis_client = None

  def RemoveAttributeContainer(self, container_type, identifier):
    """Removes an attribute container from the store.

    Args:
      container_type (Type): container type attribute of the container being
          removed.
      identifier (AttributeContainerIdentifier): event data identifier.
    """
    self._RaiseIfNotWritable()
    container_key = self._GenerateRedisKey(container_type)
    string_identifier = identifier.CopyToString()

    self._redis_client.hdel(container_key, string_identifier)
    if container_type == self._CONTAINER_TYPE_EVENT:
      event_index_name = self._GenerateRedisKey(self._EVENT_INDEX_NAME)
      self._redis_client.zrem(event_index_name, string_identifier)

  def IsFinalized(self):
    """Checks if a store has been finalized.

    Returns:
      bool: True if the store has been finalized.
    """
    self._RaiseIfNotReadable()

    finalized_key = self._GetFinalizationKey()
    finalized_value =  self._redis_client.hget(
        finalized_key, self._task_identifier)

    return finalized_value == self._FINALIZED_BYTES

  def Finalize(self):
    """Marks a store as finalized.

    No further attribute containers will be written to a finalized store.
    """
    self._RaiseIfNotWritable()

    finalized_key = self._GetFinalizationKey()
    self._redis_client.hset(
        finalized_key, self._task_identifier, self._FINALIZED_BYTES)

    logger.info('Finalized task store for: {0:s}'.format(self._task_identifier))

  def GetSortedEvents(self, time_range=None):
    """Retrieves the events in increasing chronological order.

    Args:
      time_range (Optional[TimeRange]): time range used to filter events
          that fall in a specific period.

    Yields:
      EventObject: event.
    """
    event_index_name = self._GenerateRedisKey(self._EVENT_INDEX_NAME)
    if time_range:
      raise RuntimeError('Not supported')
    sorted_event_identifiers = self._redis_client.zscan_iter(event_index_name)
    for event_identifier, _ in sorted_event_identifiers:
      event_identifier = identifiers.RedisKeyIdentifier(event_identifier)
      yield self._GetAttributeContainerByIdentifier(
          self._CONTAINER_TYPE_EVENT, event_identifier)

  def Open(self, host='localhost', port=6379, db=0, redis_client=None):
    """Opens the store.

    Args:
      host (Optional[str]):  hostname or IP address where redis is running.
      port (Optional[int]): port that redis is listening on.
      db (Optional[int]): redis db identifier for storing containers.
      redis_client (Optional[Redis]): redis client to query. If specified, no
          new client will be created.

    Raises:
      IOError: if the store is already connected to a Redis instance.
    """
    if self._redis_client:
      raise IOError('Redis client already connected')

    if redis_client:
      self._redis_client = redis_client
    else:
      self._redis_client = redis.StrictRedis(
          host=host, port=port, db=db, socket_timeout=60)

    # client_name = self._GenerateRedisKey('')
    # self._redis_client.client_setname(client_name)

    metadata_key = self._GenerateRedisKey('metadata')
    if not self._redis_client.exists(metadata_key):
      self._WriteStorageMetadata()

  def Remove(self):
    """Removes the contents of the store from Redis."""
    merging_key = '{0:s}-{1:s}'.format(
        self._session_identifier, self._MERGING_KEY_NAME)
    self._redis_client.hdel(merging_key, self._task_identifier)

    sorted_event_key = self._GenerateRedisKey(self._EVENT_INDEX_NAME)
    self._redis_client.delete(sorted_event_key)

    task_completion_key = self._GenerateRedisKey(
        self._CONTAINER_TYPE_TASK_COMPLETION)
    self._redis_client.delete(task_completion_key)

    metadata_key = self._GenerateRedisKey('metadata')
    self._redis_client.delete(metadata_key)

  @classmethod
  def ScanForProcessedTasks(
      cls, session_identifier, host='localhost', port=6379, db=0,
      redis_client=None):
    """Scans a redis database for processed tasks.

    Args:
      session_identifier:
      host (Optional[str]):  hostname or IP address where redis is running.
      port (Optional[int]): port that redis is listening on.
      db (Optional[int]): redis db identifier for storing containers.
      redis_client (Optional[Redis]): redis client to query. If specified, no
          new client will be created.

    Returns:
      list[str]: identifiers of processed tasks.
    """
    logger.debug('Starting check for processed tasks')
    if not redis_client:
      redis_client = redis.StrictRedis(
          host=host, port=port, db=db, socket_timeout=60)
    finalization_key = '{0:s}-{1:s}'.format(
        session_identifier, cls._FINALIZED_KEY_NAME)
    try:
      # redis_client.client_setname('processed_scan')
      task_identifiers = redis_client.hkeys(finalization_key)
    except redis.exceptions.TimeoutError:
      return []
    task_identifiers = [key.decode('utf-8') for key in task_identifiers]
    logger.debug('Found processed task identifiers: {0!s}'.format(task_identifiers))
    return task_identifiers

  @classmethod
  def MarkTaskAsMerging(
      cls, task_identifier, session_identifier, host='localhost', port=6379,
      db=0, redis_client=None):
    """Marks a finalized task as pending merge.

    Args:
      task_identifier: TODO task identifier
      session_identifier: TODO session identifier
      host (Optional[str]):  hostname or IP address where redis is running.
      port (Optional[int]): port that redis is listening on.
      db (Optional[int]): redis db identifier for storing containers.
      redis_client (Optional[Redis]): redis client to query. If specified, no
          new client will be created.

    Raises:
      IOError: if the task being updated is not finalized.
    """
    if not redis_client:
      redis_client = redis.StrictRedis(
          host=host, port=port, db=db, socket_timeout=60)

    # redis_client.client_setname('merge_mark')

    finalization_key = '{0:s}-{1:s}'.format(
        session_identifier, cls._FINALIZED_KEY_NAME)
    number_of_deleted_fields = redis_client.hdel(
        finalization_key, task_identifier)
    if number_of_deleted_fields == 0:
      raise IOError('Task identifier {0:s} was not finalized'.format(
          task_identifier))

    merging_key = '{0:s}-{1:s}'.format(
        session_identifier, cls._MERGING_KEY_NAME)
    redis_client.hset(merging_key, task_identifier, cls._MERGING_BYTES)