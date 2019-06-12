# -*- coding: utf-8 -*-
"""Redis reader."""
from __future__ import unicode_literals

from plaso.lib import definitions
from plaso.storage.redis import redis_store
from plaso.storage import interface

class RedisStorageReader(interface.StorageReader):
  """Redis storage file reader."""

  def __init__(self, task):
    super(RedisStorageReader, self).__init__()
    self._store = redis_store.RedisStore(
        storage_type=definitions.STORAGE_TYPE_TASK,
        session_identifier=task.session_identifier,
        task_identifier=task.identifier)

  def IsFinalized(self):
    """Checks if the store has been finalized.

    Returns:
      bool: True if the store has been finalized.
    """
    return self._store.IsFinalized()

  def Open(self):
    """Opens the storage reader."""
    self._store.Open()

  def Close(self):
    """Closes the storage reader."""
    self._store.Close()

  def GetAnalysisReports(self):
    """Retrieves the analysis reports.

    Yields:
      AnalysisReport: analysis report.
    """
    for report in self._store.GetAnalysisReports():
      yield report

  def GetErrors(self):
    """Retrieves the errors.

    Yields:
      ExtractionError: error.
    """
    for error in self._store.GetErrors():
      yield error

  def GetEventData(self):
    """Retrieves the event data.

    Yields:
      EventData: event data.
    """
    self._store.GetEventData()

  def GetEventDataByIdentifier(self, identifier):
    """Retrieves specific event data.

    Args:
      identifier (AttributeContainerIdentifier): event data identifier.

    Returns:
      EventData: event data or None if not available.
    """
    self._store.GetEventDataByIdentifier(identifier)

  def GetEvents(self):
    """Retrieves the events.

    Yields:
      EventObject: event.
    """
    self._store.GetEvents()

  def GetEventSources(self):
    """Retrieves event sources.

    Yields:
      EventSourceObject: event source.
    """
    self._store.GetEventSources()

  def GetEventTagByIdentifier(self, identifier):
    """Retrieves a specific event tag.

    Args:
      identifier (AttributeContainerIdentifier): event tag identifier.

    Returns:
      EventTag: event tag or None if not available.
    """
    self._store.GetEventTagByIdentifier(identifier)

  def GetEventTags(self):
    """Retrieves the event tags.

    Yields:
      EventTag: event tag.
    """
    for event_tag in self._store.GetEventTags():
      yield event_tag

  def GetNumberOfAnalysisReports(self):
    """Retrieves the number analysis reports.

    Returns:
      int: number of analysis reports.
    """
    return self._store.GetNumberOfAnalysisReports()

  def GetSortedEvents(self, time_range=None):
    """Retrieves the events in increasing chronological order.

    This includes all events written to the storage including those pending
    being flushed (written) to the storage.

    Args:
      time_range (Optional[TimeRange]): time range used to filter events
          that fall in a specific period.

    Yields:
      EventObject: event.
    """
    self._store.GetSortedEvents(time_range)

  def ReadPreprocessingInformation(self, knowledge_base):
    """Reads preprocessing information.

    The preprocessing information contains the system configuration which
    contains information about various system specific configuration data,
    for example the user accounts.

    Args:
      knowledge_base (KnowledgeBase): is used to store the preprocessing
          information.
    """
    pass

  def SetSerializersProfiler(self, serializers_profiler):
    """Sets the serializers profiler.

    Args:
      serializers_profiler (SerializersProfiler): serializers profiler.
    """
    self._store.SetSerializersProfiler(serializers_profiler)

  def SetStorageProfiler(self, storage_profiler):
    """Sets the storage profiler.

    Args:
      storage_profiler (StorageProfiler): storage profile.
    """
    self._store.SetStorageProfiler(storage_profiler)