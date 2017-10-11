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
    self._store.GetEventData()

  def GetEventDataByIdentifier(self, identifier):
    self._store.GetEventDataByIdentifier(identifier)

  def GetEvents(self):
    self._store.GetEvents()

  def GetEventSources(self):
    self._store.GetEventSources()

  def GetEventTagByIdentifier(self, identifier):
    self._store.GetEventTagByIdentifier(identifier)

  def GetEventTags(self):
    for event_tag in self._store.GetEventTags():
      yield event_tag

  def GetNumberOfAnalysisReports(self):
    return self._store.GetNumberOfAnalysisReports()

  def GetSortedEvents(self, time_range=None):
    self._store.GetSortedEvents(time_range)

  def ReadPreprocessingInformation(self, knowledge_base):
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