# -*- coding: utf-8 -*-
"""Storage writer for Redis."""

from __future__ import unicode_literals

from plaso.lib import definitions
from plaso.storage import interface
from plaso.storage.redis import redis_store


class RedisStorageWriter(interface.StorageWriter):
  """Redis-based storage writer."""

  def __init__(
      self, session, storage_type=definitions.STORAGE_TYPE_SESSION, task=None):
    """Initializes a storage writer.

    Args:
      session (Session): session the storage changes are part of.
      storage_type (Optional[str]): storage type.
      task(Optional[Task]): task.

    Raises:
      RuntimeError: if no task is provided.
    """
    if not task:
      raise RuntimeError('Task required.')

    super(RedisStorageWriter, self).__init__(
        session=session, storage_type=storage_type, task=task)

    self._redis_namespace = '{0:s}-{1:s}'.format(
        task.session_identifier, task.identifier)
    self._store = None

  def CreateTaskStorage(self, task, storage_format):
    """Creates a task storage.

    Args:
      task (Task): task.
      storage_format (str): storage format to store task results.

    Returns:
      FakeStorageWriter: storage writer.

    Raises:
      IOError: if the task storage already exists.
    """
    raise IOError('Unsupported storage type.')

  def FinalizeTaskStorage(self, task):
    """Finalizes a processed task storage.

    Args:
      task (Task): task.
    """
    raise IOError('Unsupported storage type.')

  def Open(self):
    """Opens the storage writer.

    Raises:
      IOError: if the storage writer is already opened.
    """
    if self._store:
      raise IOError('Storage writer already opened.')

    self._store = redis_store.RedisStore(
        storage_type=self._storage_type,
        session_identifier=self._task.session_identifier,
        task_identifier=self._task.identifier)

    self._store.Open()

  def Close(self):
    """Closes the storage writer.

    Raises:
      IOError: when the storage writer is closed.
    """
    if not self._store:
      raise IOError('Storage writer is not open.')

    self._store.Finalize()
    self._store.Close()
    self._store = None

  def PrepareMergeTaskStorage(self, task):
    """Prepares a task storage for merging.

    Args:
      task (Task): task.
    """
    pass # No preparations are necessary.

  def AddAnalysisReport(self, analysis_report):
    """Adds an analysis report.

    Args:
      analysis_report (AnalysisReport): a report.
    """
    self._store.AddAnalysisReport(analysis_report)

  def AddEvent(self, event):
    """Adds an event.

    Args:
      event(EventObject): an event.
    """
    self._store.AddEvent(event)

  def AddEventData(self, event_data):
    """Adds an event data.

    Args:
      event_data(EventData): an event.
    """
    self._store.AddEventData(event_data)

  def AddEventSource(self, event_source):
    """Adds an event source.

    Args:
      event_source (EventSource): an event source.
    """
    self._store.AddEventSource(event_source)

  def AddEventTag(self, event_tag):
    """Adds an event tag.

    Args:
      event_tag (EventTag): an event tag.
    """
    self._store.AddEventTag(event_tag)

  def AddWarning(self, warning):
    """Adds a warning.

    Args:
      warning (ExtractionWarning): a warning.
    """
    self._store.AddWarning(warning)

  def GetEventDataByIdentifier(self, identifier):
    """Retrieves specific event data.

    Args:
      identifier (AttributeContainerIdentifier): event data identifier.

    Returns:
      EventData: event data or None if not available.
    """
    return self._store.GetEventDataByIdentifier(identifier)

  def GetEvents(self):
    """Retrieves the events.

    Returns:
      generator(EventObject): event generator.
    """
    return self._store.GetEvents()

  def GetFirstWrittenEventSource(self):
    """Retrieves the first event source that was written after open.

    Using GetFirstWrittenEventSource and GetNextWrittenEventSource newly
    added event sources can be retrieved in order of addition.

    Returns:
      EventSource: event source or None if there are no newly written ones.

    Raises:
      IOError: when the storage writer is closed.
    """
    if not self._store:
      raise IOError('Unable to read from closed storage writer.')

    return None

  def GetNextWrittenEventSource(self):
    """Retrieves the next event source that was written after open.

    Returns:
      EventSource: event source or None if there are no newly written ones.

    Raises:
      IOError: when the storage writer is closed.
    """
    if not self._store:
      raise IOError('Unable to read from closed storage writer.')

    return None

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
    if not self._store:
      raise IOError('Unable to read from closed storage writer.')

    return self._store.GetSortedEvents(time_range=time_range)

  def ReadPreprocessingInformation(self, knowledge_base):
    """Reads preprocessing information.

    Args:
      knowledge_base (KnowledgeBase): is used to store the preprocessing
          information.
    """
    raise IOError('Preprocessing information not supported by storage type.')

  def RemoveProcessedTaskStorage(self, task):
    """Removes a processed task storage.

    Args:
      task (Task): task.

    Raises:
      NotImplementedError: since there is no implementation.
    """
    raise RuntimeError('Not implemented.')

  def SetSerializersProfiler(self, serializers_profiler):
    """Sets the serializers profiler.

    Args:
      serializers_profiler (SerializersProfiler): serializers profiler.
    """
    self._serializers_profiler = serializers_profiler

  def SetStorageProfiler(self, storage_profiler):
    """Sets the storage profiler.

    Args:
      storage_profiler (StorageProfiler): storage profiler.
    """
    self._storage_profiler = storage_profiler

  def WritePreprocessingInformation(self, knowledge_base):
    """Writes preprocessing information.

    Args:
      knowledge_base (KnowledgeBase): contains the preprocessing information.
    """
    raise IOError('Preprocessing information not supported by storage type.')

  def WriteSessionCompletion(self, aborted=False):
    """Writes session completion information.

    Args:
      aborted (Optional[bool]): True if the session was aborted.

    Raises:
      IOError: if the storage type does not support writing a session
          completion or when the storage writer is closed.
    """
    raise IOError('Unsupported storage type.')

  def WriteSessionStart(self):
    """Writes session start information.

    Raises:
      IOError: if the storage type does not support writing a session
          start or when the storage writer is closed.
    """
    raise IOError('Unsupported storage type.')

  def WriteTaskCompletion(self, aborted=False):
    """Writes task completion information.

    Args:
      aborted (Optional[bool]): True if the session was aborted.

    Raises:
      IOError: if the storage type is not supported or
          when the storage writer is closed.
    """
    if self._storage_type != definitions.STORAGE_TYPE_TASK:
      raise IOError('Unsupported storage type.')

    self._task.aborted = aborted
    task_completion = self._task.CreateTaskCompletion()
    self._store.WriteTaskCompletion(task_completion)


  def WriteTaskStart(self):
    """Writes task start information.

    Raises:
      IOError: if the storage type does not support writing a task
          start or when the storage writer is closed.
    """
    if self._storage_type != definitions.STORAGE_TYPE_TASK:
      raise IOError('Unsupported storage type.')

    task_start = self._task.CreateTaskStart()
    self._store.WriteTaskStart(task_start)