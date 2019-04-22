#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Tests for the redis storage."""

from __future__ import unicode_literals

import mock
import unittest

from plaso.containers import errors
from plaso.containers import events
from plaso.containers import event_sources
from plaso.containers import reports
from plaso.containers import sessions
from plaso.containers import tasks
from plaso.lib import definitions
from plaso.storage.redis import redis_store

from tests import test_lib as shared_test_lib
from tests.storage import test_lib


class RedisStoreTest(test_lib.StorageTestCase):
  """Tests for the Redis storage object."""

  # pylint: disable=protected-access

  def testAddAttributeContainers(self):
    """Tests the _AddAttributeContainer method."""
    event_data = events.EventData()

    store = redis_store.RedisStore()
    store.Open()

    self.assertEqual(
        store._GetNumberOfAttributeContainers(event_data.CONTAINER_TYPE), 0)

    store._AddAttributeContainer(
        store._CONTAINER_TYPE_EVENT_DATA, event_data)

    self.assertEqual(
        store._GetNumberOfAttributeContainers(event_data.CONTAINER_TYPE), 1)

    has_containers = store._HasAttributeContainers(event_data.CONTAINER_TYPE)
    self.assertTrue(has_containers)

    store.Close()

  def testRemoveAttributeContainer(self):
    """Tests the _RemoveAttributeContainer method."""
    event_data = events.EventData()

    store = redis_store.RedisStore()
    store.Open()

    store._AddAttributeContainer(
        store._CONTAINER_TYPE_EVENT_DATA, event_data)

    self.assertEqual(
        store._GetNumberOfAttributeContainers(event_data.CONTAINER_TYPE), 1)

    identifier = event_data.GetIdentifier()
    store.RemoveAttributeContainer(
        store._CONTAINER_TYPE_EVENT_DATA, identifier)

    self.assertEqual(
        store._GetNumberOfAttributeContainers(event_data.CONTAINER_TYPE), 0)

    store.Close()


  def testAddEvent(self):
    """Tests the _AddEvent method."""
    test_events = self._CreateTestEvents()
    event = test_events[0]

    store = redis_store.RedisStore()
    store.Open()

    self.assertEqual(
        store._GetNumberOfAttributeContainers(event.CONTAINER_TYPE), 0)

    store.AddEvent(event)

    self.assertEqual(
        store._GetNumberOfAttributeContainers(event.CONTAINER_TYPE), 1)

    store.Close()

  def testGetSortedEvents(self):
    """Tests the GetSortedEvents method."""
    test_events = self._CreateTestEvents()

    store = redis_store.RedisStore()
    store.Open()

    for event in test_events:
      store.AddEvent(event)

    retrieved_events = list(store.GetSortedEvents())
    self.assertEqual(len(retrieved_events), 4)

    store.Close()

  def testGetAttributeContainers(self):
    """Tests the _GetAttributeContainers method."""
    test_event_data = [
      events.EventData(data_type='test_event_data1'),
      events.EventData(data_type='test_event_data2'),
      events.EventData(data_type='test_event_data3')
    ]

    store = redis_store.RedisStore()
    store.Open()

    for event_data in test_event_data:
      store.AddEventData(event_data)

    retrieved_event_data = list(
        store._GetAttributeContainers(store._CONTAINER_TYPE_EVENT_DATA))
    self.assertEqual(len(retrieved_event_data), 3)

    store.Close()

  def testGetAttributeContainerByIdentifier(self):
    """Tests the _GetAttributeContainerByIdentifier method."""
    test_event_tag = events.EventTag()

    store = redis_store.RedisStore()
    store.Open()

    store.AddEventTag(test_event_tag)

    identifier = test_event_tag.GetIdentifier()

    retrieved_event_tag = store._GetAttributeContainerByIdentifier(
        test_event_tag.CONTAINER_TYPE, identifier)

    test_event_tag_dict = test_event_tag.CopyToDict()
    retrieved_event_tag_dict = retrieved_event_tag.CopyToDict()

    self.assertEqual(test_event_tag_dict, retrieved_event_tag_dict)

    store.Close()

  def testFinalization(self):
    """Tests that a store is correctly finalized."""
    store = redis_store.RedisStore()
    store.Open()
    self.assertFalse(store.IsFinalized())
    store.Close()

    store.Open()
    self.assertFalse(store.IsFinalized())
    store.Finalize()
    self.assertTrue(store.IsFinalized())
    store.Close()

    store.Open()
    self.assertTrue(store.IsFinalized())
    store.Close()
