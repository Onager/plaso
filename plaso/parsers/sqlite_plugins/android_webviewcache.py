# -*- coding: utf-8 -*-
"""Parser for Android WebviewCache databases."""

from plaso.containers import time_events
from plaso.lib import eventdata
from plaso.parsers import sqlite
from plaso.parsers.sqlite_plugins import interface


class WebViewCacheURLModificationEvent(time_events.JavaTimeEvent):
  """Convenience class for a WebViewCache modification event"""
  DATA_TYPE = u'android:webviewcache:url_modification'

  def __init__(self, timestamp, url, content_length):
    """Convenience class for a WebViewCache modification event"""
    super(WebViewCacheURLModificationEvent, self).__init__(
        timestamp, eventdata.EventTimestamp.MODIFICATION_TIME)
    self.content_length = content_length
    self.url = url

class WebViewCacheURLExpirationEvent(time_events.JavaTimeEvent):
  """Convenience class for WebView cache expiry event"""
  DATA_TYPE = u'android:webviewcache:url_expiry'

  def __init__(self, timestamp, url, content_length):
    super(WebViewCacheURLExpirationEvent, self).__init__(
        timestamp, eventdata.EventTimestamp.EXPIRATION_TIME)
    self.content_length = content_length
    self.url = url

class WebViewCachePlugin(interface.SQLitePlugin):
  """Parser for WebViewCache databases."""

  NAME = u'android_webviewcache'
  DESCRIPTION = u'Parser for Android WebViewCache databases'

  REQUIRED_TABLES = frozenset([u'android_metadata', u'cache'])

  QUERIES = frozenset([
      (u'SELECT url, contentlength, expires, lastmodify FROM cache',
       u'ParseRow')])

  def ParseRow(self, parser_mediator, row, query=None, **unused_kwargs):
    """Parses a row from the database.

    Args:
      parser_mediator: A parser mediator object (instance of ParserMediator).
      row: The row resulting from the query.
      query: Optional query string.
    """
    if row['expires'] is not None:
      expires_event = WebViewCacheURLExpirationEvent(
          row['expires'], row['url'], row['contentlength'])
      parser_mediator.ProduceEvent(expires_event, query=query)

    if row['lastmodify'] is not None:
      modification_event = WebViewCacheURLModificationEvent(
          row['lastmodify'], row['url'], row['contentlength'])
      parser_mediator.ProduceEvent(modification_event, query=query)


sqlite.SQLiteParser.RegisterPlugin(WebViewCachePlugin)
