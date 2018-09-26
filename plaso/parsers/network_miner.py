# -*- coding: utf-8 -*-
"""NetworkMiner CSV parser."""

from __future__ import unicode_literals

from __future__ import unicode_literals

from dfdatetime import definitions as dfdatetime_definitions
from dfdatetime import posix_time as dfdatetime_posix_time
from dfdatetime import time_elements as dfdatetime_time_elements

from plaso.containers import events
from plaso.containers import time_events
from plaso.lib import errors
from plaso.lib import definitions
from plaso.parsers import dsv_parser
from plaso.parsers import manager


class NetworkMinerFileInfoEventData(events.EventData):
  """Network Miner event data.

  Attributes:
    sourceip: 
    sourceport: 
    destinationip:
    destinationport: 
    filename: 
    path: 
    filesize: 
    filestreamtype :
    md5:
    header:
    details:
  """

  DATA_TYPE = 'network:network_miner:file_info'

  def __init__(self, sourceip, sourceport, destinationip, destinationport,
      filename, path, filesize, filestreamtype, md5, header, details):
    self.sourceip = sourceip
    self.sourceport = sourceport
    self.destinationip = destinationip
    self.destinationport = destinationport
    self.filename = filename
    self.path = path
    self.filesize = filesize
    self.filestreamtype = filestreamtype
    self.md5 = md5
    self.header = header
    self.details = details


class NetworkMinerBaseParser(dsv_parser.DSVParser):



class NetWorkMinerParser(dsv_parser.DSVParser):
  NAME = 'networkminer'

  COLUMNS = []
