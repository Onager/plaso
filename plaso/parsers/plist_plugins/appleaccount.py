# -*- coding: utf-8 -*-
"""Plist parser plugin for Apple Account plist files."""

from dfdatetime import time_elements as dfdatetime_time_elements

from plaso.containers import plist_event
from plaso.containers import time_events
from plaso.lib import definitions
from plaso.parsers import plist
from plaso.parsers.plist_plugins import interface


class AppleAccountPlugin(interface.PlistPlugin):
  """Plist parser plugin for Apple Account plist files.

  Further details about fields within the key:
    Accounts: account name.
    FirstName: first name associated with the account.
    LastName: family name associate with the account.
    CreationDate: timestamp when the account was configured in the system.
    LastSuccessfulConnect: last time when the account was connected.
    ValidationDate: last time when the account was validated.
  """

  NAME = 'apple_id'
  DATA_FORMAT = 'Apple account information plist file'

  PLIST_PATH_FILTERS = frozenset([
      interface.PrefixPlistPathFilter(
          'com.apple.coreservices.appleidauthenticationinfo')])

  PLIST_KEYS = frozenset(['AuthCertificates', 'AccessorVersions', 'Accounts'])

  # pylint: disable=arguments-differ
  def _ParsePlist(self, parser_mediator, match=None, **unused_kwargs):
    """Extracts relevant Apple Account entries.

    Args:
      parser_mediator (ParserMediator): mediates interactions between parsers
          and other components, such as storage and dfvfs.
      match (Optional[dict[str: object]]): keys extracted from PLIST_KEYS.
    """
    accounts = match.get('Accounts', {})
    for name_account, account in accounts.items():
      first_name = account.get('FirstName', '<FirstName>')
      last_name = account.get('LastName', '<LastName>')
      general_description = f'{name_account:s} ({first_name:s} {last_name:s})'

      event_data = plist_event.PlistTimeEventData()
      event_data.key = name_account
      event_data.root = '/Accounts'

      datetime_value = account.get('CreationDate', None)
      if datetime_value:
        event_data.desc = f'Configured Apple account {general_description:s}'

        date_time = dfdatetime_time_elements.TimeElementsInMicroseconds()
        date_time.CopyFromDatetime(datetime_value)

        event = time_events.DateTimeValuesEvent(
            date_time, definitions.TIME_DESCRIPTION_WRITTEN)
        parser_mediator.ProduceEventWithEventData(event, event_data)

      datetime_value = account.get('LastSuccessfulConnect', None)
      if datetime_value:
        event_data.desc = f'Connected Apple account {general_description:s}'

        date_time = dfdatetime_time_elements.TimeElementsInMicroseconds()
        date_time.CopyFromDatetime(datetime_value)

        event = time_events.DateTimeValuesEvent(
            date_time, definitions.TIME_DESCRIPTION_WRITTEN)
        parser_mediator.ProduceEventWithEventData(event, event_data)

      datetime_value = account.get('ValidationDate', None)
      if datetime_value:
        event_data.desc = f'Last validation Apple account {general_description:s}'

        date_time = dfdatetime_time_elements.TimeElementsInMicroseconds()
        date_time.CopyFromDatetime(datetime_value)

        event = time_events.DateTimeValuesEvent(
            date_time, definitions.TIME_DESCRIPTION_WRITTEN)
        parser_mediator.ProduceEventWithEventData(event, event_data)


plist.PlistParser.RegisterPlugin(AppleAccountPlugin)
