#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import argparse
import datetime
import csv
import os
from plaso.cli import pinfo_tool
from plaso.cli import tools
import plaso.lib.errors
import json
from google.cloud import storage


class PlasoCIFetcher(object):
  RESULTS_ROOT = 'build_results'

  def __init__(self, bucket_name='', project_name='', storage_file_temporary_directory=''):
    super(PlasoCIFetcher, self).__init__()
    self._storage_file_temporary_directory = storage_file_temporary_directory
    self._bucket_name = bucket_name
    self._project_name = project_name

  def GetStorageFileBlobs(self, test_name):
    """Iterates over plaso storage files for an end-to-end test.

    Args:
      test_name (str): name of test to get storage files for.

    Yields: names of all storage files for the given test.
    """
    storage_client = storage.Client(project=self._project_name)
    bucket = storage_client.get_bucket(self._bucket_name)
    prefix = '{0:s}/{1:s}'.format(self.RESULTS_ROOT, test_name)
    for blob in bucket.list_blobs(prefix=prefix):
      if blob.name.endswith('.plaso'):
        yield blob

  def _GetNameForBlob(self, blob):
    """Sanitizes a blog name into something that can be a path."""
    return blob.name.replace('/', '!')

  def DownloadStorageFiles(self, test_name):
    """Downloads all the storage files for a given test."""
    # downloaded_files = list()
    storage_file_dir = os.path.join(
        self._storage_file_temporary_directory, test_name)
    try:
      os.makedirs(storage_file_dir)
    except OSError:
      # Directory already exists.
      pass
    for blob in self.GetStorageFileBlobs(test_name):
      temp_name = self._GetNameForBlob(blob)
      blob_path = os.path.join(storage_file_dir, temp_name)
      if os.path.exists(blob_path):
        # Don't download already downloaded files.
        print('Not downloading {0:s}. File already exists.'.format(blob_path))
        yield blob_path
        continue
      with open(blob_path, b'w') as file_object:
        print('Downloading file {0:s}'.format(temp_name))
        blob.download_to_file(file_object)
      yield blob_path

  def UploadMetadataFile(self, filename, test_name):
    """Uploads the results of processing back to the cloud bucket."""
    storage_file_dir = os.path.join(self._storage_file_temporary_directory,
        test_name)
    if not filename.startswith(storage_file_dir):
      raise ValueError
    translated_name = filename.replace('!', '/')
    translated_name = translated_name.replace(storage_file_dir, '')
    storage_client = storage.Client(project=self._project_name)
    bucket = storage_client.get_bucket(self._bucket_name)
    # blob_name = "{0:s}/{1:s}".format(self.RESULTS_ROOT, translated_name)
    print('Uploading file {0:s}'.format(filename))
    blob = bucket.blob(translated_name)
    blob.upload_from_filename(filename)


def ProcessTest(test_name, project_name, storage_file_temporary_directory):
  fetcher = PlasoCIFetcher(
      project_name=project_name, storage_file_temporary_directory=storage_file_temporary_directory)
  for storage_file in fetcher.DownloadStorageFiles(test_name):
    filename, _, _ = storage_file.partition('.')
    output_path = '{0:s}.{1:s}'.format(filename, 'json')
    if os.path.exists(output_path):
      continue
    try:
      with open(output_path, b'w') as output_file:
        output_writer = tools.FileObjectOutputWriter(output_file)
        tool = pinfo_tool.PinfoTool(output_writer=output_writer)
        tool._storage_file_path = storage_file
        tool._output_format = 'json'
        tool.PrintStorageInformation()
    except plaso.lib.errors.BadConfigOption:
      # File couldn't be processed.
      os.unlink(output_path)
      continue

    fetcher.UploadMetadataFile(output_path, test_name)


def BuildCSV(test_name, storage_file_temporary_directory, metric_file_name):
  """

  Args:
    test_name:
    storage_file_temporary_directory:
    metric_file_name:

  Returns:

  """
  path = storage_file_temporary_directory
  fieldnames = [
    'start_date', 'elapsed_time', 'number_of_parsers', 'total_events']
  metrics_rows = []
  storage_file_dir = os.path.join(storage_file_temporary_directory,
      test_name)
  for filename in os.listdir(storage_file_dir):
    file_path = '{0:s}{1:s}/{2:s}'.format(path, test_name, filename)
    if 'json' not in filename:
      continue
    if 'gold' in filename:
      continue
    with open(file_path, 'r') as metric_file:
      results = json.load(metric_file)
      session = results.items()[0][1]
      metrics_row = {}
      start_date = datetime.datetime.utcfromtimestamp(
          session['start_time'] / 1000000)
      elapsed_time = (session['completion_time'] - session[
        'start_time']) / 100000
      number_of_parsers = len(session['enabled_parser_names'])

      # Add counts from parsers
      for parser_name, event_count in session['parsers_counter'].items():
        if parser_name in ['__type__']:
          continue
        if event_count > 0:
          csv_field_name = '{0:s}_events'.format(parser_name)
          if csv_field_name not in fieldnames:
            fieldnames.append(csv_field_name)
          metrics_row[csv_field_name] = event_count

      events_counter = session['parsers_counter'].values()
      events_counter.remove('collections.Counter')
      total_events = sum(events_counter)
      metrics_row['start_date'] = start_date
      metrics_row['elapsed_time'] = elapsed_time
      metrics_row['number_of_parsers'] = number_of_parsers
      metrics_row['total_events'] = total_events
      metrics_rows.append(metrics_row)

  metric_file_name = os.path.join('/tmp', metric_file_name)
  with open(metric_file_name, b'w') as csvfile:
    csvwriter = csv.DictWriter(csvfile, fieldnames=fieldnames, restval=0)
    csvwriter.writeheader()
    rows = sorted(metrics_rows)
    for row in rows:
      csvwriter.writerow(row)


if __name__ == '__main__':
  argument_parser = argparse.ArgumentParser()

  argument_parser.add_argument(
      'temporary_directory', type=str,
      help='Path to a temporary directory to cache storage files')

  argument_parser.add_argument(
      'project_name', type=str,
      help='Project where the tests were run')

  argument_parser.add_argument(
      'bucket_name', type=str,
      help='Bucket where test results are stored')

  options = argument_parser.parse_args()

  test_names = [
    'plaso_registrar_end_to_end', 'plaso_studentpc1_end_to_end',
    'plaso_dean_end_to_end', 'plaso_acserver_end_to_end',
    'plaso_end_to_end_windows_studentpc1']
  for test in test_names:
    ProcessTest(test, project_name=options.project_name, )
    output_name = '{0:s}_metrics.csv'.format(test)
    BuildCSV(test, storage_file_temporary_directory=options.temporary_directory,
    metric_file_name=output_name)
