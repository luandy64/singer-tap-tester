import json
import os
import unittest
from singer_tap_tester import cli

class TestRunSync(unittest.TestCase):
    file_name = 'some_cache_file'

    @unittest.mock.patch('singer_tap_tester.cli.__run_tap')
    @unittest.mock.patch('json.loads')
    def test_run_sync_use_cache_enabled_cache_hit(self, mocked_json_loads, mocked_run_tap):
        os.environ['SINGER_TAP_TESTER_USE_CACHE'] = 'true'
        os.environ['SINGER_TAP_TESTER_CACHE_FILE'] = self.file_name

        # Ensure the cache file exists
        if os.path.exists(self.file_name):
            os.remove(self.file_name)
        open(self.file_name, 'a').close()

        # Call run_sync() to maybe call __run_tap()
        cli.run_sync(None, None, None, None)

        mocked_run_tap.assert_not_called()


    @unittest.mock.patch('singer_tap_tester.cli.__run_tap')
    @unittest.mock.patch('json.loads')
    def test_run_sync_use_cache_enabled_cache_miss(self, mocked_json_loads, mocked_run_tap):
        os.environ['SINGER_TAP_TESTER_USE_CACHE'] = 'true'
        os.environ['SINGER_TAP_TESTER_CACHE_FILE'] = self.file_name

        # Ensure the cache file does not exists
        if os.path.exists(self.file_name):
            os.remove(self.file_name)

        # Call run_sync() to maybe call __run_tap()
        try:
            cli.run_sync(None, None, None, None)
        except TypeError:
            # We are able to call run_sync, but it will try to write to
            # the cache, which won't work with the Mock
            pass

        mocked_run_tap.assert_called_once()
        mocked_run_tap.assert_called_with(None, config=None, catalog=None, state=None)

    @unittest.mock.patch('singer_tap_tester.cli.__run_tap')
    @unittest.mock.patch('json.loads')
    def test_run_sync_use_cache_disabled_cache_hit(self, mocked_json_loads, mocked_run_tap):
        os.environ['SINGER_TAP_TESTER_USE_CACHE'] = 'false'
        os.environ['SINGER_TAP_TESTER_CACHE_FILE'] = self.file_name

        # Ensure the cache file exists
        if os.path.exists(self.file_name):
            os.remove(self.file_name)
        open(self.file_name, 'a').close()

        # Call run_sync() to maybe call __run_tap()
        cli.run_sync(None, None, None, None)

        mocked_run_tap.assert_called_once()
        mocked_run_tap.assert_called_with(None, config=None, catalog=None, state=None)

    @unittest.mock.patch('singer_tap_tester.cli.__run_tap')
    def test_run_sync_use_cache_disabled_cache_miss(self, mocked_run_tap):
        os.environ['SINGER_TAP_TESTER_USE_CACHE'] = 'false'
        os.environ['SINGER_TAP_TESTER_CACHE_FILE'] = self.file_name

        # Ensure the cache file does not exists
        if os.path.exists(self.file_name):
            os.remove(self.file_name)

        # Call run_sync() to maybe call __run_tap()
        cli.run_sync(None, None, None, None)

        mocked_run_tap.assert_called_once()
        mocked_run_tap.assert_called_with(None, config=None, catalog=None, state=None)
