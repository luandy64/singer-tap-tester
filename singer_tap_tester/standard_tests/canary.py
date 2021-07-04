from singer_tap_tester import cli, partition, user

def test_sync_canary(scenario):
    """
    The purpose of this test is to run the tap with all streams
    selected to naively exercise all code and any changes to those
    streams.

    This is certainly not comprehensive, and assumes that all
    streams have data, but is generally enough to provide some
    value.

    Assertions made:
    - we get get at least one RECORD, STATE, and SCHEMA message
    - every stream syncs at least one record
    """
    catalog = cli.run_discovery(scenario.tap_name, scenario.get_config())
    new_catalog = user.select_all_streams_and_fields(catalog)
    tap_output = cli.run_sync(scenario.tap_name, scenario.get_config(), new_catalog, {})
    # TODO: Run through target's validating handler
    # TODO: Generate a final summary report so that this test can be used to gauge the potential usefulness of the specific data set available to the test author/runner

    messages_by_type = partition.by_type(tap_output)
    for message_type, messages in messages_by_type.items():
        with scenario.subTest(message_type=message_type):
            # Assert that we get get at least one RECORD, STATE, and SCHEMA message
            scenario.assertGreater(len(messages), 0)

            # Assert that every stream syncs at least one record
            if message_type == 'RECORD':
                for stream, records in messages.items():
                    with scenario.subTest(stream=stream):
                        scenario.assertGreater(len(records), 50)
