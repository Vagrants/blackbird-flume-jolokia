import pytest

import blackbird.plugins.base

from flume_jolokia import JMXItemsBase
from flume_jolokia import JMXChannelItems
from flume_jolokia import JMXSinkItems
from flume_jolokia import JMXSourceItems


class TestJMXItemsBase(object):
    class JMXTestItems(JMXItemsBase):
        @property
        def _mbean_pattern(self):
            return None

        @property
        def _attributes(self):
            return None

        @property
        def _zabbix_key(self):
            return None

        @property
        def _zabbix_discovery_key(self):
            return None

    def test_set_mbeans(self):
        jmx_test_items = self.JMXTestItems()
        mbeans = [
            'org.example.test:name=object1',
            'org.example.test:name=object2',
        ]

        jmx_test_items.set_mbeans(mbeans)

        assert sorted(mbeans) == sorted(
            jmx_test_items._JMXItemsBase__mbeans
        )

    def test_get_mbeans(self):
        jmx_test_items = self.JMXTestItems()
        mbeans = [
            'org.example.test:name=object1',
            'org.example.test:name=object2',
        ]

        jmx_test_items._JMXItemsBase__mbeans = mbeans

        assert sorted(mbeans) == sorted(jmx_test_items.get_mbeans())

    def test_mbeans_not_differ(self):
        jmx_test_items = self.JMXTestItems()
        initial_mbeans = [
            'org.example.test:name=object1',
            'org.example.test:name=object2',
        ]
        tested_mbeans = [
            'org.example.test:name=object2',
            'org.example.test:name=object1',
        ]

        jmx_test_items._JMXItemsBase__mbeans = initial_mbeans

        assert not jmx_test_items.mbeans_differ(tested_mbeans)

    def test_mbeans_actually_differ(self):
        jmx_test_items = self.JMXTestItems()
        initial_mbeans = [
            'org.example.test:name=object1',
            'org.example.test:name=object2',
        ]
        tested_mbeans = [
            'org.example.test:name=object3',
            'org.example.test:name=object2',
            'org.example.test:name=object1',
        ]

        jmx_test_items._JMXItemsBase__mbeans = initial_mbeans

        assert jmx_test_items.mbeans_differ(tested_mbeans)


class TestJMXChannelItems(object):
    def test_mbean_pattern(self):
        jmx_channel_items = JMXChannelItems()

        expected_return = 'org.apache.flume.channel:type=*'
        assert expected_return == jmx_channel_items.mbean_pattern()

    def test_attributes(self):
        jmx_channel_items = JMXChannelItems()

        expected_return = [
            'ChannelCapacity',
            'ChannelFillPercentage',
            'ChannelSize',
            'EventPutAttemptCount',
            'EventPutSuccessCount',
            'EventTakeAttemptCount',
            'EventTakeSuccessCount',
            'StartTime',
            'StopTime',
        ]
        assert expected_return == jmx_channel_items.attributes()

    def test_zabbix_key(self):
        jmx_channel_items = JMXChannelItems()
        mbean = 'org.apache.flume.channel:type=channel1'
        attribute = 'ChannelCapacity'

        expected_return = 'flume.channel[type=channel1,ChannelCapacity]'
        assert expected_return == jmx_channel_items.zabbix_key(
            mbean, attribute,
        )

    def test_zabbix_discovery_key(self):
        jmx_channel_items = JMXChannelItems()

        expected_return = 'flume.channel.discovery'
        assert expected_return == jmx_channel_items.zabbix_discovery_key()


class TestJMXSinkItems(object):
    def test_mbean_pattern(self):
        jmx_sink_items = JMXSinkItems()

        expected_return = 'org.apache.flume.sink:type=*'
        assert expected_return == jmx_sink_items.mbean_pattern()

    def test_attributes(self):
        jmx_sink_items = JMXSinkItems()

        expected_return = [
            'BatchCompleteCount',
            'BatchEmptyCount',
            'BatchUnderflowCount',
            'ConnectionClosedCount',
            'ConnectionCreatedCount',
            'ConnectionFailedCount',
            'EventDrainAttemptCount',
            'EventDrainSuccessCount',
            'StartTime',
            'StopTime',
        ]
        assert expected_return == jmx_sink_items.attributes()

    def test_zabbix_key(self):
        jmx_sink_items = JMXSinkItems()
        mbean = 'org.apache.flume.sink:type=sink1'
        attribute = 'BatchCompleteCount'

        expected_return = 'flume.sink[type=sink1,BatchCompleteCount]'
        assert expected_return == jmx_sink_items.zabbix_key(mbean, attribute)

    def test_zabbix_discovery_key(self):
        jmx_sink_items = JMXSinkItems()

        expected_return = 'flume.sink.discovery'
        assert expected_return == jmx_sink_items.zabbix_discovery_key()


class TestJMXSourceItems(object):
    def test_mbean_pattern(self):
        jmx_source_items = JMXSourceItems()

        expected_return = 'org.apache.flume.source:type=*'
        assert expected_return == jmx_source_items.mbean_pattern()

    def test_attributes(self):
        jmx_source_items = JMXSourceItems()

        expected_return = [
            'AppendAcceptedCount',
            'AppendBatchAcceptedCount',
            'AppendBatchReceivedCount',
            'AppendReceivedCount',
            'EventAcceptedCount',
            'EventReceivedCount',
            'OpenConnectionCount',
            'StartTime',
            'StopTime',
        ]
        assert expected_return == jmx_source_items.attributes()

    def test_zabbix_key(self):
        jmx_source_items = JMXSourceItems()
        mbean = 'org.apache.flume.source:type=source1'
        attribute = 'AppendAcceptedCount'

        expected_return = 'flume.source[type=source1,AppendAcceptedCount]'
        assert expected_return == jmx_source_items.zabbix_key(
            mbean, attribute,
        )

    def test_zabbix_discovery_key(self):
        jmx_source_items = JMXSourceItems()

        expected_return = 'flume.source.discovery'
        assert expected_return == jmx_source_items.zabbix_discovery_key()