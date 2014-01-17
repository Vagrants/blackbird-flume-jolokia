#!/usr/bin/env python
# -*- encoding: utf-8 -*-

"""
Blackbird plugin for monitoring Apache Flume over Jolokia.
"""

import collections
import json
import pprint
import urllib2

import blackbird.plugins


class ConcreteJob(blackbird.plugins.base.JobBase):
    """
    Blackbird job for actual data collection.
    """
    options_factory = collections.namedtuple(
        'options',
        [
            'flume_channel',
            'flume_sink',
            'flume_source',
            'jolokia_context',
            'jolokia_host',
            'jolokia_port',
            'jolokia_timeout',
            'zabbix_hostname',
        ]
    )

    class ChannelFactory(object):
        __mbean = 'org.apache.flume.channel:type={0}'
        __attributes = [
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
        __zabbix_key = 'flume.channel[{0},{1}]'

        def __init__(self, channel_name):
            self.__channel_name = channel_name

        def mbean(self):
            return self.__mbean.format(self.__channel_name)

        def attributes(self):
            return self.__attributes

        def zabbix_key(self, attribute):
            return self.__zabbix_key.format(self.__channel_name, attribute)


    class SinkFactory(object):
        __mbean = 'org.apache.flume.sink:type={0}'
        __attributes = [
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
        __zabbix_key = 'flume.sink[{0},{1}]'

        def __init__(self, sink_name):
            self.__sink_name = sink_name

        def mbean(self):
            return self.__mbean.format(self.__sink_name)

        def attributes(self):
            return self.__attributes

        def zabbix_key(self, attribute):
            return self.__zabbix_key.format(self.__sink_name, attribute)


    class SourceFactory(object):
        __mbean = 'org.apache.flume.source:type={0}'
        __attributes = [
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
        __zabbix_key = 'flume.source[{0},{1}]'

        def __init__(self, source_name):
            self.__source_name = source_name

        def mbean(self):
            return self.__mbean.format(self.__source_name)

        def attributes(self):
            return self.__attributes

        def zabbix_key(self, attribute):
            return self.__zabbix_key.format(self.__source_name, attribute)


    def __init__(self, options, queue=None, logger=None):
        super(ConcreteJob, self).__init__(options, queue, logger)
        self.options = self.options_factory(
            flume_channel=options['flume_channel'],
            flume_sink=options['flume_sink'],
            flume_source=options['flume_source'],
            jolokia_context=options['jolokia_context'],
            jolokia_host=options['jolokia_host'],
            jolokia_port=options['jolokia_port'],
            jolokia_timeout=options['jolokia_timeout'],
            zabbix_hostname=options['zabbix_hostname'],
        )

        self.channel = self.ChannelFactory(self.options.flume_channel)
        self.sink = self.SinkFactory(self.options.flume_sink)
        self.source = self.SourceFactory(self.options.flume_source)

    def build_items(self):
        self.__build_items(self.channel)
        self.__build_items(self.sink)
        self.__build_items(self.source)

        # For fatal error(restarts thread itself)
        # raise blackbird.plugins.base.BlackbirdPluginError('Piyo')

    def __build_items(self, metrics):
        result = self.__jolokia_read(
            metrics.mbean(),
            metrics.attributes(),
        )

        for attribute in metrics.attributes():
            clock = result['timestamp']
            host = self.options.zabbix_hostname
            key = metrics.zabbix_key(attribute)
            value = result['value'][attribute]

            item = FlumeItem(clock, host, key, value)

            self.queue.put(item, block=False)
            self.logger.debug(item)

    def __jolokia_read(self, mbean, attributes):
        jolokia_result_json = urllib2.urlopen(
            url=self.__jolokia_url(),
            data=self.__jolokia_json_read(mbean, attributes),
            timeout=self.options.jolokia_timeout,
        )

        jolokia_result_dict = json.load(jolokia_result_json)

        return jolokia_result_dict

    def __jolokia_url(self):
        jolokia_url = 'http://{0}:{1}{2}/'.format(
            self.options.jolokia_host,
            self.options.jolokia_port,
            self.options.jolokia_context,
        )

        return jolokia_url

    @classmethod
    def __jolokia_json_read(cls, mbean, attributes):
        jolokia_dict = {}
        jolokia_dict['type'] = 'read'
        jolokia_dict['mbean'] = mbean
        jolokia_dict['attribute'] = attributes
        jolokia_json = json.dumps(jolokia_dict).encode('utf-8')

        return jolokia_json


class FlumeItem(blackbird.plugins.base.ItemBase):
    """
    Blackbird data type for collected flume metrics.
    """
    def __init__(self, clock, host, key, value):
        super(FlumeItem, self).__init__(key, value, host, clock)
        self.__data = {}
        self.__generate()

    @property
    def data(self):
        return self.__data

    def __generate(self):
        self.__data['clock'] = int(self.clock)
        self.__data['host'] = self.host
        self.__data['key'] = self.key

        if isinstance(self.value, float):
            self.__data['value'] = round(self.value, 6)
        else:
            self.__data['value'] = self.value

    def __str__(self):
        return str(self.__data)

    def __repr__(self):
        return repr(self.__data)


class Validator(blackbird.plugins.base.ValidatorBase):
    """
    Blackbird configuration validator and default value setter.
    """
    def __init__(self):
        self.__spec = None

    @property
    def spec(self):
        self.__spec = (
            '[{0}]'.format(__name__),
            'zabbix_hostname = string(default={0})'.format(
                self.detect_hostname()
            ),
            'jolokia_host = string(default="localhost")',
            'jolokia_port = integer(0, 65535)',
            'jolokia_context = string(default="/jolokia")',
            'jolokia_timeout = integer(default=10)',
            'flume_channel = string',
            'flume_sink = string',
            'flume_source = string',
        )
        return self.__spec


if __name__ == '__main__':
    pass
