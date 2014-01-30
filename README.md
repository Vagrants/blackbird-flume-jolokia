blackbird-flume-jolokia
=======================

Blackbird plugin for collecting MBeans of Apache Flume via Jolokia.
Currently supports Apache Flume >= 1.5 .


Blackbird configuration file
----------------------------

| key               | type    | default             | notes                                                                        |
|-------------------|---------|---------------------|------------------------------------------------------------------------------|
| `module`          | string  |                     | Mandatory. `flume_jolokia` is the only appropriate choice.                   |
| `interval`        | integer | `60`                | Interval of standard item collection.                                        |
| `lld_interval`    | integer | `600`               | Interval of low level discovery item collection.                             |
| `zabbix_hostname` | string  | `detect_hostname()` | Hostname on Zabbix.                                                          |
| `jolokia_host`    | string  | `localhost`         | Address of Jolokia.                                                          |
| `jolokia_port`    | integer |                     | Mandatory. Port number of Jolokia.                                           |
| `jolokia_context` | string  | `/jolokia`          | Context of Jolokia. Must contain leading `/`, must not contain trailing `/`. |
| `jolokia_timeout` | integer | `10`                | Timeout when connecting to Jolokia.                                          |
