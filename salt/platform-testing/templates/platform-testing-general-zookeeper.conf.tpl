description "Platform testing general zookeeper upstart script"
author      "PNDA team"
start on runlevel [2345]
task
exec python {{ platform_testing_directory }}/{{platform_testing_package}}/monitor.py --plugin zookeeper --postjson http://{{ console_hosts|join(',') }}/metrics --extra "--zconnect {{ kafka_zookeepers|join(',') }}"
