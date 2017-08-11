{% set status = salt['kernel_reboot.required']() %}
{% if status %}
system.reboot:
  module.run:
    - at_time: 1
{% endif %}
