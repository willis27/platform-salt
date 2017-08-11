{% set data = salt.pillar.get('event_data') %}
Call the system reboot entry sls:
    salt.state:
      - tgt: {{ data['data']['id'] }}
      - sls:  
        - reboot.kernel_entry
      - kwarg:
        pillar:
          file_exist: {{ data['data']['file_exist'] }}

