{% if grains['os'] == 'Ubuntu' %}
tasks-run_apt_update:
  cmd.run:
    - name: 'apt-get update -y --force-yes'
{% elif grains['os'] == 'RedHat' %}
tasks-update_system:
  pkg.uptodate:
    - refresh: True
{% endif %}
