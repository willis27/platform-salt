'''
Module for to check system reboot
'''

# Import python libs
from __future__ import absolute_import

import logging

log = logging.getLogger(__name__)

def required ():
    """ returns system needs reboot required or not """
    ret = []
    ret_dict={}
    os = __salt__['grains.item']('os')

    if os['os'] == "CentOS" or os['os'] == "RedHat":
        try:
            current_version = __salt__['cmd.run']('uname -r')
	    latest_version = __salt__['cmd.run']('rpm -q --last kernel')
	    latest_version = latest_version.split(" ")
	    latest_version = [version for version in latest_version if 'kernel' in version]
	    latest_version = str(latest_version[0]).strip('kernel-')
	    if current_version == latest_version:
	        return False
        except:
           return False
        return True

    return  __salt__['file.file_exists']('/var/run/reboot-required')


