# Import python libs
from __future__ import absolute_import

import logging

log = logging.getLogger(__name__)

def beacon(config):
    ret_dict = dict()
    ret = list()
    result =  __salt__['pnda_service_restart.manageHadoopClusterRestart'] ()

    if result:
        ret_dict['Restarted']=True
        ret.append(ret_dict)
    else:
        ret_dict['Restarted']=False
        ret.append(ret_dict)
    return ret

