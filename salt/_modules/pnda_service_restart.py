import sys
import re
import time
import json
import logging
import traceback
from cm_api.api_client import ApiResource
from cm_api.endpoints import hosts


logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)

#retry the start service only for every DOWN_COUNT_MAX 
DOWN_COUNT_MAX = 10
#maximum retry count is RETRY_COUNT_MAX
RETRY_COUNT_MAX = 3
#retry_count will reset after RETRY_COUNT_MAX 
RETRY_COUNT_RESET = 10

CMS_SERVICE_LIST = ['SERVICEMONITOR','ALERTPUBLISHER','EVENTSERVER','HOSTMONITOR']

#pnda_service_restart.manageHadoopClusterRestart
def manageHadoopClusterRestart():
    #hadoop_distro = __salt__['pillar.get']('hadoop.distro')
    hadoop_distro = "CDH"
    #for service in ["ZOOKEEPER", "HDFS","HBASE", "YARN", "HIVE", "SPARK_ON_YARN", "IMPALA", "OOZIE", "HUE"]:
    try:
        serviceList = __salt__['grains.get']('serviceList')
        if not serviceList:
           serviceList = {}
    except:
        serviceList = {}
    dependencyList={
        "SERVICEMONITOR": [],
        "HOSTMONITOR": [],
        "ALERTPUBLISHER": ['SERVICEMONITOR'],
        "EVENTSERVER": ['SERVICEMONITOR'],
        "ZOOKEEPER":['SERVICEMONITOR'],
        "HDFS":['ZOOKEEPER'],
        "HBASE":["ZOOKEEPER","HDFS"],
        "YARN":["ZOOKEEPER","HDFS"],
        "HIVE":["ZOOKEEPER","YARN"],
        "SPARK_ON_YARN":["YARN"],
        "IMPALA":["HBASE","HDFS","HIVE"],
        "OOZIE":["ZOOKEEPER","YARN"],
        "HUE":["ZOOKEEPER","HBASE","HIVE","IMPALA","OOZIE"]
    }

    connection_object = check_connectivity()
    if not connection_object :
        log.error("CDH Connection issue")
        return False

	#check the CMS and hadoop status and created in Grains(serviceList)
    serviceList = getServiceStatus(connection_object,hadoop_distro=hadoop_distro,serviceList=serviceList)

	#check the CMS and hadoop status and start the service node based
    result = checkStatusandTrigger(connection_object,hadoop_distro=hadoop_distro,serviceList=serviceList,dependencyList=dependencyList)

    return result



def check_connectivity():
    hadoop_distro = __salt__['pillar.get']('hadoop.distro')
    if hadoop_distro == 'CDH':
        cm_host = __salt__['pnda.hadoop_manager_ip']()
        cm_user = __salt__['pillar.get']('admin_login:user')
        cm_pass = __salt__['pillar.get']('admin_login:password')
        try:
            cm_api = ApiResource(cm_host, version=11, username=cm_user, password=cm_pass)
            if not cm_api:
                return False
            return cm_api
        except:
            return False
    else:
        log.error ("HDP not implemented")
        return False

def getServiceStatus(connection_object,hadoop_distro,serviceList):
    if hadoop_distro == 'CDH':
        #get cluster name
        for cluster_detail in connection_object.get_all_clusters():
            cluster_name = cluster_detail.name
            break
        cluster_manager = connection_object.get_cluster(cluster_name)

        #Cloudra management services
        cloudera_manager = connection_object.get_cloudera_manager()
        cms_service = cloudera_manager.get_service()
        for role in cms_service.get_all_roles():
            role_name = str(role.type)
            if role_name not in serviceList.keys():
                serviceList[role_name] = {}
            name = hosts.get_host(role._resource_root, role.hostRef.hostId)
            hostname = str(name.hostname)
            if role_name  not in serviceList[role_name].keys():
               serviceList[role_name][role_name] = {hostname:{}}
            elif hostname not in serviceList[role_name][role_name].keys():
                 serviceList[role_name][role_name][hostname] = {}
            status = "Maintenance" if role.maintenanceMode else str(role.entityStatus)
            serviceList[role_name][role_name][hostname]["status"] = status
            serviceList[role_name]['status'] = str(status)
		
		#Hadoop Services
        for service in cluster_manager.get_all_services():
            service_name = str(service.type)
            if service_name not in serviceList.keys():
               serviceList[service_name] = {}
            for role in service.get_all_roles():
                role_name = str(role.type)
                name = hosts.get_host(role._resource_root, role.hostRef.hostId)
                hostname = str(name.hostname)
                if role_name  not in serviceList[service_name].keys():
                   serviceList[service_name][role_name] = {hostname:{}}
                elif hostname not in serviceList[service_name][role_name].keys():
                   serviceList[service_name][role_name][hostname] = {}
                status = "Maintenance" if role.maintenanceMode else str(role.entityStatus)
                serviceList[service_name][role_name][hostname]["status"] = status
            serviceList[service_name]['status'] = str(service.entityStatus)
    #Grain update not working properly , so reset value and added new value
    __salt__['grains.set']("serviceList",{},True)
    __salt__['grains.set']("serviceList".format(service_name),serviceList,True)
    return serviceList

def checkDependency(service_name,serviceList,dependencyList ):
    for sname in dependencyList[service_name]:
        status = serviceList[sname]['status']
        if not re.search("GOOD_HEALTH|NONE",str(status)):
            return False

    return True

def checkStatusandTrigger(connection_object,hadoop_distro,serviceList,dependencyList):
    result = True
    for service_name in serviceList.keys():
        for role_name in serviceList[service_name]:
            if role_name == "status":
               continue
            for node_name in serviceList[service_name][role_name]:
                status = serviceList[service_name][role_name][node_name]['status']
                counters = {}
                try :
                   counters['up_count'] = serviceList[service_name][role_name][node_name]['up_count']
                   counters['down_count'] = serviceList[service_name][role_name][node_name]['down_count']
                   counters['retry_count'] = serviceList[service_name][role_name][node_name]['retry_count']
                except:
                   counters['up_count'] = 0
                   counters['down_count'] = 0
                   counters['retry_count'] = 0

                if re.search("Maintenance",str(status)):
                   log.debug("Maintenance mode")
                   pass
                elif not re.search("GOOD_HEALTH|NONE",str(status)):
                   log.debug("{0} {1} {2} {3} ".format(service_name,role_name,node_name,status))
                   log.debug("{0}  ".format(counters))
                   counters['up_count'] = 0
                   dep = checkDependency(service_name=service_name,serviceList=serviceList,dependencyList=dependencyList)
                   if dep and counters['retry_count'] < RETRY_COUNT_MAX and  counters['down_count'] > DOWN_COUNT_MAX :
                       counters['retry_count'] += 1
                       counters['down_count'] = 0
                       if not startService(connection_object,service_name=service_name,role_name=role_name,node_name=node_name):
                          result = False
                   else:
                       counters['down_count'] += 1
                   log.debug("{0}  ".format(counters))
                else:
                   counters['up_count'] += 1
                   if counters['up_count'] > RETRY_COUNT_RESET:
                      counters['retry_count'] = 0
                #update Counters
                serviceList[service_name][role_name][node_name].update(counters)
    #Grain update not working properly , so reset value and added new value
    __salt__['grains.set']("serviceList",{},True)
    __salt__['grains.set']("serviceList".format(service_name),serviceList,True)
    return result

def startService(connection_object,service_name,role_name,node_name):
    if service_name in CMS_SERVICE_LIST:
        cloudera_manager = connection_object.get_cloudera_manager()
        cms_service = cloudera_manager.get_service()
        for role in cms_service.get_all_roles():
            if str(role.type) == service_name:
               status,message  = wait_on_command(cms_service.start_roles(role.name))
               break
    else:
        for cluster_detail in connection_object.get_all_clusters():
            cluster_name = cluster_detail.name
            break
        cluster_manager = connection_object.get_cluster(cluster_name)
        for service in cluster_manager.get_all_services():
            if  service_name != service.type : continue
            for role in service.get_all_roles():
                if  role_name != role.type : continue
                name = hosts.get_host(role._resource_root, role.hostRef.hostId)
                hostname = str(name.hostname)
                if  node_name != hostname : continue
                status,message  = wait_on_command(service.start_roles(role.name))
                break
    if not status:
       log.error("{0} service start failed, error message : {1}".format(service_name,message))
    return status


#Verify the command execution completed
def wait_on_command(cmds):
    messages = []
    success = False
    for cmd in cmds :
        logging.debug('Executing %s', cmd.name)
        while cmd.active is True and cmd.success is None:
           time.sleep(5)
           cmd = cmd.fetch()
        if cmd.active is None:
            messages.append('%s (cmd.active is None)' % cmd.resultMessage)
        if cmd.success is False:
            log.error('%s (cmd.success is False)' % cmd.resultMessage)
            messages.append('%s (cmd.success is False)' % cmd.resultMessage)
        elif cmd.success is None:
            log.error('%s (cmd.success is None)' % cmd.resultMessage)
            messages.append('%s (cmd.success is None)' % cmd.resultMessage)
        elif cmd.success is True:
            success = True
    return success, messages

