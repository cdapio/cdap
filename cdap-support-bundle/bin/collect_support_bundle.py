# Copyright © 2023 Cask Data, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

# collect_support_bundle.py to get logs from CDAP instance deployed on kubernetes.

# To diagnose the issues with cdap system services and get the information about CDAP installation, run the below command
# python3 collect_support_bundle.py -n ${kube-namespace} -u ${username} -p ${password}
# with kube-namespace value(default is default) and username and password of CDAP system if authentication is available

# To get the logs for failed pipeline run, run the same command with parameters for pipeline-name(-l), run-id(-r) and runtime-namespace(-t).
# To avoid cleaning up of the resources after a failed run, run the pipeline with property
# system.runtime.cleanup.disabled = true in runtime arguments

import argparse
import base64
import json
from json.decoder import JSONDecodeError
import os
import shutil
import subprocess
import sys
from datetime import datetime
import re

parser = argparse.ArgumentParser(description='Log collector program for CDAP.')
parser.add_argument(
    "--kube-namespace",
    "-n",
    help="Kubernetes namespace where CDAP is installed",
    type=str,
    default="default"
)
parser.add_argument(
    "--username",
    "-u",
    help="CDAP User Name",
    type=str
)
parser.add_argument(
    "--password",
    "-p",
    help="CDAP Password",
    type=str
)
parser.add_argument(
    "--cdap-ns",
    "-c",
    help="CDAP namespace",
    type=str,
    default="default"
)
parser.add_argument(
    "--pipeline-name",
    "-l",
    help="CDAP pipeline name",
    type=str
)
parser.add_argument(
    "--run-id",
    "-r",
    help="CDAP Run id",
    type=str
)
parser.add_argument(
    "--runtime-namespace",
    "-t",
    help="CDAP Runtime namespace. Namespace where pipeline is running",
    type=str,
    default="default"
)
def main(kube_namespace, username, password, cdap_ns, pipeline_name, run_id, runtime_namespace):

    fetch_instance_command = f'kubectl get cdapmasters.cdap.cdap.io --no-headers -o custom-columns=":metadata.name" -n {kube_namespace}'
    instance_name = subprocess.getoutput(fetch_instance_command)
    if(instance_name == "" or instance_name is None):
        print(f"Error : Unable to fetch the instance name. Please check for namespace in input parameters, either it's invalid or no instance is available in {kube_namespace}")
        exit()

    auth_header = ""
    if username is not None and password is not None:
        passToken = base64.b64encode(bytes(f'{username}:{password}', 'utf-8'))
        passToken = passToken.decode('utf-8')
        output = subprocess.getoutput(f'kubectl -n {kube_namespace} exec -t cdap-{instance_name}-appfabric-0 -c appfabric -- bash -c \'curl -s cdap-{instance_name}-external-authentication:10009/token -H "Authorization:Basic {passToken}"\'')
        try:
            str_obj = json.loads(output)
            auth_header = f"-H \"Authorization: Bearer {str_obj['access_token']}\""
        except JSONDecodeError as e:
            print(e)
            print('Error : Unable to authenticate due to Invalid credentials. Please check for username and password in input parameters')
            exit()
    else:
        output_check_auth = subprocess.getoutput(
            f'kubectl exec -t cdap-{instance_name}-runtime-0 -n {kube_namespace} -c runtime -- bash -c \'curl -s cdap-{instance_name}-router:11015/v3/system/services/status {auth_header}\'')
        try:
            str_obj_check_auth = json.loads(output_check_auth)
            if "appfabric" in str_obj_check_auth:
                print("Authentication Disabled. Log extraction process started.")
            else:
                print('Error : Authentication is enabled for the kubernetes namespace provided, please also provide username and password parameter while running the script')
                exit()

        except JSONDecodeError as e:
            print(f"message : {e}")
            print('Error : Authentication is enabled for the kubernetes namespace provided, please also provide username and password parameter while running the script')
            exit()


    update_progress(1)
    current_datetime = datetime.now()

    current_date_time = current_datetime.strftime("%m-%d-%YT%H%M%S")
    directoryName = "cdap-logs-" + current_date_time

    parent_dir = os.getcwd() + '/'
    path = os.path.join(parent_dir, directoryName)

    os.mkdir(path)
    update_progress(4)
    config_file = "config_file"
    pod_info = "pod_info"
    job_info = "job_info"
    config_map_info = "config_map_info"
    deployment_info = "deployment_info"
    node_info = "node_info"
    sts_info = "stateful_set_info"
    services_info = "services_info"
    memory_info = "memory_info"
    cdap_services_logs = "cdap_services_logs"
    cdap_services_status = "cdap_services_status"
    pipeline_logs = "pipeline_logs"
    runtime_namespace_logs = "runtime_namespace_logs"

    cdapCommandLists = [
        {
            "command": f'kubectl top nodes',
            "fileName": 'kubectl-top-nodes',
            "fileExt": 'log',
            "fileWrite": True,
            "forEachNode": False,
            "forEachCommand": '',
            "placeholderString": "",
            "parentFolder": node_info
        },
        {
            "command": f'kubectl get nodes',
            "fileName": 'kubectl-get-nodes',
            "fileExt": 'log',
            "fileWrite": True,
            "forEachNode": False,
            "forEachCommand": '',
            "placeholderString": "",
            "parentFolder": node_info
        },
        {
            "command": f'kubectl get nodes --no-headers -o custom-columns=":metadata.name"',
            "fileName": 'kubectl-get-nodes',
            "fileExt": 'log',
            "fileWrite": False,
            "forEachNode": True,
            "forEachCommand": f'kubectl describe node <node>',
            "placeholderString": "<node>",
            "childFileName": "kubectl-describe-node",
            "parentFolder": node_info
        },
        {
            "command": f'kubectl get jobs -n {kube_namespace}',
            "fileName": 'kubectl-get-jobs',
            "fileExt": 'log',
            "fileWrite": True,
            "forEachNode": False,
            "forEachCommand": '',
            "placeholderString": "",
            "parentFolder": job_info
        },
        {
            "command": f'kubectl get jobs -n {kube_namespace} --no-headers -o custom-columns=":metadata.name"',
            "fileName": 'kubectl-get-jobs',
            "fileExt": 'log',
            "fileWrite": False,
            "forEachNode": True,
            "forEachCommand": f'kubectl describe jobs <job> -n {kube_namespace}',
            "placeholderString": "<job>",
            "childFileName": "kubectl-describe-job",
            "parentFolder": job_info
        },
        {
            "command": f'kubectl get cdapmaster -n {kube_namespace} -o yaml',
            "fileName": 'kubectl-get-cdapmaster-yaml',
            "fileExt": 'yaml',
            "fileWrite": True,
            "forEachNode": False,
            "forEachCommand": '',
            "placeholderString": "",
            "parentFolder": config_file
        },
        {
            "command": f'kubectl get pods -n {kube_namespace} -o wide',
            "fileName": 'kubectl-get-pods',
            "fileExt": 'log',
            "fileWrite": True,
            "forEachNode": False,
            "forEachCommand": '',
            "placeholderString": "",
            "parentFolder": pod_info
        },
        {
            "command": f'kubectl get pods --no-headers -o custom-columns=":metadata.name" -n {kube_namespace}',
            "fileName": 'kubectl-get-pods',
            "fileExt": 'log',
            "fileWrite": False,
            "forEachNode": True,
            "placeholderString": "<pod>",
            "forEachCommand": f'kubectl exec -ti <pod> -n {kube_namespace} -- df -h',
            "childFileName": "kubectl-exec-ti-df-h",
            "parentFolder": memory_info
        },
        {
            "command": f'kubectl get pods --no-headers -o custom-columns=":metadata.name" -n {kube_namespace}',
            "fileName": 'kubectl-get-pods',
            "fileExt": 'log',
            "fileWrite": False,
            "forEachNode": True,
            "placeholderString": "<pod>",
            "forEachCommand": f'kubectl describe pod <pod> -n {kube_namespace}',
            "childFileName": "kubectl-describe-pod",
            "parentFolder": pod_info
        },
        {
            "command": f'kubectl get svc -n {kube_namespace}',
            "fileName": 'kubectl-get-svc',
            "fileExt": 'log',
            "fileWrite": True,
            "forEachNode": False,
            "forEachCommand": '',
            "placeholderString": "",
            "parentFolder": services_info
        },
        {
            "command": f'kubectl get svc --no-headers -o custom-columns=":metadata.name" -n {kube_namespace}',
            "fileName": 'kubectl-get-svc',
            "fileExt": 'log',
            "fileWrite": False,
            "forEachNode": True,
            "placeholderString": "<svc>",
            "forEachCommand": f'kubectl describe svc <svc> -n {kube_namespace}',
            "childFileName": "kubectl-describe-svc",
            "parentFolder": services_info
        },
        {
            "command": f'kubectl get sts -n {kube_namespace}',
            "fileName": 'kubectl-get-sts',
            "fileExt": 'log',
            "fileWrite": True,
            "forEachNode": False,
            "forEachCommand": '',
            "placeholderString": "",
            "parentFolder": sts_info
        },
        {
            "command": f'kubectl get sts --no-headers -o custom-columns=":metadata.name" -n {kube_namespace}',
            "fileName": 'kubectl-get-sts',
            "fileExt": 'log',
            "fileWrite": False,
            "forEachNode": True,
            "placeholderString": "<sts>",
            "forEachCommand": f'kubectl describe sts <sts> -n {kube_namespace}',
            "childFileName": "kubectl-describe-sts",
            "parentFolder": sts_info
        },
        {
            "command": f'kubectl get deployment -n {kube_namespace}',
            "fileName": 'kubectl-get-deployment',
            "fileExt": 'log',
            "fileWrite": True,
            "forEachNode": False,
            "forEachCommand": '',
            "placeholderString": "",
            "parentFolder": deployment_info
        },
        {
            "command": f'kubectl get deployment --no-headers -o custom-columns=":metadata.name" -n {kube_namespace}',
            "fileName": 'kubectl-get-deployment',
            "fileExt": 'log',
            "fileWrite": False,
            "forEachNode": True,
            "placeholderString": "<deploy>",
            "forEachCommand": f'kubectl describe deployment <deploy> -n {kube_namespace}',
            "childFileName": "kubectl-describe-deployment",
            "parentFolder": deployment_info
        },
        {
            "command": f'kubectl get configmap -n {kube_namespace}',
            "fileName": 'kubectl-get-configmap',
            "fileExt": 'log',
            "fileWrite": True,
            "forEachNode": False,
            "forEachCommand": '',
            "placeholderString": "",
            "parentFolder": config_map_info
        },
        {
            "command": f'kubectl get configmap --no-headers -o custom-columns=":metadata.name" -n {kube_namespace}',
            "fileName": 'kubectl-get-configmap',
            "fileExt": 'log',
            "fileWrite": False,
            "forEachNode": True,
            "placeholderString": "<configmap>",
            "forEachCommand": f'kubectl describe configmap <configmap> -n {kube_namespace}',
            "childFileName": "kubectl-describe-configmap",
            "parentFolder": config_map_info
        },
        {
            "command": f'kubectl logs -l cdap.container.TetheringAgent -n {kube_namespace} --tail -1',
            "fileName": 'kubectl-log-l-cdap-container-TetheringAgent',
            "fileExt": 'log',
            "fileWrite": True,
            "forEachNode": False,
            "forEachCommand": '',
            "placeholderString": "",
            "parentFolder": cdap_services_logs
        },
        {
            "command": f'kubectl logs -l cdap.container.ArtifactCache -n {kube_namespace} --tail -1',
            "fileName": 'kubectl-log-l-cdap-container-ArtifactCache',
            "fileExt": 'log',
            "fileWrite": True,
            "forEachNode": False,
            "forEachCommand": '',
            "placeholderString": "",
            "parentFolder": cdap_services_logs
        },
        {
            "command": f'kubectl logs -l cdap.twill.app=system-worker -n {kube_namespace} --tail -1',
            "fileName": 'kubectl-log-system-worker',
            "fileExt": 'log',
            "fileWrite": True,
            "forEachNode": False,
            "forEachCommand": '',
            "placeholderString": "",
            "parentFolder": cdap_services_logs
        },
        {
            "command": f'kubectl logs -l cdap.twill.app=task-worker -n {kube_namespace} -c taskworkertwillrunnable --tail -1',
            "fileName": 'kubectl-log-task-worker-c-taskworkertwillrunnable',
            "fileExt": 'log',
            "fileWrite": True,
            "forEachNode": False,
            "forEachCommand": '',
            "placeholderString": "",
            "parentFolder": cdap_services_logs
        },
        {
            "command": f'kubectl logs -l cdap.twill.app=task-worker -n {kube_namespace} -c artifactlocalizertwillrunnable --tail -1',
            "fileName": 'kubectl-log-task-worker-c-artifactlocalizertwillrunnable',
            "fileExt": 'log',
            "fileWrite": True,
            "forEachNode": False,
            "forEachCommand": '',
            "placeholderString": "",
            "parentFolder": cdap_services_logs
        },
        {
            "command": f'kubectl logs -l cdap.twill.app=preview-runner -n {kube_namespace} -c artifactlocalizertwillrunnable --tail -1',
            "fileName": 'kubectl-log-preview-runner-c-artifactlocalizertwillrunnable',
            "fileExt": 'log',
            "fileWrite": True,
            "forEachNode": False,
            "forEachCommand": '',
            "placeholderString": "",
            "parentFolder": cdap_services_logs
        },
        {
            "command": f'kubectl logs -l cdap.twill.app=preview-runner -n {kube_namespace} -c previewrunnertwillrunnable --tail -1',
            "fileName": 'kubectl-log-preview-runner-c-previewrunnertwillrunnable',
            "fileExt": 'log',
            "fileWrite": True,
            "forEachNode": False,
            "forEachCommand": '',
            "placeholderString": "",
            "parentFolder": cdap_services_logs
        },
        {
            "command": f'kubectl logs -l control-plane=monitoring-agent -c monitoring-agent -n {kube_namespace} --tail -1',
            "fileName": 'kubectl-log-monitoring-agent-c-monitoring-agent',
            "fileExt": 'log',
            "fileWrite": True,
            "forEachNode": False,
            "forEachCommand": '',
            "placeholderString": "",
            "parentFolder": cdap_services_logs
        },
        {
            "command": f'kubectl logs -l control-plane=monitoring-agent -c access-token-generator -n {kube_namespace} --tail -1',
            "fileName": 'kubectl-log-monitoring-agent-c-access-token-generator',
            "fileExt": 'log',
            "fileWrite": True,
            "forEachNode": False,
            "forEachCommand": '',
            "placeholderString": "",
            "parentFolder": cdap_services_logs
        },
        {
            "command": f'kubectl exec -ti {instance_name}-fs-0 -n {kube_namespace} -- /usr/local/hadoop/bin/hdfs dfs -df -h',
            "fileName": 'kubectl-exec-ti-cdap-fs-0-n-usr-local-hadoop-dfs-HDFS-disk-usage',
            "fileExt": 'log',
            "fileWrite": True,
            "forEachNode": False,
            "forEachCommand": '',
            "placeholderString": "",
            "parentFolder": memory_info
        },
        {
            "command": f'kubectl exec cdap-{instance_name}-runtime-0 -n {kube_namespace} -- curl cdap-{instance_name}-router:11015/v3/system/services/appfabric/logs?start=0 {auth_header} ',
            "fileName": 'kubectl-exec-appfabric-logs-from-api',
            "fileExt": 'log',
            "fileWrite": True,
            "forEachNode": False,
            "forEachCommand": '',
            "placeholderString": "",
            "parentFolder": cdap_services_logs
        },
        {
            "command": f'kubectl exec cdap-{instance_name}-runtime-0 -n {kube_namespace} -- curl cdap-{instance_name}-router:11015/v3/system/services/metrics/logs?start=0 {auth_header} ',
            "fileName": 'kubectl-exec-metrics-logs-from-api',
            "fileExt": 'log',
            "fileWrite": True,
            "forEachNode": False,
            "forEachCommand": '',
            "placeholderString": "",
            "parentFolder": cdap_services_logs
        },
        {
            "command": f'kubectl exec cdap-{instance_name}-runtime-0 -n {kube_namespace} -- curl cdap-{instance_name}-router:11015/v3/system/services/messaging.service/logs?start=0 {auth_header} ',
            "fileName": 'kubectl-exec-messaging-logs-from-api',
            "fileExt": 'log',
            "fileWrite": True,
            "forEachNode": False,
            "forEachCommand": '',
            "placeholderString": "",
            "parentFolder": cdap_services_logs
        },
        {
            "command": f'kubectl exec cdap-{instance_name}-runtime-0 -n {kube_namespace} -- curl cdap-{instance_name}-router:11015/v3/system/services/log.saver/logs?start=0 {auth_header} ',
            "fileName": 'kubectl-exec-log-saver-logs-from-api',
            "fileExt": 'log',
            "fileWrite": True,
            "forEachNode": False,
            "forEachCommand": '',
            "placeholderString": "",
            "parentFolder": cdap_services_logs
        },
        {
            "command": f'kubectl exec cdap-{instance_name}-runtime-0 -n {kube_namespace} -- curl cdap-{instance_name}-router:11015/v3/system/services/runtime/logs?start=0 {auth_header} ',
            "fileName": 'kubectl-exec-runtime-logs-from-api',
            "fileExt": 'log',
            "fileWrite": True,
            "forEachNode": False,
            "forEachCommand": '',
            "placeholderString": "",
            "parentFolder": cdap_services_logs
        },
        {
            "command": f'kubectl exec cdap-{instance_name}-runtime-0 -n {kube_namespace}  -c runtime -- bash -c \'curl -s cdap-{instance_name}-router:11015/v3/system/services/status {auth_header}\'',
            "fileName": 'kubectl-exec-system-service-status',
            "fileExt": 'txt',
            "fileWrite": True,
            "forEachNode": False,
            "forEachCommand": '',
            "placeholderString": "",
            "parentFolder": cdap_services_status
        },
        {
            "command": f'kubectl exec cdap-{instance_name}-runtime-0 -n {kube_namespace} -c runtime -- bash -c  \'curl -s cdap-{instance_name}-router:11015/v3/namespaces/system/apps/pipeline/services/studio/status {auth_header}\'',
            "fileName": 'kubectl-exec-system-service-studio-status',
            "fileExt": 'txt',
            "fileWrite": True,
            "forEachNode": False,
            "forEachCommand": '',
            "placeholderString": "",
            "parentFolder": cdap_services_status
        },
        {
            "command": f'kubectl exec cdap-{instance_name}-runtime-0 -n {kube_namespace} -c runtime -- bash -c  \'curl -s cdap-{instance_name}-router:11015/v3/namespaces/system/apps/dataprep/services/service/status  {auth_header}\'',
            "fileName": 'kubectl-exec-system-service-wrangler-status',
            "fileExt": 'txt',
            "fileWrite": True,
            "forEachNode": False,
            "forEachCommand": '',
            "placeholderString": "",
            "parentFolder": cdap_services_status
        },
        {
            "command": f'kubectl exec cdap-{instance_name}-runtime-0 -n {kube_namespace} -c runtime -- curl cdap-{instance_name}-router:11015/v3/namespaces/{cdap_ns}/apps/{pipeline_name}/spark/DataStreamsSparkStreaming/runs/{run_id}/logs {auth_header}',
            "fileName": f'pipeline_{pipeline_name}',
            "fileExt": 'log',
            "fileWrite": True,
            "forEachNode": False,
            "forEachCommand": '',
            "placeholderString": "",
            "isPipelineLog" : True,
            "parentFolder": pipeline_logs,

        },
        {
            "command": f'kubectl exec cdap-{instance_name}-runtime-0 -n {kube_namespace}  -c runtime -- bash -c \'curl -s cdap-{instance_name}-router:11015/v3/namespaces/{cdap_ns}/apps/{pipeline_name} {auth_header}\'',
            "fileName": f'pipeline_{pipeline_name}_json',
            "fileExt": 'json',
            "fileWrite": True,
            "forEachNode": False,
            "forEachCommand": '',
            "placeholderString": "",
            "isPipelineLog" : True,
            "parentFolder": "pipeline_logs",
        },
        {
            "command": f'kubectl get pods -l cdap.twill.run.id={run_id} -n {runtime_namespace} -o wide',
            "fileName": f'pipeline_pods_{run_id}',
            "fileExt": 'log',
            "fileWrite": True,
            "forEachNode": False,
            "forEachCommand": '',
            "placeholderString": "",
            "isPipelineLog" : True,
            "parentFolder": pipeline_logs,
        },
        {
            "command": f'kubectl get jobs -n {runtime_namespace}',
            "fileName": 'kubectl-get-jobs',
            "fileExt": 'log',
            "fileWrite": True,
            "forEachNode": False,
            "forEachCommand": '',
            "placeholderString": "",
            "isRunTimeNameSpace" : True,
            "parentFolder": runtime_namespace_logs,
        },
        {
            "command": f'kubectl get jobs -n {runtime_namespace} --no-headers -o custom-columns=":metadata.name"',
            "fileName": 'kubectl-get-jobs',
            "fileExt": 'log',
            "fileWrite": False,
            "forEachNode": True,
            "forEachCommand": f'kubectl describe jobs <job> -n {runtime_namespace}',
            "placeholderString": "<job>",
            "childFileName": "kubectl-describe-jobs",
            "isRunTimeNameSpace" : True,
            "parentFolder": runtime_namespace_logs
        },
        {
            "command": f'kubectl get configmap -n {runtime_namespace}',
            "fileName": 'kubectl-get-configmap',
            "fileExt": 'log',
            "fileWrite": True,
            "forEachNode": False,
            "forEachCommand": '',
            "placeholderString": "",
            "isRunTimeNameSpace" : True,
            "parentFolder": runtime_namespace_logs,
        },
        {
            "command": f'kubectl get configmap --no-headers -o custom-columns=":metadata.name" -n {runtime_namespace}',
            "fileName": 'kubectl-get-configmap',
            "fileExt": 'log',
            "fileWrite": False,
            "forEachNode": True,
            "placeholderString": "<configmap>",
            "forEachCommand": f'kubectl describe configmap <configmap> -n {runtime_namespace}',
            "childFileName": "kubectl-describe-configmap",
            "isRunTimeNameSpace" : True,
            "parentFolder": runtime_namespace_logs
        },
        {
            "command": f'kubectl get pods -l cdap.twill.run.id={run_id} -n {runtime_namespace} -o wide',
            "fileName": f'pipeline_pods_{run_id}',
            "fileExt": 'log',
            "fileWrite": True,
            "forEachNode": False,
            "forEachCommand": '',
            "placeholderString": "",
            "isRunTimeNameSpaceWithRunId" : True,
            "parentFolder": runtime_namespace_logs,
        },
        {
            "command": f'kubectl get pods -n {runtime_namespace} -o wide',
            "fileName": 'kubectl-get-pods',
            "fileExt": 'log',
            "fileWrite": True,
            "forEachNode": False,
            "forEachCommand": '',
            "placeholderString": "",
            "isRunTimeNameSpace" : True,
            "parentFolder": runtime_namespace_logs
        },
        {
            "command": f'kubectl get pods --no-headers -o custom-columns=":metadata.name" -n {runtime_namespace}',
            "fileName": 'kubectl-get-pods',
            "fileExt": 'log',
            "fileWrite": False,
            "forEachNode": True,
            "placeholderString": "<pod>",
            "forEachCommand": f'kubectl logs <pod> -n {runtime_namespace}',
            "childFileName": "kubectl-pod-logs-runtime",
            "isRunTimeNameSpace" : True,
            "parentFolder": runtime_namespace_logs
        }
    ]

    log_commands_processor(cdapCommandLists, directoryName, cdap_ns, pipeline_name, run_id, runtime_namespace)

    update_progress(98)
    compressed_file = shutil.make_archive(
        base_name=directoryName,
        format='gztar',
        root_dir=parent_dir + directoryName
    )

    shutil.rmtree(directoryName)
    update_progress(100)
    print(f'Logs collection completed. Please find {directoryName}.tar.gz in current directory')
    exit()

def remove_security_authorization_from_cconf_log(cconf_file_output):
    flag = True
    while flag:
        start = cconf_file_output.find("""<property>
    <name>security.authorization.extension.config.""")
        if start != -1:
            end = cconf_file_output.find("""</property>""", start)
            cconf_file_output = cconf_file_output[0:start] + cconf_file_output[end + 11:].strip()
        else:
            flag = False
    return cconf_file_output

def log_file_write(pathName, filename, fileWriteMode, contentToWrite, fileExt="log"):
    with open(f"{pathName}/{filename}.{fileExt}", fileWriteMode) as f:
        f.write(contentToWrite)


def update_progress(count):
    total = 100
    suffix = 'Done' if (count == 100) else 'In Progress'
    bar_len = 60
    filled_len = int(round(bar_len * count / float(total)))

    percents = round(100.0 * count / float(total), 1)
    bar = '=' * filled_len + '-' * (bar_len - filled_len)

    sys.stdout.write('[%s] %s%s ...%s\r' % (bar, percents, '%', suffix))
    sys.stdout.flush()


def log_commands_processor(commandLists, directoryName, cdap_ns, pipeline_name, run_id, runtime_namespace):
    progress_len = 5
    count = 1

    for x in commandLists:
        directory_name = directoryName
        if "isPipelineLog" in x  and (cdap_ns is None or pipeline_name is None or run_id is None):
            update_progress(progress_len)
        elif "isRunTimeNameSpace" in x  and runtime_namespace is None:
            update_progress(progress_len)
        elif "isRunTimeNameSpaceWithRunId" in x  and (runtime_namespace is None or run_id is None):
            update_progress(progress_len)
        else:
            if x["parentFolder"] != "" and os.path.isdir(os.path.join(directoryName , x["parentFolder"])) == False:
                os.makedirs(os.path.join(directoryName , x["parentFolder"]))

            command_output = subprocess.getoutput(x["command"])
            if x["fileWrite"] == True:
                directory_name = os.path.join(directoryName , x["parentFolder"])
                log_file_write(directory_name, f'{x["fileName"]}', "a", command_output, x["fileExt"])

            if x["forEachNode"] == True:
                nArr = command_output.split('\n')
                for a in nArr:
                    placeholderCommand = x["forEachCommand"]
                    command = placeholderCommand.replace(x["placeholderString"], a)
                    for_command_output = subprocess.getoutput(command)
                    if x["parentFolder"] == "config_map_info" and re.search("-cconf$", a):
                        for_command_output = remove_security_authorization_from_cconf_log(for_command_output)
                    directory_name = os.path.join(directoryName , x["parentFolder"])
                    log_file_write(directory_name , f'{x["childFileName"]}-{a}', "a", for_command_output, x["fileExt"])

            update_progress(progress_len)
            progress_len += 2
            count += 1

if __name__ == "__main__":
    args = parser.parse_args()
    main(args.kube_namespace, args.username, args.password, args.cdap_ns, args.pipeline_name, args.run_id, args.runtime_namespace)