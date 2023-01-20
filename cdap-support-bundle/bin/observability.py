# -*- coding: utf-8 -*-

# Copyright © 2014-2023 Cask Data, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

# conf.py to build high-level document comprised of multiple manuals
# Relies on settings in common_conf.py

# Search Index
# Includes handler to build a common search index of all manuals combined

import base64
import json
import os
import shutil
import subprocess
import sys
from datetime import datetime

import click


@click.command()
@click.option(
    "--namespace",
    help="Mandatory parameter. Kubernetes namespace where CDAP is installed",
    type=str,
)
@click.option(
    "--instancename",
    help="Mandatory parameter. CDAP instance name",
    type=str,
)
@click.option(
    "--username",
    help="Mandatory parameter. CDAP User Name",
    type=str,
)
@click.option(
    "--password",
    help="Mandatory parameter. CDAP Password",
    type=str,
)
def main(namespace, instancename, username, password):
    """Log collector program for CDAP."""

    # if namespace provided as parameter then assign else take input from user
    if namespace is None or instancename is None or username is None or password is None:
        print('Missing mandatory input parameters. Please visit help using --help')
        exit()


    passtoken = base64.b64encode(bytes(f'{username}:{password}', 'utf-8'))
    passtoken = passtoken.decode('utf-8')

    # running command to get AUTH_TOKEN
    output = subprocess.getoutput(
        f'kubectl -n {namespace} exec -t cdap-{instancename}-appfabric-0 -c appfabric -- bash -c \'curl -s cdap-{instancename}-external-authentication:10009/token -H "Authorization:Basic {passtoken}"\'')
    try:
        str_obj = json.loads(output)
    except:
        print('Invalid input parameters. Please check if you have provided correct values for all input parameters.')
        exit()
    auth_token = str_obj['access_token']

    # creating object for current datetime
    update_progress(1)
    current_datetime = datetime.now()
    # fetching formatted date time
    current_date_time = current_datetime.strftime("%m-%d-%YT%H:%M:%S")
    # Directory
    directoryName = "cdap-logs-" + current_date_time

    # Parent Directory path
    parent_dir = os.getcwd() + '/'
    # print(f"current path :{parent_dir}")

    # Path
    path = os.path.join(parent_dir, directoryName)

    # Create the directory
    os.mkdir(path)
    # print("Directory '% s' created" % path)
    update_progress(4)

    # listing command
    hdfCommandLists = [
        {
            "command": f'kubectl top nodes',
            "fileName": 'kubectl-top-nodes',
            "fileExt": 'log',
            "fileWrite": True,
            "isSub": False,
            "forEachNode": False,
            "forEachCommand": '',
            "placeholderString": ""
        },
        {
            "command": f'kubectl get nodes',
            "fileName": 'kubectl-get-nodes',
            "fileExt": 'log',
            "fileWrite": True,
            "isSub": False,
            "forEachNode": False,
            "forEachCommand": '',
            "placeholderString": ""
        },
        {
            "command": f'kubectl get nodes --no-headers -o custom-columns=":metadata.name"',
            "fileName": 'kubectl-get-nodes',
            "fileExt": 'log',
            "fileWrite": False,
            "isSub": True,
            "forEachNode": True,
            "forEachCommand": f'kubectl describe node <node>',
            "placeholderString": "<node>",
            "childFileName": "kubectl-describe-node"
        },
        {
            "command": f'kubectl get cdapmaster -n {namespace} -o yaml',
            "fileName": 'kubectl-get-cdapmaster-yaml',
            "fileExt": 'log',
            "fileWrite": True,
            "isSub": True,
            "forEachNode": False,
            "forEachCommand": '',
            "placeholderString": ""
        },
        {
            "command": f'kubectl get pods -n {namespace} -o wide',
            "fileName": 'kubectl-get-pods',
            "fileExt": 'log',
            "fileWrite": True,
            "isSub": False,
            "forEachNode": False,
            "forEachCommand": '',
            "placeholderString": ""
        },
        {
            "command": f'kubectl get pods --no-headers -o custom-columns=":metadata.name" -n {namespace}',
            "fileName": 'kubectl-get-pods',
            "fileExt": 'log',
            "fileWrite": False,
            "isSub": True,
            "forEachNode": True,
            "placeholderString": "<pod>",
            "forEachCommand": f'kubectl exec -ti <pod> -n {namespace} -- df -h',
            "childFileName": "kubectl-exec-ti-df-h"
        },
        {
            "command": f'kubectl get pods --no-headers -o custom-columns=":metadata.name" -n {namespace}',
            "fileName": 'kubectl-get-pods',
            "fileExt": 'log',
            "fileWrite": False,
            "isSub": True,
            "forEachNode": True,
            "placeholderString": "<pod>",
            "forEachCommand": f'kubectl describe pod <pod> -n {namespace}',
            "childFileName": "kubectl-describe-pod"
        },
        {
            "command": f'kubectl get svc -n {namespace}',
            "fileName": 'kubectl-get-svc',
            "fileExt": 'log',
            "fileWrite": True,
            "isSub": False,
            "forEachNode": False,
            "forEachCommand": '',
            "placeholderString": ""
        },
        {
            "command": f'kubectl get svc --no-headers -o custom-columns=":metadata.name" -n {namespace}',
            "fileName": 'kubectl-get-svc',
            "fileExt": 'log',
            "fileWrite": False,
            "isSub": True,
            "forEachNode": True,
            "placeholderString": "<svc>",
            "forEachCommand": f'kubectl describe svc <svc> -n {namespace}',
            "childFileName": "kubectl-describe-svc"
        },
        {
            "command": f'kubectl get sts -n {namespace}',
            "fileName": 'kubectl-get-sts',
            "fileExt": 'log',
            "fileWrite": True,
            "isSub": False,
            "forEachNode": False,
            "forEachCommand": '',
            "placeholderString": ""
        },
        {
            "command": f'kubectl get sts --no-headers -o custom-columns=":metadata.name" -n {namespace}',
            "fileName": 'kubectl-get-sts',
            "fileExt": 'log',
            "fileWrite": False,
            "isSub": True,
            "forEachNode": True,
            "placeholderString": "<sts>",
            "forEachCommand": f'kubectl describe sts <sts> -n {namespace}',
            "childFileName": "kubectl-describe-sts"
        },
        {
            "command": f'kubectl get deployment -n {namespace}',
            "fileName": 'kubectl-get-deployment',
            "fileExt": 'log',
            "fileWrite": True,
            "isSub": False,
            "forEachNode": False,
            "forEachCommand": '',
            "placeholderString": ""
        },
        {
            "command": f'kubectl get deployment --no-headers -o custom-columns=":metadata.name" -n {namespace}',
            "fileName": 'kubectl-get-deployment',
            "fileExt": 'log',
            "fileWrite": False,
            "isSub": True,
            "forEachNode": True,
            "placeholderString": "<deploy>",
            "forEachCommand": f'kubectl describe deployment <deploy> -n {namespace}',
            "childFileName": "kubectl-describe-deployment"
        },
        {
            "command": f'kubectl get configmap -n {namespace}',
            "fileName": 'kubectl-get-configmap',
            "fileExt": 'log',
            "fileWrite": True,
            "isSub": False,
            "forEachNode": False,
            "forEachCommand": '',
            "placeholderString": ""
        },
        {
            "command": f'kubectl get configmap --no-headers -o custom-columns=":metadata.name" -n {namespace}',
            "fileName": 'kubectl-get-configmap',
            "fileExt": 'log',
            "fileWrite": False,
            "isSub": True,
            "forEachNode": True,
            "placeholderString": "<configmap>",
            "forEachCommand": f'kubectl describe configmap <configmap> -n {namespace}',
            "childFileName": "kubectl-describe-configmap"
        },
        {
            "command": f'kubectl logs -l cdap.container.TetheringAgent -n {namespace}',
            "fileName": 'kubectl-log-l-cdap-container-TetheringAgent',
            "fileExt": 'log',
            "fileWrite": True,
            "isSub": False,
            "forEachNode": False,
            "forEachCommand": '',
            "placeholderString": ""
        },
        {
            "command": f'kubectl logs -l cdap.container.ArtifactCache -n {namespace}',
            "fileName": 'kubectl-log-l-cdap-container-ArtifactCache',
            "fileExt": 'log',
            "fileWrite": True,
            "isSub": False,
            "forEachNode": False,
            "forEachCommand": '',
            "placeholderString": ""
        },
        {
            "command": f'kubectl logs -l cdap.twill.app=system-worker -n {namespace}',
            "fileName": 'kubectl-log-system-worker',
            "fileExt": 'log',
            "fileWrite": True,
            "isSub": False,
            "forEachNode": False,
            "forEachCommand": '',
            "placeholderString": ""
        },
        {
            "command": f'kubectl logs -l cdap.twill.app=task-worker -n {namespace} -c taskworkertwillrunnable',
            "fileName": 'kubectl-log-task-worker-c-taskworkertwillrunnable',
            "fileExt": 'log',
            "fileWrite": True,
            "isSub": False,
            "forEachNode": False,
            "forEachCommand": '',
            "placeholderString": ""
        },
        {
            "command": f'kubectl logs -l cdap.twill.app=task-worker -n {namespace} -c artifactlocalizertwillrunnable',
            "fileName": 'kubectl-log-task-worker-c-artifactlocalizertwillrunnable',
            "fileExt": 'log',
            "fileWrite": True,
            "isSub": False,
            "forEachNode": False,
            "forEachCommand": '',
            "placeholderString": ""
        },
        {
            "command": f'kubectl logs -l cdap.twill.app=preview-runner -n {namespace} -c artifactlocalizertwillrunnable',
            "fileName": 'kubectl-log-preview-runner-c-artifactlocalizertwillrunnable',
            "fileExt": 'log',
            "fileWrite": True,
            "isSub": False,
            "forEachNode": False,
            "forEachCommand": '',
            "placeholderString": ""
        },
        {
            "command": f'kubectl logs -l cdap.twill.app=preview-runner -n {namespace} -c previewrunnertwillrunnable',
            "fileName": 'kubectl-log-preview-runner-c-previewrunnertwillrunnable',
            "fileExt": 'log',
            "fileWrite": True,
            "isSub": False,
            "forEachNode": False,
            "forEachCommand": '',
            "placeholderString": ""
        },
        {
            "command": f'kubectl exec -ti {instancename}-fs-0 -n {namespace} -- /usr/local/hadoop/bin/hdfs dfs -df -h',
            "fileName": 'kubectl-exec-ti-cdap-fs-0-n-usr-local-hadoop-dfs-HDFS-disk-usage',
            "fileExt": 'log',
            "fileWrite": True,
            "isSub": False,
            "forEachNode": False,
            "forEachCommand": '',
            "placeholderString": ""
        },
        {
            "command": f'kubectl exec cdap-{instancename}-runtime-0 -n {namespace} -- curl cdap-{instancename}-router:11015/v3/system/services/appfabric/logs?start=0 -H "Authorization: Bearer {auth_token}" ',
            "fileName": 'kubectl-exec-appfabric-logs-from-api',
            "fileExt": 'log',
            "fileWrite": True,
            "isSub": False,
            "forEachNode": False,
            "forEachCommand": '',
            "placeholderString": ""
        },
        {
            "command": f'kubectl exec cdap-{instancename}-runtime-0 -n {namespace} -- curl cdap-{instancename}-router:11015/v3/system/services/metrics/logs?start=0 -H "Authorization: Bearer {auth_token}" ',
            "fileName": 'kubectl-exec-metrics-logs-from-api',
            "fileExt": 'log',
            "fileWrite": True,
            "isSub": False,
            "forEachNode": False,
            "forEachCommand": '',
            "placeholderString": ""
        },
        {
            "command": f'kubectl exec cdap-{instancename}-runtime-0 -n {namespace} -- curl cdap-{instancename}-router:11015/v3/system/services/messaging.service/logs?start=0 -H "Authorization: Bearer {auth_token}" ',
            "fileName": 'kubectl-exec-messaging-logs-from-api',
            "fileExt": 'log',
            "fileWrite": True,
            "isSub": False,
            "forEachNode": False,
            "forEachCommand": '',
            "placeholderString": ""
        },
        {
            "command": f'kubectl exec cdap-{instancename}-runtime-0 -n {namespace} -- curl cdap-{instancename}-router:11015/v3/system/services/log.saver/logs?start=0 -H "Authorization: Bearer {auth_token}" ',
            "fileName": 'kubectl-exec-log-saver-logs-from-api',
            "fileExt": 'log',
            "fileWrite": True,
            "isSub": False,
            "forEachNode": False,
            "forEachCommand": '',
            "placeholderString": ""
        },
        {
            "command": f'kubectl exec cdap-{instancename}-runtime-0 -n {namespace} -- curl cdap-{instancename}-router:11015/v3/system/services/runtime/logs?start=0 -H "Authorization: Bearer {auth_token}" ',
            "fileName": 'kubectl-exec-runtime-logs-from-api',
            "fileExt": 'log',
            "fileWrite": True,
            "isSub": False,
            "forEachNode": False,
            "forEachCommand": '',
            "placeholderString": ""
        },
        {
            "command": f'kubectl exec cdap-{instancename}-runtime-0 -n {namespace} -- curl cdap-{instancename}-router:11015/v3/system/services/status -H "Authorization: Bearer {auth_token}"',
            "fileName": 'kubectl-exec-system-service-status',
            "fileExt": 'log',
            "fileWrite": True,
            "isSub": False,
            "forEachNode": False,
            "forEachCommand": '',
            "placeholderString": ""
        },
        {
            "command": f'kubectl exec cdap-{instancename}-runtime-0 -n {namespace} -- curl cdap-{instancename}-router:11015/v3/namespaces/system/apps/pipeline/services/studio/status -H "Authorization: Bearer {auth_token}"',
            "fileName": 'kubectl-exec-system-service-studio-status',
            "fileExt": 'log',
            "fileWrite": True,
            "isSub": False,
            "forEachNode": False,
            "forEachCommand": '',
            "placeholderString": ""
        },
        {
            "command": f'kubectl exec cdap-{instancename}-runtime-0 -n {namespace} -- curl cdap-{instancename}-router:11015/v3/namespaces/system/apps/dataprep/services/service/status -H "Authorization: Bearer {auth_token}"',
            "fileName": 'kubectl-exec-system-service-wrangler-status',
            "fileExt": 'log',
            "fileWrite": True,
            "isSub": False,
            "forEachNode": False,
            "forEachCommand": '',
            "placeholderString": ""
        }
        # ,
        # {
        #     "command": f'kubectl exec cdap-{instancename}-runtime-0 -n {namespace} -- curl cdap-{instancename}-router:11015/v3//namespaces/default/apps/Zendesk_ConnectionManager_Nov17_v1/workflows/DataPipelineWorkflow/runs/0bfa7d54-9804-11ed-9f22-00000076742c/logs -H "Authorization: Bearer {auth_token}"',
        #     "fileName": 'kubectl-exec-cdap-curl-router-system-service-status-logs-from-authenticated-api',
        #     "fileExt": 'log',
        #     "fileWrite": True,
        #     "isSub" : False,
        #     "forEachNode": False,
        #     "forEachCommand": '',
        #     "placeholderString": ""
        # }
    ]

    log_commands_processor(hdfCommandLists, directoryName, True)

    update_progress(98)
    # Compressing the created directory
    compressed_file = shutil.make_archive(
        base_name=directoryName + '-generated',  # archive file name w/o extension
        format='gztar',  # available formats: zip, gztar, bztar, xztar, tar
        root_dir=parent_dir + directoryName  # directory to compress
    )

    # removing the directory after compression
    shutil.rmtree(directoryName)
    update_progress(100)
    print(f'Logs collection completed. Please find {directoryName}-generated.tar.gz in current directory')
    exit()


######
# function for writing the passed log into the log file inside the directory
######
def log_file_write(pathName, filename, fileWriteMode, contentToWrite, fileExt="log"):
    f = open(f"{pathName}/{filename}.{fileExt}", fileWriteMode)
    f.write(contentToWrite)
    f.close()


# function updating progress bar
def update_progress(count):
    total = 100
    suffix = 'Done' if (count == 100) else 'In Progress'
    bar_len = 60
    filled_len = int(round(bar_len * count / float(total)))

    percents = round(100.0 * count / float(total), 1)
    bar = '=' * filled_len + '-' * (bar_len - filled_len)

    sys.stdout.write('[%s] %s%s ...%s\r' % (bar, percents, '%', suffix))
    sys.stdout.flush()


# function for logging command outputs to file
def log_commands_processor(commandLists, directoryName, progressShow):
    progress_len = 5
    count = 1
    for x in commandLists:
        command_output = subprocess.getoutput(x["command"])
        if x["fileWrite"] == True:
            log_file_write(directoryName, f'{x["fileName"]}', "a", command_output, x["fileExt"])

        if x["forEachNode"] == True:
            nArr = command_output.split('\n')
            for a in nArr:
                placeholderCommand = x["forEachCommand"]
                command = placeholderCommand.replace(x["placeholderString"], a)
                for_command_output = subprocess.getoutput(command)
                log_file_write(directoryName, f'{x["childFileName"]}-{a}', "a", for_command_output, x["fileExt"])

        if progressShow:
            update_progress(progress_len)
            progress_len += 3
        count += 1


    # main function execution starts here
if __name__ == "__main__":
    main()
