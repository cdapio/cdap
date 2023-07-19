**CDAP Support Bundle Script**
------------------------------

**Description**
---------------

Collect the logs from CDAP instance deployed on Kubernetes and create a tarball file for the same logs. 

**Use Case** 
-------------

This script can be used to troubleshoot issues with CDAP system services or to get information about CDAP installation. 

**Command Line Arguments**
--------------------------

The CDAP Support Bundle Script takes the following command-line arguments:

1. --kube-namespace, -n : The Kubernetes namespace where CDAP is installed. Default is set to "default".

2. --username, -u : The CDAP username, if authentication is enabled.

3. --password, -p : The CDAP password, if authentication is enabled.

4. --cdap-ns, -c : The CDAP namespace, which is the default namespace where CDAP services are deployed. Default is set to "default".

5. --pipeline-name, -l : The name of the pipeline, if you want to collect logs for a failed pipeline run.

6. --run-id, -r : The run ID of the pipeline, if you want to collect logs for a failed pipeline run.

7. --runtime-namespace, -t : The runtime namespace, which is the namespace where the pipeline is running. Default is set to "default".

**Support Bundle Script Execution**
-----------------------------------

To execute the support bundle script, use the below command

`python3 collect_support_bundle.py -n <kube-namespace> -u <username> -p <password>`

On each run, the script first fetches the name of the CDAP instance from Kubernetes. Then, it checks if the authentication is enabled for the Kubernetes namespace with CDAP using the provided username and password. 

Next, a directory will be created to store the collected logs. For each command, the script will write the output either to a file or to a file for each node based on the number of nodes.

Finally, a message will be printed to show the log collection process is complete. 

**Support Bundle Script Output**
--------------------------------

The Support Bundle Script iterates through a list of commands and performs different actions depending on the conditions and settings in each command.

At the end, a tarball file will be created. This file will contain the logs for _**config_file**_, _**pod_info**_, _**job_info**_, _**config_map_info**_, _**deployment_info**_, _**node_info**_, _**stateful_set_info**_, _**services_info**_, _**memory_info**_, _**cdap_services_log**_, _**cdap_services_status**_, and _**runtime_namespace_logs**_.

If you want to get the logs for pipeline runs that failed, use the parameters **pipeline-name (-l)**, **run-id (-r)**, and **runtime-namespace (-t)**. 

Below is the command for collecting the logs for the pipeline runs:

`python3 collect_support_bundle.py -n <kube-namespace> -u <username> -p <password> -l <pipeline-name> -r <run-id> -t <runtime-namespace>`

**Note**: If you don't want the resources to be cleaned up after a failed run, run the pipeline with the property system.runtime.cleanup.disabled set to true in the runtime arguments.


**Credentials**
---------------

The Support Bundle Script does not share credentials in the output tarball file. It only includes the logs that are collected from the CDAP instance. 

You can also use the "**-p**" flag option to pass the password as an environment variable. This will prevent the password from being visible in the tarball file. 

**Example**
-----------

Below is an example of how to use the "**-p**" option:  

`export CDAP_PASSWORD=mypassword`  
`python3 collect_support_bundle.py -n <kube-namespace> -u <username> -p $CDAP_PASSWORD`

The CDAP_PASSWORD environment variable is set to the value "mypassword". The support bundle script will then use this value to authenticate with CDAP and the password will not be visible in the tarball file.
