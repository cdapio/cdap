**HDFS Support Bundle Script**
------------------------------

**Description**
---------------

Collect the logs from CDAP instance deployed on Kubernetes and create a tarball file for the same logs. 

**Use Case** 
-------------

This script can be used to troubleshoot issues with CDAP system services or to get information about CDAP installation. 

**Command Line Arguments**
--------------------------

The script takes the following command-line arguments:

1. kube-namespace: The Kubernetes namespace where CDAP is installed.

2. username: The CDAP username, if authentication is enabled.

3. password: The CDAP password, if authentication is enabled.

4. cdap-ns: The CDAP namespace, which is the default namespace where CDAP services are deployed.

5. pipeline-name: The name of the pipeline, if you want to collect logs for a failed pipeline run.

6. run-id: The run ID of the pipeline, if you want to collect logs for a failed pipeline run.

7. runtime-namespace: The runtime namespace, which is the namespace where the pipeline is running.

**Support Bundle Script Execution**
-----------------------------------

On each run, the script first fetches the name of the CDAP instance from Kubernetes. Then, it checks if the authentication is enabled for the Kubernetes namespace with CDAP using the provided username and password. 

Next, a directory will be created to store the collected logs. For each command, the script will write the output either to a file or to a file for each node based on the number of nodes.

Finally, a message will be printed to show the log collection process is complete. 

**Support Bundle Script Output**
--------------------------------

A tarball file for all the log files will be created as an output file.

**Credentials**
---------------

The Support Bundle Script does not share credentials in the output tarball file. It only includes the logs that are collected from the CDAP instance. 

You can also use the "**-p**" flag option to pass the password as an environment variable. This will prevent the password from being visible in the tarball file. 

**Example**
-----------

Below is an example of how to use the "**-p**" option:  

`export CDAP_PASSWORD=mypassword`  
`python3 collect_support_bundle.py -n my-kube-namespace -p`

The CDAP_PASSWORD environment variable is set to the value "mypassword". The support bundle script will then use this value to authenticate with CDAP. The password will not be visible in the tarball file.
