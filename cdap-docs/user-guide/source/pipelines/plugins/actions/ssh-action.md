# SSH


Description
-----------
Establishes an SSH connection with remote machine to execute command on that machine.


Use Case
--------
This action can be used when data needs to be copied to HDFS from a remote server before the pipeline starts.
It can also be used to move data for archiving before the pipeline starts.


Properties
----------

**host:** The host name of the remote machine where the command needs to be executed.

**port:** The port used to make SSH connection. Defaults to 22.

**user:** The username used to connect to host.

**privateKey:** The private key to be used to perform the secure shell action. Users can also specify a macro that will
pull the private key from the secure key management store in CDAP such as ${secure(myPrivateKey)}.

**passphrase:** Passphrase used to decrypt the provided private key in "privateKey".

**command:** The command to be executed on the remote host. This includes the filepath of the script and any arguments
needing to be passed.

**outputKey:** The key used to store the output of the command run by the action.
Plugins that run at later stages in the pipeline can retrieve the command's output using this key through
macro substitution (e.g. ${outputKey} where "outputKey" is the key specified). Defaults to "sshOutput".

Example
-------
This example runs a script on demo@example.com:

    {
        "name": "SSHAction",
          "plugin": {
            "name": "SSH",
            "type": "action",
            "label": "SSH",
            "artifact": {
              "name": "core-plugins",
              "version": "1.4.0-SNAPSHOT",
              "scope": "SYSTEM"
          },
          "properties": {
              "host": "example.com",
              "port": "22",
              "user": "demo",
              "privateKey": "${secure(myPrivateKey)}",
              "passphrase": "pass",
              "command": "~/scripts/remoteScript.sh",
              "outputKey": "myActionOutput"
            }
          }
    }

---
- CDAP Pipelines Plugin Type: action
- CDAP Pipelines Version: 1.7.0
