# Email


Description
-----------
Sends an email at the end of a pipeline run.


Use Case
--------
This action can be used when you want to send an email at the end of a pipeline run.
For example, you may want to configure a pipeline so that an email is sent whenever
the run failed for any reason.


Properties
----------
**runCondition:**" When to run the action. Must be 'completion', 'success', or 'failure'. Defaults to 'completion'.
If set to 'completion', the action will be executed regardless of whether the pipeline run succeeded or failed.
If set to 'success', the action will only be executed if the pipeline run succeeded.
If set to 'failure', the action will only be executed if the pipeline run failed.

**recipients:** Comma-separated list of addresses to send the email to.

**sender:** The address to send the email from.

**message:** The message of the email.

**subject:** The subject of the email.

**protocol:** The email protocol to use. SMTP, SMTPS, and TLS are supported. Defaults to SMTP.

**username:** The username to use for authentication if the protocol requires it.

**password:** The password to use for authentication if the protocol requires it.

**host:** The SMTP host to use. Defaults to 'localhost'.

**port:** The SMTP port to use. Defaults to 25.

**includeWorkflowToken:** Whether to include the contents of the workflow token in the email message. Defaults to false.

Example
-------
This example sends an email from 'team-ops@example.com' to 'team-alerts@example.com' whenever a run fails:

    {
        "name": "Email",
        "type": "postaction",
        "properties": {
            "recipientEmailAddress": "team-alerts@example.com",
            "senderEmailAddress": "team-ops@example.com",
            "subject": "Pipeline Failure ${logicalStartTime(yyyy-MM-dd)}",
            "message": "The pipeline run failed.",
            "includeWorkflowToken": "true",
            "host": "smtp-server.com",
            "port": "25",
            "runCondition": "failure"
        }
    }

---
- CDAP Pipelines Plugin Type: postaction
- CDAP Pipelines Version: 1.7.0
