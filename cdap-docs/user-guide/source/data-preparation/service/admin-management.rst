.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2017 Cask Data, Inc.
    :description: The CDAP User Guide

.. _user-guide-data-preparation-service-admin-management:

=============================
Administration and Management
=============================

- `Creating a Workspace`_
- `Deleting a Workspace`_
- `Uploading a File to a Workspace`_
- `Downloading a File from a Workspace`_

Creating a Workspace
====================
This REST API call creates a workspace or scratch pad for temporarily storing the data to
be wrangled in the backend. Workspace is identified by an identifier that can be
alpha-numeric with only other allowed character as underscore(_).

- **URL**
  ::

    workspaces/:workspaceid

- **Method**
  ::

    PUT

- **URL Params**

  *None*

- **Data Params**

  *Not Applicable*

- **Success Response**

  - **Code** 200 |br|
    **Content**
    ::

      {
        'status' : 200,
        'message' : "Successfully created workspace ':workspaceid'"
      }

- **Error Responses**

  - **Code** 500 Server Error |br|
    **Content**
    ::

      {
        'status' : 500,
        'message' : "<appropriate error message>"
      }

  - **Code** 500 Server Error |br|
    **Content**
    ::

      Unable to route to service <url>

- **Sample Call**
  ::

    $.ajax({
      url: "${base-url}/workspaces/${workspace}",
      dataType: "json",
      type : "PUT",
      success : function(r) {
        console.log(r);
      }
    });

- **Notes**

  API call will fail if the backend service is not started or if dataset write fails.


Deleting a Workspace
====================
This REST API call deletes the workspace or scratch pad. This will also delete any data
associated with it.

- **URL**
  ::

    workspaces/:workspaceid

- **Method**
  ::

    DELETE

- **URL Params**

  *None*

- **Data Params**

  *Not Applicable*

- **Success Response**

  - **Code** 200 |br|
    **Content**
    ::

      {
        'status' : 200,
        'message' : "Successfully deleted workspace ':workspaceid'"
      }

- **Error Responses**

  - **Code** 500 Server Error |br|
    **Content**
    ::

      {
        'status' : 500,
        'message' : "<appropriate error message>"
      }

  - **Code** 500 Server Error |br|
    **Content**
    ::

      Unable to route to service <url>


- **Sample Call**
  ::

    $.ajax({
      url: "${base-url}/workspaces/${workspace}",
      dataType: "json",
      type : "DELETE",
      success : function(r) {
        console.log(r);
      }
    });

- **Notes**

  API call will fail if the backend service is not started or if dataset write fails.


Uploading a File to a Workspace
===============================
This REST API call will upload a file to the workspace. The file is split into lines based
on the end-of-line delimiter (EOL).

- **URL**
  ::

    workspaces/:workspaceid/upload

- **Method**
  ::

    POST

- **URL Params**

  *None*

- **Data Params**

  *Not Applicable*

- **Success Response:**

  - **Code** 200 |br|
    **Content**
    ::

      {
        'status' : 200,
        'message' : "Successfully uploaded data to workspace ':workspaceid' (records 1000)"
      }

- **Error Responses**

  - **Code** 500 Server Error |br|
    **Content**
    ::

      {
        'status' : 500,
        'message' : "Body not present, please post the file containing the records to be wrangle."
      }

  - **Code** 500 Server Error |br|
    **Content**
    ::

      Unable to route to service <url>

  - **Code** 500 Server Error |br|
    **Content**
    ::

      {
        'status' : 500,
        'message' : "<appropriate error message>"
      }

- **Sample Call**
  ::

    $.ajax({
      url: "${base-url}/workspaces/${workspace}/upload",
      type: 'POST',
      data: data,
      cache: false,
      contentType: 'application/octet-stream',
      processData: false, // Don't process the files
      contentType: false,
      success: function(r) {
        console.log(r);
      },
      error: function(r) {
        console.log(r);
      }
    });


Downloading a File from a Workspace
===================================
This REST API allows to download data stores in the workspace.

- **URL**
  ::

    workspaces/:workspaceid/download

- **Method**
  ::

    GET

- **URL Params**

  *None*

- **Data Params**

  *Not Applicable*

- **Success Response**

  - **Code** 200 |br|
    **Content**
    ::

      <data stored in workspace>

- **Error Responses**

  - **Code** 500 Server Error |br|
    **Content**
    ::

      {
        'status' : 500,
        'message' : "No data exists in the workspace. Please upload the data to this workspace."
      }

  - **Code** 500 Server Error |br|
    **Content**
    ::

      Unable to route to service <url>

  - **Code** 500 Server Error |br|
    **Content**
    ::

      {
        'status' : 500,
        'message' : "<appropriate error message>"
      }

- **Sample Call**
  ::

    $.ajax({
      url: "${base-url}/workspaces/${workspace}/download",
      type: 'GET',
      success: function(r) {
        console.log(r);
      },
      error: function(r) {
        console.log(r);
      }
    });
