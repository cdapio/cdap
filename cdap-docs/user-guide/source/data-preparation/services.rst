.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2017 Cask Data, Inc.
    :description: The CDAP User Guide

.. _user-guide-data-preparation-services:

========
Services
========

- `Administration and Management`_
- `Executing Directives`_


.. _user-guide-data-preparation-services-administration-management:

Administration and Management
=============================

- `Creating a Workspace`_
- `Deleting a Workspace`_
- `Uploading a File to a Workspace`_
- `Downloading a File from a Workspace`_

Creating a Workspace
--------------------
This HTTP RESTful API call creates a workspace (a "scratch pad") for temporarily storing
data to be wrangled in the CDAP backend. A workspace is identified by an identifier (the
``<workspace-id>``) composed of alphanumeric (``a-z A-Z 0-9``) and underscore (``_``)
characters.

.. highlight:: json-ellipsis

.. list-table::
   :widths: 20 80

   * - **URL**
     - ``workspaces/<workspace-id>``
   * - **Method**
     - ``PUT``
   * - **URL Params**
     - *None*
   * - **Data Params**
     - *Not applicable*
   * - **Success Response**
     - **Code** 200 |br| ::

        {
          "status": 200,
          "message": "Successfully created workspace 'workspace-id'"
        }

   * - **Error Responses**
     - **Code** 500 Server Error |br| ::

        {
          "status": 500,
          "message": "<appropriate-error-message>"
        }

       .. highlight:: console

       **Code** 500 Server Error |br| ::

        Unable to route to service <url>

   * - **Sample Call**

       .. highlight:: javascript

     - ::

        $.ajax({
          url: "${base-url}/workspaces/${workspace-id}",
          method: "PUT",
          dataType: "json",
          success: function(r) {
            console.log(r);
          }
        });

   * - **Notes**
     - The API call will fail if the backend service is not started or unavailable, or if
       the dataset write fails.


Deleting a Workspace
--------------------
This HTTP RESTful API call deletes a workspace. It deletes any data stored with the
workspace.

.. highlight:: json-ellipsis

.. list-table::
   :widths: 20 80

   * - **URL**
     - ``workspaces/<workspace-id>``
   * - **Method**
     - ``DELETE``
   * - **URL Params**
     - *None*
   * - **Data Params**
     - *Not applicable*
   * - **Success Response**
     - **Code** 200 |br| ::

        {
          "status": 200,
          "message": "Successfully deleted workspace 'workspace-id'"
        }

   * - **Error Responses**
     - **Code** 500 Server Error |br| ::

        {
          "status": 500,
          "message": "<appropriate-error-message>"
        }

       .. highlight:: console

       **Code** 500 Server Error |br| ::

        Unable to route to service <url>

   * - **Sample Call**

       .. highlight:: javascript

     - ::

        $.ajax({
          url: "${base-url}/workspaces/${workspace-id}",
          method: "DELETE",
          dataType: "json",
          success: function(r) {
            console.log(r);
          }
        });

   * - **Notes**
     - The API call will fail if the backend service is not started or unavailable.


Uploading a File to a Workspace
-------------------------------
This HTTP RESTful API call uploads a file to a workspace. The file is split into lines based
on the end of line (EOL) delimiter.

.. highlight:: json-ellipsis

.. list-table::
   :widths: 20 80

   * - **URL**
     - ``workspaces/<workspace-id>/upload``
   * - **Method**
     - ``POST``
   * - **URL Params**
     - *None*
   * - **Data Params**
     - *Not applicable*
   * - **Success Response**
     - **Code** 200 |br| ::

        {
          "status": 200,
          "message": "Successfully uploaded data to workspace 'workspace-id' (records <record-count>)"
        }

   * - **Error Responses**
     - **Code** 500 Server Error |br| ::

        {
          "status": 500,
          "message": "Body not present; please post the file containing records to be wrangled."
        }

       **Code** 500 Server Error |br| ::

        {
          "status": 500,
          "message": "<appropriate-error-message>"
        }

       .. highlight:: console

       **Code** 500 Server Error |br| ::

        Unable to route to service <url>

   * - **Sample Call**

       .. highlight:: javascript

     - ::

        $.ajax({
          url: "${base-url}/workspaces/${workspace-id}/upload",
          method: "POST",
          data: data-filepath,
          dataType: "json",
          cache: false,
          contentType: "application/octet-stream",
          processData: false, // Don't process the files
          contentType: false,
          success: function(r) {
            console.log(r);
          },
          error: function(r) {
            console.log(r);
          }
        });

   * - **Notes**
     - The API call will fail if the backend service is not started or unavailable, or if the
       dataset write fails.


Downloading a File from a Workspace
-----------------------------------
This HTTP RESTful API downloads data stored in the workspace to a local file.

.. highlight:: json-ellipsis

.. list-table::
   :widths: 20 80

   * - **URL**
     - ``workspaces/<workspace-id>/download``
   * - **Method**
     - ``GET``
   * - **URL Params**
     - *None*
   * - **Data Params**
     - *Not applicable*
   * - **Success Response**

       .. highlight:: console

     - **Code** 200 |br| ::

         <data-stored-in-workspace>

   * - **Error Responses**

       .. highlight:: json-ellipsis

     - **Code** 500 Server Error |br| ::

        {
          "status": 500,
          "message": "No data exists in the workspace. Please upload the data to this workspace."
        }

       **Code** 500 Server Error |br| ::

        {
          "status": 500,
          "message": "<appropriate-error-message>"
        }

       .. highlight:: console

       **Code** 500 Server Error |br| ::

        Unable to route to service <url>

   * - **Sample Call**

       .. highlight:: javascript

     - ::

        $.ajax({
          url: "${base-url}/workspaces/${workspace-id}/download",
          method: "GET",
          dataType: "json",
          success: function(r) {
            console.log(r);
          },
          error: function(r) {
            console.log(r);
          }
        });

   * - **Notes**
     - The API call will fail if the backend service is not started or unavailable, or if the
       dataset read fails.


.. _user-guide-data-preparation-services-executing-directives:

Executing Directives
====================
This HTTP RESTful API applies directives on the data stored in a workspace.

.. highlight:: console

.. list-table::
   :widths: 20 80

   * - **URL**
     - ``workspaces/<workspace-id>/execute``
   * - **Method**
     - ``GET``
   * - **URL Params**
     - The directives to be executed are passed as query arguments. For multiple
       directives to be executed, they are passed as multiple query arguments.

       **Required** |br| ::

         directive=[encoded directive]

       **Optional** |br| ::

         limit=[numeric]

   * - **Data Params**
     - *Not applicable*
   * - **Success Response**

       .. highlight:: json-ellipsis

     - **Code** 200 |br| ::

        {
          "status": 200,
          "message": "Success",
          "items": "<count-of-records>",
          "header": [ "header-1", "header-2", ..., "header-n" ],
          "value": [
            { "<processed-record-1>" },
            { "<processed-record-2>" },
            ...
            { "<processed-record-n>" }
          ]
        }

   * - **Error Responses**
     - **Code** 500 Server Error |br| ::

        {
          "status": 500,
          "message": "<appropriate-error-message>"
        }

       .. highlight:: console

       **Code** 500 Server Error |br| ::

        Unable to route to service <url>

   * - **Sample Call**

       .. highlight:: javascript

     - ::

        $.ajax({
          url: "${base-url}/workspaces/${workspace-id}/execute",
          data: {
            directive: "<directive-1>",
            directive: "<directive-2>",
            ...
            directive: "<directive-k>",
            limit: "<count>"
          }
          cache: false,
          method: "GET",
          dataType: "json",
          success: function(r) {
            console.log(r);
          }
        });

   * - **Notes**
     - The API call will fail if the backend service is not started or unavailable, or if
       the dataset reads or writes fail.
