.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2017 Cask Data, Inc.

======================================
Administration and Management Services
======================================

-  `Creating a Workspace <#creating-a-workspace>`__
-  `Deleting a Workspace <#deleting-a-workspace>`__
-  `Uploading a File to a
   Workspace <#uploading-a-file-to-a-workspace>`__
-  `Downloading a File from a
   Workspace <#downloading-a-file-from-a-workspace>`__

 ## Creating a Workspace

This RESTful API call creates a workspace, a scratch pad for temporarily
storing the data to be wrangled in the backend. A workspace is
identified by a name containing either alphanumeric or underscore (\_)
characters.

-  **URL**

``workspaces/:workspaceid``

-  **Method**

``PUT``

-  **URL Params**

*None*

-  **Data Params**

*Not Applicable*

-  **Success Response**

-  **Code** 200 **Content**

   ::

         {
           'status': 200,
           'message': "Successfully created workspace ':workspaceid'"
         }

-  **Error Responses**

-  **Code** 500 Server Error **Content**

   ::

         {
           'status': 500,
           'message': "<appropriate error message>"
         }

   or

-  **Code** 500 Server Error **Content**

   ::

         Unable to route to service <url>

-  **Sample Call**

``$.ajax({       url: "${base-url}/workspaces/${workspace}",       dataType: "json",       type : "PUT",       success : function(r) {         console.log(r);       }     });``

-  **Notes**

The API call will fail if the backend service is not started or if the
dataset write fails.

 ## Deleting a Workspace

This RESTful API call deletes a workspace. This will also delete any
data associated with it.

-  **URL**

``workspaces/:workspaceid``

-  **Method**

``DELETE``

-  **URL Params**

*None*

-  **Data Params**

*Not Applicable*

-  **Success Response**

-  **Code** 200 **Content**

   ::

         {
           'status': 200,
           'message': "Successfully deleted workspace ':workspaceid'"
         }

-  **Error Responses**

-  **Code** 500 Server Error **Content**

   ::

         {
           'status': 500,
           'message': "<appropriate error message>"
         }

   or

-  **Code** 500 Server Error **Content**

   ::

         Unable to route to service <url>

-  **Sample Call**

``$.ajax({       url: "${base-url}/workspaces/${workspace}",       dataType: "json",       type : "DELETE",       success : function(r) {         console.log(r);       }     });``

-  **Notes**

The API call will fail if the backend service is not started or if the
dataset write fails.

 ## Uploading a File to a Workspace

This RESTful API call will upload a file to a workspace. The file is
split into lines based on a line delimiter (EOL).

-  **URL**

``workspaces/:workspaceid/upload``

-  **Method**

``POST``

-  **URL Params**

*None*

-  **Data Params**

*Not Applicable*

-  **Success Response**

-  **Code** 200 **Content**

   ::

         {
           'status': 200,
           'message': "Successfully uploaded data to workspace ':workspaceid' (records 1000)"
         }

-  **Error Responses**

-  **Code** 500 Server Error **Content**

   ::

         {
           'status': 500,
           'message': "Body not present, please post the file containing the records to be wrangle."
         }

   or

-  **Code** 500 Server Error **Content**

   ::

         Unable to route to service <url>

   or

-  **Code** 500 Server Error **Content**

   ::

         {
           'status': 500,
           'message': "<appropriate error message>"
         }

-  **Sample Call**

``$.ajax({         url: "${base-url}/workspaces/${workspace}/upload",         type: 'POST',         data: data,         cache: false,         contentType: 'application/octet-stream',         processData: false, // Don't process the files         contentType: false,         success: function(r) {           console.log(r);         },         error: function(r) {           console.log(r);       }     });``

 ## Downloading a File from a Workspace

This RESTful API will download to a file the data stores in a workspace.

-  **URL**

``workspaces/:workspaceid/download``

-  **Method**

``GET``

-  **URL Params**

*None*

-  **Data Params**

*Not Applicable*

-  **Success Response**

-  **Code** 200 **Content**

   ::

         <data stored in workspace>

-  **Error Responses**

-  **Code** 500 Server Error **Content**

   ::

         {
           'status': 500,
           'message': "No data exists in the workspace. Please upload the data to this workspace."
         }

   or

-  **Code** 500 Server Error **Content**

   ::

         Unable to route to service <url>

   or

-  **Code** 500 Server Error **Content**

   ::

         {
           'status': 500,
           'message': "<appropriate error message>"
         }

-  **Sample Call**

``$.ajax({         url: "${base-url}/workspaces/${workspace}/download",         type: 'GET',         success: function(r) {           console.log(r);         },         error: function(r) {           console.log(r);       }     });``
