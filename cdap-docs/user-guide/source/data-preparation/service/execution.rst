.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2017 Cask Data, Inc.

===================
Directive Execution
===================

Executing Directives
--------------------

Applies directives on the data in a workspace.

-  **URL**

``workspaces/:workspaceid/execute``

-  **Method**

``GET``

-  **URL Params**

The directives to be executed are passed as query arguments. For
multiple directives to be executed, they are passed as multiple query
arguments.

**Required**

``directive=[encoded directive]``

**Optional**

``limit=[numeric]``

-  **Data Params**

*Not Applicable*

-  **Success Response**

-  **Code:** 200 **Content:**

   ::

         {
           'status': 200,
           'message': 'Success',
           'items': <count of records>,
           'header': [ 'header-1', 'header-2', ..., 'header-n' ],
           'value': {
             { processed record - 1},
             { processed record - 2},
             . . .
             { processed record - n}
           }
         }

-  **Error Responses**

-  **Code:** 500 Server Error **Content:**

   ::

         {
           'status': 500,
           'message': "<appropriate error message>"
         }

   or

-  **Code:** 500 Server Error **Content:**

   ::

         Unable to route to service <url>

-  **Sample Call**

``$.ajax({       url: "${base-url}/workspaces/${workspace}/execute",       data: {         'directive': <directive-1>,         'directive': <directive-2>,         ...         'directive': <directive-k>         'limit': <count>       }       cache: false       type : "GET",       success : function(r) {         console.log(r);       }     });``
