# Error Collector


Description
-----------
The ErrorCollector plugin takes errors emitted from the previous stage and flattens them by adding
the error message, code, and stage to the record and outputting the result.

Use Case
--------
The plugin is used when you want to capture errors emitted from another stage and pass them along
with all the error information flattened into the record. For example, you may want to connect a sink
to this plugin in order to store and later examine the error records.

Properties
----------
**messageField:** The name of the error message field to use in the output schema. Defaults to 'errMsg'.
If this is not specified, the error message will be dropped.

**codeField:** The name of the error code field to use in the output schema. Defaults to 'errCode'.
If this is not specified, the error code will be dropped.

**stageField:** The name of the error stage field to use in the output schema. Defaults to 'errStage'.
If this is not specified, the error stage will be dropped.


Example
-------
This example adds the error message, error code, and error stage as the 'errMsg', 'errCode', and 'errStage' fields.

    {
        "name": "ErrorCollector",
        "type": "errortransform",
        "properties": {
            "messageField": "errMsg",
            "codeField": "errCode",
            "stageField": "errStage"
        }
    }

For example, suppose the plugin receives this error record:

    +============================+
    | field name | type | value  |
    +============================+
    | A          | int  | 10     |
    | B          | int  | 20     |
    +============================+

with error code 17, error message 'invalid', from stage 'parser'. It will add the error information
to the record and output:

    +===============================+
    | field name | type   | value   |
    +===============================+
    | A          | int    | 10      |
    | B          | int    | 20      |
    | errMsg     | string | invalid |
    | errCode    | int    | 17      |
    | errStage   | string | parser  |
    +===============================+
