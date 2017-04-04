# XML Reader


Description
-----------
The XML Reader plugin is a source plugin that allows users to read XML files stored on HDFS.


Use Case
--------
A user would like to read XML files that have been dropped into HDFS.
These can range in size from small to very large XML files. The XMLReader will read and parse the files,
and when used in conjunction with the XMLParser plugin, fields can be extracted.
This reader emits one XML event, specified by the node path property, for each file read.


Properties
----------
| Configuration              | Required | Default | Description                                                                                                                                                                                                                                                                     |
| :------------------------- | :------: | :------ | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **Reference Name**         |  **Y**   | None    | This will be used to uniquely identify this source for lineage, annotating metadata, etc.                                                                                                                                                                                       |
| **Path**                   |  **Y**   | None    | Path to file(s) to be read. If a directory is specified, terminate the path name with a '/'. This leverages glob syntax as described in the [Java Documentation](https://docs.oracle.com/javase/tutorial/essential/io/fileOps.html#glob).                                       |
| **Pattern**                |  **N**   | None    | The regular expression pattern used to select specific files. This should be used in cases when the glob syntax in the `Path` is not precise enough. See examples in the Usage Notes.                                                                                           |
| **Node Path**              |  **Y**   | None    | Node path (XPath) to emit as an individual event from the XML schema. Example: '/book/price' to read only the price from under the book node. For more information about XPaths, see the [Java Documentation](https://docs.oracle.com/javase/tutorial/jaxp/xslt/xpath.html).    |
| **Action After Process**   |  **Y**   | None    | Action to be taken after processing of the XML file. Possible actions are: (DELETE) delete from HDFS; (ARCHIVE) archive to the target location; and (MOVE) move to the target location.                                                                                         |
| **Target Folder**          |  **N**   | None    | Target folder path if the user select an action for after the process, either one of ARCHIVE or MOVE. Target folder must be an existing directory.                                                                                                                              |
| **Reprocessing Required?** |  **Y**   | Yes     | Specifies whether the files should be reprocessed. If set to `No`, the files are tracked and will not be processed again on future runs of the pipeline.                                                                                                                        |
| **Table Name**             |  **N**   | None    | When keeping track of processed files, this is the name of the Table dataset used to store the data. This is required when reprocessing is set to `No`.                                                                                                                         |
| **Table Expiry Period**    |  **N**   | None    | The amount of time (in days) to wait before clearing the table used to track processed filed. If omitted, data will not expire in the tracking table. Example: for `tableExpiryPeriod = 30`, data before 30 days is deleted from the table.                                     |
| **Temporary Folder**       |  **Y**   | None    | An existing folder path with read and write access for the current user. This is required for storing temporary files containing paths of the processed XML files. These temporary files will be read at the end of the job to update the file track table. Defaults to `/tmp`. |


Usage Notes
-----------
When specifying a regular expression for filtering files, you must use glob syntax in the folder path.
This usually means ending the path with '/*'.

Here are some regular expression pattern examples:
1. Use '^' to select files with names starting with 'catalog', such as '^catalog'.
2. Use '$' to select files with names ending with 'catalog.xml', such as 'catalog.xml$'.
3. Use '.\*' to select files with a name that contains 'catalogBook', such as 'catalogBook.*'.


Example
-------
This example reads data from the folder "hdfs:/cask/source/xmls/" and emits XML records on the basis of the node path
"/catalog/book/title". It will generate structured records with the fields 'offset', 'fileName', and 'record'.
It will move the XML files to the target folder "hdfs:/cask/target/xmls/" and update the processed file information
in the table named "trackingTable".

      {
         "name": "XMLReaderBatchSource",
         "plugin":{
                    "name": "XMLReaderBatchSource",
                    "type": "batchsource",
                    "properties":{
                                  "referenceName": "referenceName""
                                  "path": "hdfs:/cask/source/xmls/*",
                                  "Pattern": "^catalog.*"
                                  "nodePath": "/catalog/book/title"
                                  "actionAfterProcess" : "Move",
                                  "targetFolder":"hdfs:/cask/target/xmls/",
                                  "reprocessingRequired": "No",
                                  "tableName": "trackingTable",
                                  "temporaryFolder": "hdfs:/cask/tmp/"
                    }
         }
      }


 For this XML as an input:

     <catalog>
       <book id="bk104">
         <author>Corets, Eva</author>
         <title>Oberon's Legacy</title>
         <genre>Fantasy</genre>
         <price><base>5.95</base><tax><surcharge>13.00</surcharge><excise>13.00</excise></tax></price>
         <publish_date>2001-03-10</publish_date>
         <description><name><name>In post-apocalypse England, the mysterious
         agent known only as Oberon helps to create a new life
         for the inhabitants of London. Sequel to Maeve
         Ascendant.</name></name></description>
       </book>
       <book id="bk105">
         <author>Corets, Eva</author>
         <title>The Sundered Grail</title>
         <genre>Fantasy</genre>
         <price><base>5.95</base><tax><surcharge>14.00</surcharge><excise>14.00</excise></tax></price>
         <publish_date>2001-09-10</publish_date>
         <description><name>The two daughters of Maeve, half-sisters,
         battle one another for control of England. Sequel to
         Oberon's Legacy.</name></description>
       </book>
     </catalog>

 The output records will be:

    +==================================================================================+
    | offset | filename                            | record                            |
    +==================================================================================+
    | 2      | hdfs:/cask/source/xmls/catalog.xml  | <title>Oberon's Legacy</title>    |
    | 13     | hdfs:/cask/source/xmls/catalog.xml  | <title>The Sundered Grail</title> |
    +==================================================================================+

---
- CDAP Pipelines Plugin Type: batchsource
- CDAP Pipelines Version: 1.7.0
