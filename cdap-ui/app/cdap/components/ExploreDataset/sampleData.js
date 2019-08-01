export const sinks = {
    "configParamList": [
      {
        "paramName": "hiveSinkConfig",
        "description": "Hive Sink Configurations",
        "isCollection": false,
        "dataType": "unknown",
        "isMandatory": true,
        "defaultValue": "",
        "isSchemaSpecific": false,
        "subParams": [
          {
            "paramName": "hiveHostUrl",
            "description": "The hostname of the Hive metastore.",
            "isCollection": false,
            "dataType": "string",
            "isMandatory": true,
            "defaultValue": "jdbc:hive2://localhost:10000",
            "isSchemaSpecific": false,
            "subParams": null,
            "displayName": "Hive Host URL",
            "groupName": "dataSink"
          },
          {
            "paramName": "hiveTableName",
            "description": "Name of the Hive table",
            "isCollection": false,
            "dataType": "string",
            "isMandatory": true,
            "defaultValue": "",
            "isSchemaSpecific": false,
            "subParams": null,
            "displayName": "Hive Table Name",
            "groupName": "dataSink"
          },
          {
            "paramName": "hiveMetastoreDir",
            "description": "The location directory of Hive metastore in HDFS.",
            "isCollection": false,
            "dataType": "string",
            "isMandatory": false,
            "defaultValue": "/apps/hive/warehouse",
            "isSchemaSpecific": false,
            "subParams": null,
            "displayName": "Hive Metastore Dir",
            "groupName": "dataSink"
          },
          {
            "paramName": "hiveDatabaseName",
            "description": "Name of the Hive database",
            "isCollection": false,
            "dataType": "string",
            "isMandatory": false,
            "defaultValue": "default",
            "isSchemaSpecific": false,
            "subParams": null,
            "displayName": "Hive Database Name",
            "groupName": "dataSink"
          },
          {
            "paramName": "hiveSavemode",
            "description": "This is the mode to save the incoming data when the table is already present in Hive. You can save it either in append mode, overwrite mode, or error mode. The error mode returns an error if an existing table name is incorrectly provided.",
            "isCollection": false,
            "dataType": "string",
            "isMandatory": false,
            "defaultValue": "overwrite",
            "isSchemaSpecific": false,
            "subParams": null,
            "displayName": "Hive Save Mode",
            "groupName": "dataSink"
          },
          {
            "paramName": "hiveStorageFormat",
            "description": "The format to be used to save Hive table. Supported formats are Optimized Row Columnar (ORC) and Parquet.",
            "isCollection": false,
            "dataType": "string",
            "isMandatory": false,
            "defaultValue": "parquet",
            "isSchemaSpecific": false,
            "subParams": null,
            "displayName": "Hive Storage Format",
            "groupName": "dataSink"
          },
          {
            "paramName": "hivePartitioningColumns",
            "description": "Comma separated values of column names to apply partitioning logic to the table.",
            "isCollection": false,
            "dataType": "string",
            "isMandatory": false,
            "defaultValue": "",
            "isSchemaSpecific": false,
            "subParams": null,
            "displayName": "Hive Partition Columns",
            "groupName": "dataSink"
          },
          {
            "paramName": "hiveSortByColumns",
            "description": "A key value pair of column-name:mode on which data is sorted. The mode is either ascending or descending.",
            "isCollection": false,
            "dataType": "string",
            "isMandatory": false,
            "defaultValue": "",
            "isSchemaSpecific": false,
            "subParams": null,
            "displayName": "Hive Sort By Columns",
            "groupName": "dataSink"
          },
          {
            "paramName": "hiveProperties",
            "description": "Key value pair of ORC specific properties and their values. These properties are optional.",
            "isCollection": false,
            "dataType": "string",
            "isMandatory": false,
            "defaultValue": "",
            "isSchemaSpecific": false,
            "subParams": null,
            "displayName": "Hive Additional Properties",
            "groupName": "dataSink"
          },
          {
            "paramName": "hiveExternalTableRequired",
            "description": "Set this config as true if external table required and provide external table location in table location config.",
            "isCollection": false,
            "dataType": "string",
            "isMandatory": true,
            "defaultValue": "0",
            "isSchemaSpecific": false,
            "subParams": null,
            "displayName": "Hive External table",
            "groupName": "dataSink"
          },
          {
            "paramName": "hiveTableLocationDir",
            "description": "The location directory of Hive table in HDFS.",
            "isCollection": false,
            "dataType": "string",
            "isMandatory": false,
            "defaultValue": "",
            "isSchemaSpecific": false,
            "subParams": null,
            "displayName": "Hive Table location",
            "groupName": "dataSink"
          },
          {
            "paramName": "hiveTimeColumns",
            "description": "A key value pair of time-based-fields:expected-hive-datatype for writing into Hive table. Note that data in time-based-field must contain Epoch values in milliseconds and the data-type of time-based-fields should be string or long. In expected-hive-datatype the date data type is of format yyyy-MM-dd and timestamp data type is of format yyyy-MM-dd hh:mm:ss. The time column Epoch values are converted according to your default system timezone.",
            "isCollection": false,
            "dataType": "string",
            "isMandatory": false,
            "defaultValue": "",
            "isSchemaSpecific": false,
            "subParams": null,
            "displayName": "Hive Time Columns",
            "groupName": "dataSink"
          },
          {
            "paramName": "hiveProperties",
            "description": "Key value pair of ORC specific properties and their values. These properties are optional.",
            "isCollection": false,
            "dataType": "string",
            "isMandatory": false,
            "defaultValue": "",
            "isSchemaSpecific": false,
            "subParams": null,
            "displayName": "Hive Additional Properties",
            "groupName": "dataSink"
          }
        ],
        "displayName": "Hive Sink Configurations",
        "groupName": "dataSink"
      }
    ]
  };
export const configurations ={
    "configParamList": [
      {
        "paramName": "numDataPartitions",
        "description": "Specify number of partitions to be used by spark for its data while executing cdap pipeline",
        "isCollection": false,
        "dataType": "int",
        "isMandatory": false,
        "defaultValue": "10",
        "isSchemaSpecific": false,
        "subParams": null,
        "displayName": "Spark Data Partitions",
        "groupName": "advanced"
      },
      {
        "paramName": "postActionUrl",
        "description": "Base URL where http post call is made to update generated dataschema info",
        "isCollection": false,
        "dataType": "string",
        "isMandatory": false,
        "defaultValue": "",
        "isSchemaSpecific": false,
        "subParams": null,
        "displayName": "Post Action Http Base URL",
        "groupName": "advanced"
      },
      {
        "paramName": "sparkDriverMemory",
        "description": "Spark Driver Memory",
        "isCollection": false,
        "dataType": "string",
        "isMandatory": true,
        "defaultValue": "5g",
        "isSchemaSpecific": false,
        "subParams": null,
        "displayName": "Spark Driver Memory",
        "groupName": "advanced"
      },
      {
        "paramName": "sparkSqlShufflePartitions",
        "description": "Spark sql shuffle partitions",
        "isCollection": false,
        "dataType": "string",
        "isMandatory": false,
        "defaultValue": "5",
        "isSchemaSpecific": false,
        "subParams": null,
        "displayName": "Spark Sql Shuffle Partitions",
        "groupName": "advanced"
      },
      {
        "paramName": "sparkExecutorMemory",
        "description": "Spark Executor Memory",
        "isCollection": false,
        "dataType": "string",
        "isMandatory": true,
        "defaultValue": "5g",
        "isSchemaSpecific": false,
        "subParams": null,
        "displayName": "Spark Executor Memory",
        "groupName": "advanced"
      },
      {
        "paramName": "sparkExecutorCores",
        "description": "Spark Executor Cores",
        "isCollection": false,
        "dataType": "string",
        "isMandatory": true,
        "defaultValue": "2",
        "isSchemaSpecific": false,
        "subParams": null,
        "displayName": "Spark Executor Cores",
        "groupName": "advanced"
      },
      {
        "paramName": "sparkDriverCores",
        "description": "Spark Driver Cores",
        "isCollection": false,
        "dataType": "string",
        "isMandatory": true,
        "defaultValue": "1",
        "isSchemaSpecific": false,
        "subParams": null,
        "displayName": "Spark Driver Cores",
        "groupName": "advanced"
      },
      {
        "paramName": "sparkExecutorInstances",
        "description": "Spark Executor Instances",
        "isCollection": false,
        "dataType": "string",
        "isMandatory": true,
        "defaultValue": "2",
        "isSchemaSpecific": false,
        "subParams": null,
        "displayName": "Spark Executor Instances",
        "groupName": "advanced"
      },
      {
        "paramName": "sparkPipelineCachingStorageLevel",
        "description": "Storage Level to be used for persisting Spark RDDs like \"MEMORY_ONLY\", \"MEMORY_AND_DISK\"",
        "isCollection": false,
        "dataType": "string",
        "isMandatory": true,
        "defaultValue": "MEMORY_AND_DISK",
        "isSchemaSpecific": false,
        "subParams": null,
        "displayName": "Spark RDDs Persistence Caching Level",
        "groupName": "advanced"
      },
      {
        "paramName": "sparkMemoryFraction",
        "description": "Fraction of (heap space - 300MB) used for execution and storage. The lower this is, the more frequently spills and cached data eviction occur. The purpose of this config is to set aside memory for internal metadata, user data structures, and imprecise size estimation in the case of sparse, unusually large records. ",
        "isCollection": false,
        "dataType": "string",
        "isMandatory": false,
        "defaultValue": "0.6",
        "isSchemaSpecific": false,
        "subParams": null,
        "displayName": "Spark Memory Fraction",
        "groupName": "advanced"
      },
      {
        "paramName": "sparkMemoryStorageFraction",
        "description": "Amount of storage memory immune to eviction, expressed as a fraction of the size of the region set aside by spark.memory.fraction. The higher this is, the less working memory may be available to execution and tasks may spill to disk more often.",
        "isCollection": false,
        "dataType": "string",
        "isMandatory": false,
        "defaultValue": "0.1",
        "isSchemaSpecific": false,
        "subParams": null,
        "displayName": "Spark Memory Storage Fraction",
        "groupName": "advanced"
      },
      {
        "paramName": "categoricalDistributionConfig",
        "description": "This plugin finds the distribution of data in categorical features. It basically finds the value counts of different unique values in a feature after grouping by on the target feature. The feature should be specified as non-numeric in the input schema.",
        "isCollection": false,
        "dataType": "unknown",
        "isMandatory": false,
        "defaultValue": "",
        "isSchemaSpecific": true,
        "subParams": [
          {
            "paramName": "categoricalDistributionFeatures",
            "description": "Name of the feature column",
            "isCollection": true,
            "dataType": "string",
            "isMandatory": true,
            "defaultValue": "",
            "isSchemaSpecific": true,
            "subParams": null,
            "displayName": "Features Column",
            "groupName": "eda"
          },
          {
            "paramName": "categoricalDistributionTargetVariable",
            "description": "Name of the target column. This column will be used to group by the feature values if the target is non numeric. The target type is specified in the input schema",
            "isCollection": false,
            "dataType": "unknown",
            "isMandatory": true,
            "defaultValue": "",
            "isSchemaSpecific": true,
            "subParams": null,
            "displayName": "Target Column",
            "groupName": "eda"
          },
        ],
        "displayName": "Categorical Distribution",
        "groupName": "eda"
      },
      {
        "paramName": "numericDistributionConfig",
        "description": "Numeric Distribution Plugin Configurations",
        "isCollection": false,
        "dataType": "unknown",
        "isMandatory": false,
        "defaultValue": "",
        "isSchemaSpecific": true,
        "subParams": [
          {
            "paramName": "numericlDistributionFeatures",
            "description": "Specify all input feature columns",
            "isCollection": true,
            "dataType": "number",
            "isMandatory": true,
            "defaultValue": "",
            "isSchemaSpecific": true,
            "subParams": null,
            "displayName": "Features Column",
            "groupName": "eda"
          },
          {
            "paramName": "numericDistributionNumBins",
            "description": "The number of bins in which the data will be split.",
            "isCollection": false,
            "dataType": "unknown",
            "isMandatory": true,
            "defaultValue": "",
            "isSchemaSpecific": false,
            "subParams": null,
            "displayName": "Num Bins",
            "groupName": "eda"
          }
        ],
        "displayName": "Numeric Distribution",
        "groupName": "eda"
      },
      {
        "paramName": "describeConfig",
        "description": "This plugin does basic data description. The output of this stage includes metrics namely count of null entries in every column, count of unique values",
        "isCollection": false,
        "dataType": "unknown",
        "isMandatory": false,
        "defaultValue": "",
        "isSchemaSpecific": true,
        "subParams": [
          {
            "paramName": "describeFeatures",
            "description": "Specify all input feature columns",
            "isCollection": true,
            "dataType": "all",
            "isMandatory": true,
            "defaultValue": "",
            "isSchemaSpecific": true,
            "subParams": null,
            "displayName": "Features Column",
            "groupName": "eda"
          },
          {
            "paramName": "describeTarget",
            "description": "Name of the target column against which the correlation of other numeric columns would be found out.",
            "isCollection": false,
            "dataType": "unknown",
            "isMandatory": true,
            "defaultValue": "",
            "isSchemaSpecific": true,
            "subParams": null,
            "displayName": "Target Column",
            "groupName": "eda"
          }
        ],
        "displayName": "Describe",
        "groupName": "eda"
      },
      {
        "paramName": "entropyConfig",
        "description": "This plugin checks the sample stability per attribute by distribution entropy",
        "isCollection": false,
        "dataType": "unknown",
        "isMandatory": false,
        "defaultValue": "",
        "isSchemaSpecific": true,
        "subParams": [
          {
            "paramName": "entropyResampleRate",
            "description": "Resample Rate Values can be : S, T, H, D, W , M, Q which represent seconds, minutes, hours, days, weeks, months and quarter respectively. User can add a numeric in from of letters to make larger like 10T will create bins of 10 minutes each",
            "isCollection": false,
            "dataType": "string",
            "isMandatory": true,
            "defaultValue": "",
            "isSchemaSpecific": false,
            "subParams": null,
            "displayName": "Resample Rate",
            "groupName": "eda"
          },
          {
            "paramName": "entropyTimeColumn",
            "description": "Name of the time column. This column will be used to resampling and it is assumed it is present in input schema.",
            "isCollection": false,
            "dataType": "string",
            "isMandatory": true,
            "defaultValue": "",
            "isSchemaSpecific": true,
            "subParams": null,
            "displayName": "Time Column",
            "groupName": "eda"
          },
          {
            "paramName": "entropyAttribute",
            "description": "Name of the Attribute to be considered while entropy analysis.",
            "isCollection": false,
            "dataType": "string",
            "isMandatory": true,
            "defaultValue": "",
            "isSchemaSpecific": true,
            "subParams": null,
            "displayName": "Attribute",
            "groupName": "eda"
          },
        ],
        "displayName": "Entropy",
        "groupName": "eda"
      },
      {
        "paramName": "correlationConfig",
        "description": "Correlation refers to any of a broad class of statistical relationships involving dependence.",
        "isCollection": false,
        "dataType": "unknown",
        "isMandatory": false,
        "defaultValue": "",
        "isSchemaSpecific": true,
        "subParams": [
          {
            "paramName": "correlationTarget",
            "description": "Name of the target column.",
            "isCollection": false,
            "dataType": "unknown",
            "isMandatory": true,
            "defaultValue": "",
            "isSchemaSpecific": true,
            "subParams": null,
            "displayName": "Target Column",
            "groupName": "eda"
          },
          {
            "paramName": "correlationMethod",
            "description": "You can specify correlation method here like pearson ,spearman etc.",
            "isCollection": false,
            "dataType": "string",
            "isMandatory": true,
            "defaultValue": "pearson",
            "isSchemaSpecific": false,
            "subParams": null,
            "displayName": "Correlation Method",
            "groupName": "eda"
          }
        ],
        "displayName": "Correlation",
        "groupName": "eda"
      },
      {
        "paramName": "xratioConfig",
        "description": "X-Ratio is used to find out associations that there may be between Property1 and Property2 where Property is set of feature values defining a typical characteristics of the system.",
        "isCollection": false,
        "dataType": "unknown",
        "isMandatory": false,
        "defaultValue": "",
        "isSchemaSpecific": true,
        "subParams": [
          {
            "paramName": "xratioFeatures",
            "description": "Name of the feature column.",
            "isCollection": true,
            "dataType": "all",
            "isMandatory": true,
            "defaultValue": "",
            "isSchemaSpecific": true,
            "subParams": null,
            "displayName": "Features Column",
            "groupName": "eda"
          },
          {
            "paramName": "xratioTarget",
            "description": "Name of the target column.",
            "isCollection": false,
            "dataType": "unknown",
            "isMandatory": true,
            "defaultValue": "",
            "isSchemaSpecific": true,
            "subParams": null,
            "displayName": "Target Column",
            "groupName": "eda"
          },
        ],
        "displayName": "XRatio",
        "groupName": "eda"
      }
    ]
  };
