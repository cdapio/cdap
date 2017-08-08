/*
 * Copyright © 2015-2017 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

angular.module(PKG.name + '.commons')
  .factory('DAGPlusPlusFactory', function() {
    var defaultConnectionStyle = {
      PaintStyle: {
        strokeStyle: '#4e5568',
        lineWidth: 2,
        outlineColor: 'transparent',
        outlineWidth: 4
      },
      HoverPaintStyle: { strokeStyle: '#58b7f6', lineWidth: 4 }
    };

    var selectedConnectionStyle = {
      paintStyle: {
        strokeStyle: '#58b7f6',
        lineWidth: 4,
        outlineColor: 'transparent',
        outlineWidth: 4
      }
    };

    var defaultSettings = angular.extend({
      Anchor: [1, 0.5, 1, 0, 5, 0],
      Endpoint: 'Dot',
      EndpointStyle: { radius: 10 },
      Connector: ['Flowchart', { stub: [10, 15], alwaysRespectStubs: true, cornerRadius: 20, midpoint: 0.2 }],
      ConnectionOverlays: [
        ['Arrow', {
            location: 1,
            id: 'arrow',
            length: 14,
            foldback: 0.8
        }]
      ]
    }, defaultConnectionStyle);

    function getSettings() {
      var settings = {
        default: defaultSettings,
        selectedConnectionStyle: selectedConnectionStyle
      };

      return settings;
    }

    function getIcon(plugin) {
      var iconMap = {
        'script': 'icon-script',
        'scriptfilter': 'icon-scriptfilter',
        'twitter': 'icon-twitter',
        'cube': 'icon-cube',
        'data': 'fa-database',
        'database': 'icon-database',
        'table': 'icon-table',
        'kafka': 'icon-kafka',
        'stream': 'icon-streams',
        'jms': 'icon-jms',
        'projection': 'icon-projection',
        'amazonsqs': 'icon-amazonsqs',
        'datagenerator': 'icon-datagenerator',
        'validator': 'icon-validator',
        'corevalidator': 'corevalidator',
        'logparser': 'icon-logparser',
        'file': 'icon-file',
        'kvtable': 'icon-kvtable',
        's3': 'icon-s3',
        's3avro': 'icon-s3avro',
        's3parquet': 'icon-s3parquet',
        'snapshotavro': 'icon-snapshotavro',
        'snapshotparquet': 'icon-snapshotparquet',
        'tpfsavro': 'icon-tpfsavro',
        'tpfsparquet': 'icon-tpfsparquet',
        'sink': 'icon-sink',
        'hive': 'icon-hive',
        'structuredrecordtogenericrecord': 'icon-structuredrecord',
        'cassandra': 'icon-cassandra',
        'teradata': 'icon-teradata',
        'elasticsearch': 'icon-elasticsearch',
        'hbase': 'icon-hbase',
        'mongodb': 'icon-mongodb',
        'pythonevaluator': 'icon-pythonevaluator',
        'csvformatter': 'icon-csvformatter',
        'csvparser': 'icon-csvparser',
        'clonerecord': 'icon-clonerecord',
        'compressor': 'icon-compressor',
        'decompressor': 'icon-decompressor',
        'encoder': 'icon-encoder',
        'decoder': 'icon-decoder',
        'jsonformatter': 'icon-jsonformatter',
        'jsonparser': 'icon-jsonparser',
        'streamformatter': 'icon-streamformatter',
        'hdfs': 'icon-hdfs',
        'hasher': 'icon-hasher',
        'javascript': 'icon-javascript',
        'deduper': 'icon-deduper',
        'distinct': 'icon-distinct',
        'naivebayestrainer': 'icon-naivebayestrainer',
        'groupbyaggregate': 'icon-groupbyaggregate',
        'naivebayesclassifier': 'icon-naivebayesclassifier',
        'azureblobstore': 'icon-azureblobstore',
        'xmlreader': 'icon-XMLreader',
        'xmlparser': 'icon-XMLparser',
        'ftp': 'icon-FTP',
        'joiner': 'icon-joiner',
        'deduplicate': 'icon-deduplicator',
        'valuemapper': 'icon-valuemapper',
        'rowdenormalizer': 'icon-rowdenormalizer',
        'ssh': 'icon-ssh',
        'sshaction': 'icon-sshaction',
        'copybookreader': 'icon-COBOLcopybookreader',
        'excel': 'icon-excelinputsource',
        'encryptor': 'icon-Encryptor',
        'decryptor': 'icon-Decryptor',
        'hdfsfilemoveaction': 'icon-filemoveaction',
        'hdfsfilecopyaction': 'icon-filecopyaction',
        'sqlaction': 'icon-SQLaction',
        'impalahiveaction': 'icon-impalahiveaction',
        'email': 'icon-emailaction',
        'kinesissink': 'icon-Amazon-Kinesis',
        'bigquerysource': 'icon-Big-Query',
        'tpfsorc': 'icon-ORC',
        'groupby': 'icon-groupby',
        'sparkmachinelearning': 'icon-sparkmachinelearning',
        'solrsearch': 'icon-solr',
        'sparkstreaming': 'icon-sparkstreaming',
        'rename': 'icon-rename',
        'archive': 'icon-archive',
        'wrangler': 'icon-DataPreparation',
        'normalize': 'icon-normalize',
        'xmlmultiparser': 'icon-XMLmultiparser',
        'xmltojson': 'icon-XMLtoJSON',
        'decisiontreepredictor': 'icon-decisiontreeanalytics',
        'decisiontreetrainer': 'icon-DesicionTree',
        'hashingtffeaturegenerator': 'icon-HashingTF',
        'ngramtransform': 'icon-NGram',
        'tokenizer': 'icon-tokenizeranalytics',
        'skipgramfeaturegenerator': 'icon-skipgram',
        'skipgramtrainer': 'icon-skipgramtrainer',
        'logisticregressionclassifier': 'icon-logisticregressionanalytics',
        'logisticregressiontrainer': 'icon-LogisticRegressionclassifier',
        'hdfsdelete': 'icon-hdfsdelete',
        'hdfsmove': 'icon-hdfsmove',
        'windowssharecopy': 'icon-windowssharecopy',
        'httppoller': 'icon-httppoller',
        'window': 'icon-window',
        'run': 'icon-Run',
        'oracleexport': 'icon-OracleDump',
        'snapshottext': 'icon-SnapshotTextSink',
        'errorcollector': 'fa-exclamation-triangle',
        'mainframereader': 'icon-MainframeReader',
        'fastfilter': 'icon-fastfilter',
        'trash': 'icon-TrashSink',
        'staterestore': 'icon-Staterestore',
        'topn': 'icon-TopN',
        'wordcount': 'icon-WordCount',
        'datetransform': 'icon-DateTransform',
        'sftpcopy': 'icon-FTPcopy',
        'sftpdelete': 'icon-FTPdelete',
        'validatingxmlconverter': 'icon-XMLvalidator',
        'wholefilereader': 'icon-Filereader',
        'xmlschemaaction': 'icon-XMLschemagenerator',
        's3toredshift': 'icon-S3toredshift',
        'redshifttos3': 'icon-redshifttoS3',
        'verticabulkexportaction': 'icon-Verticabulkexport',
        'verticabulkimportaction': 'icon-Verticabulkload',
        'loadtosnowflake': 'icon-snowflake',
        'kudu': 'icon-apachekudu',
        'orientdb': 'icon-OrientDB',
        'recordsplitter': 'icon-recordsplitter',
        'scalasparkprogram': 'icon-spark',
        'scalasparkcompute': 'icon-spark',
        'cdcdatabase': 'icon-database',
        'cdchbase': 'icon-hbase',
        'cdckudu': 'icon-apachekudu',
        'changetrackingsqlserver': 'icon-database'
      };

      var pluginName = plugin.toLowerCase();
      var icon = iconMap[pluginName] ? iconMap[pluginName]: 'fa-plug';
      return icon;
    }

    function getGraphLayout(nodes, connections, separation) {
      var rankSeparation = separation || 200;

      var graph = new dagre.graphlib.Graph();
      graph.setGraph({
        nodesep: 90,
        ranksep: rankSeparation,
        rankdir: 'LR',
        marginx: 0,
        marginy: 0
      });
      graph.setDefaultEdgeLabel(function() { return {}; });

      nodes.forEach(function (node) {
        var id = node.id || node.name;
        graph.setNode(id, { label: node.label, width: 100, height: 100 });
      });

      connections.forEach(function (connection) {
        graph.setEdge(connection.from, connection.to);
      });

      dagre.layout(graph);
      return graph;
    }

    return {
      getSettings: getSettings,
      getIcon: getIcon,
      getGraphLayout: getGraphLayout
    };

  });
