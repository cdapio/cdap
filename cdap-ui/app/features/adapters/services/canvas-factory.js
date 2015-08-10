angular.module(PKG.name + '.feature.adapters')
  .factory('CanvasFactory', function(myHelpers, MyPlumbService, $q, $alert) {
    function getNodes(config) {
      var nodes = [];
      var i =0;
      nodes.push({
        id: config.source.name + '-source-' + (++i),
        name: config.source.name,
        type: 'source',
        properties: config.source.properties
      });
      config.transforms.forEach(function(transform) {
        nodes.push({
          id: transform.name + '-transform-' + (++i),
          name: transform.name,
          type: 'transform',
          properties: transform.properties
        });
      });
      nodes.push({
        id: config.sink.name + '-sink-' + (++i),
        name: config.sink.name,
        type: 'sink',
        properties: config.sink.properties
      });
      return nodes;
    }

    function extractMetadataFromDraft(config, data) {
      var returnConfig = {};
      // Too many ORs. Should be removed when all drafts eventually are
      // resaved in the new format. This is temporary
      returnConfig.name = myHelpers.objectQuery(config, 'metadata', 'name')
      || myHelpers.objectQuery(data, 'name');

      returnConfig.description = myHelpers.objectQuery(config, 'metadata', 'description')
      || myHelpers.objectQuery(data, 'description');

      var template = myHelpers.objectQuery(config, 'metadata', 'type')
      || myHelpers.objectQuery(data, 'template');

      returnConfig.template = {
        type: template
      };
      if (template === 'ETLBatch') {
        returnConfig.template.schedule = {};
        returnConfig.template.schedule.cron = myHelpers.objectQuery(config, 'schedule', 'cron')
        || myHelpers.objectQuery(config, 'schedule');
      } else if (template === 'ETLRealtime') {
        returnConfig.template.instance = myHelpers.objectQuery(config, 'instance')
        || myHelpers.objectQuery(config, 'metadata', 'template', 'instance');
      }

      return returnConfig;
    }

    function getConnectionsBasedOnNodes(nodes) {
      var j;
      var connections = [];
      for (j=0; j<nodes.length-1; j++) {
        connections.push({
          source: nodes[j].id,
          target: nodes[j+1].id
        });
      }
      return connections;
    }

    function exportAdapter(detailedConfig, name) {
      var defer = $q.defer();
      if (!name || name === '') {
        detailedConfig.name = 'noname';
      } else {
        detailedConfig.name =  name;
      }

      detailedConfig.ui = {
        nodes: angular.copy(MyPlumbService.nodes),
        connections: angular.copy(MyPlumbService.connections)
      };

      angular.forEach(detailedConfig.ui.nodes, function(node) {
        delete node._backendProperties;
      });
      detailedConfig.ui.connections.forEach(function(conn) {
        delete conn.visited;
      });

      var content = JSON.stringify(detailedConfig, null, 4);
      var blob = new Blob([content], { type: 'application/json'});
      defer.resolve({
        name:  detailedConfig.name + '-' + detailedConfig.template,
        url: URL.createObjectURL(blob)
      });
      return defer.promise;
    }

    function importAdapter(files, type) {
      var defer = $q.defer();
      var reader = new FileReader();
      reader.readAsText(files[0], "UTF-8");

      reader.onload = function (evt) {
        var errorMessage;
        var result;
        try {
          result = JSON.parse(evt.target.result);
        } catch(e) {
          errorMessage = 'The imported config json is incorrect. Please check the JSON content'
          $alert({
            type: 'danger',
            content: errorMessage
          });
          defer.reject(errorMessage);
          return;
        }

        if (result.template !== type) {
          errorMessage = 'Template imported is for ' + result.template + '. Please switch to ' + result.template + ' creation to import.';
          $alert({
            type: 'danger',
            content: errorMessage
          });
          defer.reject(errorMessage);
          return;
        }
        // We need to perform more validations on the uploaded json.
        if (!result.config.source ||
            !result.config.sink ||
            !result.config.transforms) {
          errorMessage = 'The structure of imported config is incorrect. To the base structure of the config please try creating a new adpater and viewing the config.';
          $alert({
            type: 'danger',
            content: errorMessage
          });
          defer.reject(errorMessage);
          return;
        }
        defer.resolve(result);
      };

      reader.onerror = function (evt) {
        defer.reject(evt);
      };
      return defer.promise;
    }

    return {
      getNodes: getNodes,
      extractMetadataFromDraft: extractMetadataFromDraft,
      getConnectionsBasedOnNodes: getConnectionsBasedOnNodes,
      exportAdapter: exportAdapter,
      importAdapter: importAdapter
    }
  });
