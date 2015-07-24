angular.module(PKG.name + '.feature.adapters')
  .factory('CanvasFactory', function(myHelpers) {
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

    return {
      getNodes: getNodes,
      extractMetadataFromDraft: extractMetadataFromDraft,
      getConnectionsBasedOnNodes: getConnectionsBasedOnNodes
    }
  });
