angular.module(PKG.name + '.feature.adapters')
  .factory('AdapterErrorFactory', function () {

    function isModelValid (nodes, connections, metadata, config) {
      var validationRules = [
        hasExactlyOneSourceAndSink,
        hasNameAndTemplateType,
        checkForRequiredField,
        checkForUnconnectedNodes,
        checkForParallelDAGs
      ];
      var errors = {};
      validationRules.forEach(function(rule) {
        rule.call(this, nodes, connections, metadata, config, errors);
      });

      if (Object.keys(errors).length === 0) {
        return true;
      } else {
        return errors;
      }

    }

    function addCanvasError (error, errors) {
      if (!errors.canvas) {
        errors.canvas = [error];
      } else {
        errors.canvas.push(error);
      }
    }

    function hasExactlyOneSourceAndSink(nodes, connections, metadata, config, errors) {
      var source = [], sink = [];

      angular.forEach(nodes, function (value, key) {
        switch (value.type) {
          case 'sink':
            sink.push(key);
            break;
          case 'source':
            source.push(key);
            break;
        }
      });

      if (source.length !== 1) {
        if (source.length < 1) {
          errors.source = 'Adapter is missing a source';
        } else if (source.length > 1) {
          angular.forEach(source, function (node) {
            errors[node] = 'Adapter should only have 1 source';
          });

          addCanvasError('Adapter should only have 1 source', errors);

        }
      }

      if (sink.length !== 1) {
        if (sink.length < 1) {
          errors.sink = 'Adapter is missing a sink';
        } else if (sink.length > 1) {
          angular.forEach(sink, function (node) {
            errors[node] = 'Adapter should only have 1 sink';
          });

          addCanvasError('Adapter should only have 1 sink', errors);

        }
      }

    }

    function hasNameAndTemplateType(nodes, connections, metadata, config, errors) {
      var name = metadata.name;
      if (!name.length) {
        errors.name = 'Adapter needs to have a name';
        metadata.error = 'Enter adapter name';
      }

      // Should probably add template type check here. Waiting for design.
    }

    function checkForRequiredField(nodes, connections, metadata, config, errors) {

      if(config.source.name && !isValidPlugin(config.source)) {
        errors[config.source.id] = 'Adapter\'s source is missing required fields';
      }
      if (config.sink.name && !isValidPlugin(config.sink)) {
        errors[config.sink.id] = 'Adapter\'s sink is missing required fields';
      }
      config.transforms.forEach(function(transform) {
        console.log('check', isValidPlugin(transform));
        if (transform.name && !isValidPlugin(transform)) {
          errors[transform.id] = 'Adapter\'s transforms is missing required fields';
        }
      });

    }
    function isValidPlugin(plugin) {
      var i;
      var keys = Object.keys(plugin.properties);
      if (!keys.length) {
        plugin.valid = false;
        return plugin.valid;
      }
      plugin.valid = true;
      for (i=0; i< keys.length; i++) {
        var property = plugin.properties[keys[i]];
        if (plugin._backendProperties[keys[i]].required && (!property || property === '')) {
          plugin.valid = false;
          break;
        }
      }
      return plugin.valid;
    }

    function checkForUnconnectedNodes(nodes, connections, metadata, config, errors) {

      var nodesCopy = angular.copy(nodes);

      angular.forEach(connections, function (conn) {
        nodesCopy[conn.source].visited = true;
        nodesCopy[conn.target].visited = true;
      });

      var unattached = [];

      angular.forEach(nodesCopy, function (value, key) {
        if (!value.visited) {
          unattached.push(key);
          errors[key] = 'Node is not connected to any other node';
        }
      });

    }

    function checkForParallelDAGs(nodes, connections, metadata, config, errors) {
      var i,
          currConn,
          nextConn;
      for(i=0; i<connections.length-1; i++) {
        currConn = connections[i];
        nextConn = connections[i+1];
        if (currConn.target !== nextConn.source) {
          addCanvasError('There are parallel connections inside this adapter', errors);
          break;
        }
      }
    }

    return {
      isModelValid: isModelValid
    };

  });
