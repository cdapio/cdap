angular.module(PKG.name + '.feature.adapters')
  .factory('AdapterErrorFactory', function () {

    function isModelValid (nodes, connections, metadata, config) {
      var validationRules = [
        hasExactlyOneSourceAndSink,
        hasNameAndTemplateType,
        checkForRequiredField,
        checkForUnconnectedNodes
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
          errors.source = 'Application is missing a source';
        } else if (source.length > 1) {
          angular.forEach(source, function (node) {
            errors[node] = 'Application should only have 1 source';
          });

          addCanvasError('Application should only have 1 source', errors);

        }
      }

      if (sink.length !== 1) {
        if (sink.length < 1) {
          errors.sink = 'Application is missing a sink';
        } else if (sink.length > 1) {
          angular.forEach(sink, function (node) {
            errors[node] = 'Application should only have 1 sink';
          });

          addCanvasError('Application should only have 1 sink', errors);

        }
      }

    }

    function hasNameAndTemplateType(nodes, connections, metadata, config, errors) {
      var name = metadata.name;
      if (typeof name !== 'string' || !name.length) {
        errors.name = 'Application needs to have a name';
        metadata.error = 'Enter application name';
        return;
      }

      var pattern = /^[\w]+$/;

      if (!pattern.test(name)) {
        errors.name = 'Application name can only have alphabets, numbers, and \'_\'';
        metadata.error = 'Application name can only have alphabets, numbers, and \'_\'';
      }

      // Should probably add template type check here. Waiting for design.
    }

    function checkForRequiredField(nodes, connections, metadata, config, errors) {

      if(config.source.name && !isValidPlugin(config.source)) {
        errors[config.source.id] = 'Source is missing required fields';
      }
      if (config.sink.name && !isValidPlugin(config.sink)) {
        errors[config.sink.id] = 'Sink is missing required fields';
      }
      config.transforms.forEach(function(transform) {
        if (transform.name && !isValidPlugin(transform)) {
          errors[transform.id] = 'Transform is missing required fields';
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
        if (plugin._backendProperties[keys[i]] && plugin._backendProperties[keys[i]].required && (!property || property === '')) {
          plugin.valid = false;
          break;
        }
      }
      return plugin.valid;
    }


    /*
      This checks for unconnected nodes and for parallel connections.
      1. It will traverse the graph starting with the source, and should end in a sink.
      2. If it does start with source && end with sink, check that all connections were traversed
    */
    function checkForUnconnectedNodes(nodes, connections, metadata, config, errors) {

      var nodesCopy = angular.copy(nodes);

      // at this point in the checking, I can assume that there is only 1 source and 1 sink
      var source,
          sink,
          transforms = [];

      angular.forEach(nodes, function (value, key) {
        switch (value.type) {
          case 'source':
            source = key;
            break;
          case 'sink':
            sink = key;
            break;
          case 'transform':
            transforms.push(key);
            break;
        }
      });

      if (!source || !sink) {
        return;
      }

      var connectionHash = {};
      var branch = false;

      angular.forEach(connections, function (conn) {
        nodesCopy[conn.source].visited = true;
        nodesCopy[conn.target].visited = true;

        if (!connectionHash[conn.source]) {
          connectionHash[conn.source] = {
            target: conn.target,
            visited: false
          };
        } else {
          branch = true;
        }
      });

      if (branch) {
        addCanvasError('Branching in this application is not supported', errors);
        return;
      }


      var unattached = [];
      angular.forEach(nodesCopy, function (value, key) {
        if (!value.visited) {
          unattached.push(key);
          errors[key] = value.name + ' ' + value.type + ' is not connected to any other node';
        }
      });

      if (unattached.length > 0) {
        addCanvasError('There are unconnected nodes in this application', errors);
        return;
      }

      var currNode = source;
      while (currNode !== sink) {
        if (connectionHash[currNode]) {
          if (connectionHash[currNode].visited) {
            addCanvasError('There is circular connection in this application', errors);
            return;
          }

          connectionHash[currNode].visited = true;
          currNode = connectionHash[currNode].target;
        } else {
          addCanvasError('This application connections do not end in a sink', errors);
          return;
        }
      }

      var connKeys = Object.keys(connectionHash);
      for (var i = 0; i < connKeys.length; i++) {
        if (!connectionHash[connKeys[i]].visited) {
          addCanvasError('There are parallel connections inside this application', errors);
          break;
        }
      }

    }

    return {
      isModelValid: isModelValid,
      hasNameAndTemplateType: hasNameAndTemplateType
    };

  });
