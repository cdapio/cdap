angular.module(PKG.name + '.feature.adapters')
  .factory('AdapterErrorFactory', function () {
    var nameError = false,
        sinkError = false,
        sourceError = false,
        canvasError = [],
        nodesError = {};

    // clearing out previous errors
    function initialize() {
      nameError = false;
      sinkError = false;
      sourceError = false;
      canvasError = [];
      nodesError = {};
    }

    function processError(errors) {
      initialize();

      var nodes = {};

      angular.forEach(errors, function (error) {
        if (error.type === 'name') {
          nameError = true;
        } else if (error.type === 'sink') {
          sinkError = true;
        } else if (error.type === 'source') {
          sourceError = true;
        } else if (error.type === 'canvas') {
          canvasError.push(error);
        } else if (error.type === 'nodes') {
          if (angular.isArray(error.unattached)) {
            angular.forEach(error.unattached, function (node) {
              if (!nodes[node]) {
                nodes[node] = [error.message];
              } else {
                nodes[node].push(error.message);
              }
            });
          } else {
            if (!nodes[error.node]) {
              nodes[error.node] = [error.message];
            } else {
              nodes[error.node].push(error.message);
            }
          }
        }
      });

      angular.forEach(nodes, function (value, key) {
        var htmlString = '<ul>';
        angular.forEach(value, function (error) {
          htmlString += '<li>' + error + '</li>';
        });
        htmlString+= '</ul>';

        nodesError[key] = htmlString;
      });
    }

    function getNameError() {
      return nameError;
    }
    function getSinkError() {
      return sinkError;
    }
    function getSourceError() {
      return sourceError;
    }
    function getCanvasError() {
      return canvasError;
    }
    function getNodesError() {
      return nodesError;
    }


    return {
      nameError: getNameError,
      sinkError: getSinkError,
      sourceError: getSourceError,
      canvasError: getCanvasError,
      nodesError: getNodesError,
      processError: processError
    };

  });
