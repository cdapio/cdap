angular.module(PKG.name+'.services')

  /**
    Example Usage:

    MyDataSource // usage in a controller:

    var dataSrc = new MyDataSource($scope);

    // polling a namespaced resource example:
    dataSrc.poll({
        method: 'GET',
        _cdapNsPath: '/foo/bar',
        interval: 5000 // in milliseconds.
      },
      function(result) {
        $scope.foo = result;
      }
    ); // will poll <host>:<port>/v3/namespaces/<currentNamespace>/foo/bar

    // posting to a systemwide resource:
    dataSrc.request({
        method: 'POST',
        _cdapPath: '/system/config',
        body: {
          foo: 'bar'
        }
      },
      function(result) {
        $scope.foo = result;
      }
    ); // will post to <host>:<port>/v3/system/config

   */

  .factory('MyDataSource', function ($log, $rootScope, caskWindowManager, mySocket, MYSOCKET_EVENT, $q, $filter, myCdapUrl, MyPromise, myHelpers) {

    var instances = {}; // keyed by scopeid

    /**
     * Generates unique id's for each request that is being sent on
     * the websocket connection.
     */
    var generateUUID = function() {
      return uuid.v4();
    };

    /**
     * Start polling of the resource - sends the action 'poll-start' to
     * the node backend.
     */
    function _pollStart (resource) {
      var re = {};
      if (!resource.url) {
        re = resource;
      }else {
        re = {
          url: resource.url,
          id: resource.id,
          json: resource.json,
          method: resource.method
        };
        if (resource.interval) {
          re.interval = resource.interval;
        }
        if (resource.body) {
          re.body = resource.body;
        }
      }

      // FIXME: There is a circular dependency and that is why
      // myAuth.isAuthenticated is not used. There should be a better way to do this.
      if ($rootScope.currentUser && $rootScope.currentUser.token) {
        re.headers = {
          Authorization: 'Bearer '+ $rootScope.currentUser.token
        };
      }

      mySocket.send({
        action: 'poll-start',
        resource: re
      });
    }

    /**
     * Stops polling of the resource - sends the actions 'poll-stop' to
     * the node backend.
     */
    function _pollStop (resource) {
      var re = {};
      if (!resource.url) {
        re = resource;
      }else {
        re = {
          url: resource.url,
          id: resource.id,
          json: resource.json,
          method: resource.method
        };
      }
      if ($rootScope.currentUser && $rootScope.currentUser.token) {
        re.headers = {
          Authorization: 'Bearer '+ $rootScope.currentUser.token
        };
      }

      mySocket.send({
        action: 'poll-stop',
        resource: re
      });
    }


    function DataSource (scope) {
      scope = scope || $rootScope.$new();

      var id = scope.$id,
          self = this;

      if(instances[id]) {
        // Reuse the same instance if already created.
        return instances[id];
      }

      if (!(this instanceof DataSource)) {
        return new DataSource(scope);
      }
      instances[id] = self;

      this.bindings = [];



      scope.$on(MYSOCKET_EVENT.message, function (event, data) {

        if(data.statusCode>299 || data.warning) {
          angular.forEach(self.bindings, function (b) {
            if (b.resource.id === data.resource.id) {
              if (b.errorCallback) {
                $rootScope.$apply(b.errorCallback.bind(null, data.response));
              } else if (b.reject) {
                $rootScope.$apply(b.reject.bind(null, {data: data.response}));
              }
            }
          });
          return; // errors are handled at $rootScope level
        }
        // Not using angular.forEach for performance reasons.
        for (var i=0; i<self.bindings.length; i++) {
          var b = self.bindings[i];
          if (b.resource.id === data.resource.id) {
            if (angular.isFunction(b.callback)) {
              data.response = data.response || {};
              data.response.__pollId__ = b.resource.id;
              scope.$apply(b.callback.bind(null, data.response));
            } else if (b && b.resolve) {
              // https://github.com/angular/angular.js/wiki/When-to-use-$scope.$apply%28%29
              scope.$apply(b.resolve.bind(null, {data: data.response, id: b.resource.id}));
              return;
            }
          }
        }

      });

      scope.$on('$destroy', function () {
        setTimeout(function() {
          for (var i=0; i<self.bindings.length; i++) {
            var b = self.bindings[i];
            if (b.poll) {
              _pollStop(b.resource);
            }
          }

          delete instances[id];
        });
      });

      scope.$on(caskWindowManager.event.blur, function () {
        angular.forEach(self.bindings, function (b) {
          if(b.poll) {
            _pollStop(b.resource);
          }
        });
      });

      scope.$on(caskWindowManager.event.focus, function () {
        angular.forEach(self.bindings, function (b) {
          if(b.poll) {
            _pollStart(b.resource);
          }
        });
      });

      this.scope = scope;
    }

    /**
     * Start polling of a resource when in scope.
     */
    DataSource.prototype.poll = function (resource, cb, errorCb) {
      var self = this;
      var id = generateUUID();
      var generatedResource = {};
      var promise = new MyPromise(function(resolve, reject) {
        generatedResource = {
          json: resource.json,
          id: id,
          interval: resource.interval || myHelpers.objectQuery(resource, 'options', 'interval') ,
          body: resource.body,
          method: resource.method || 'GET'
        };

        if (!resource.url) {
          generatedResource.url = buildUrl(myCdapUrl.constructUrl(resource), resource.params || {});
        } else {
          generatedResource.url = buildUrl(resource.url, resource.params || {});
        }

        self.bindings.push({
          poll: true,
          resource: generatedResource,
          callback: cb,
          errorCallback: errorCb,
          resolve: resolve,
          reject: reject
        });

        _pollStart(generatedResource);
      }, true);

      if (!resource.$isResource) {
        promise = promise.then(function(res) {
          var id = res.id;
          res = res.data;
          res.__pollId__ = id;
          return $q.when(res);
        });
      }
      promise.__pollId__ = id;
      return promise;
    };

    /**
     * Stop polling of a resource when requested.
     * (when scope is destroyed Line 196 takes care of deleting the polling resource)
     */
    DataSource.prototype.stopPoll = function(resourceId) {
      var filterFilter = $filter('filter');
      // Duck Typing for angular's $resource.
      var defer = $q.defer();
      var id;
      if (angular.isObject(resourceId)) {
        id = resourceId.params.pollId;
      } else {
        id = resourceId;
      }
      var match = filterFilter(this.bindings, { 'resource': {id: id} });

      if (match.length) {
        _pollStop(match[0].resource);
        this.bindings.splice(this.bindings.indexOf(match[0]), 1);
        defer.resolve({});
      } else {
        defer.reject({});
      }
      return defer.promise;
    };

    /**
     * Fetch a template configuration on-demand. Send the action
     * 'template-config' to the node backend.
     */
    DataSource.prototype.config = function (resource, cb) {
      var deferred = $q.defer();

      var id = generateUUID();
      resource.id = id;
      this.bindings.push({
        resource: resource,
        id: id,
        callback: function (result) {
          if (cb) {
            cb.apply(this, arguments);
          }
          deferred.resolve(result);
        },
        errorCallback: function(err) {
          deferred.reject(err);
        }
      });
      mySocket.send({
        action: 'template-config',
        resource: resource
      });
      return deferred.promise;
    };

    /**
     * Fetch a resource on-demand. Send the action 'request' to
     * the node backend.
     */
    DataSource.prototype.request = function (resource, cb) {
      var self = this;

      var promise = new MyPromise(function(resolve, reject) {
        var id = generateUUID();

        var generatedResource = {
          id: id,
          json: resource.json,
          method: resource.method
        };
        if (resource.body) {
          generatedResource.body = resource.body;
        }

        if (resource.data) {
          generatedResource.body = resource.data;
        }
        if ($rootScope.currentUser && $rootScope.currentUser.token) {
          generatedResource.headers = {
            Authorization: 'Bearer '+ $rootScope.currentUser.token
          };
        }

        if (!resource.url) {
          generatedResource.url = buildUrl(myCdapUrl.constructUrl(resource), resource.params || {});
        } else {
          generatedResource.url = buildUrl(resource.url, resource.params || {});
        }

        self.bindings.push({
          resource: generatedResource,
          callback: cb,
          resolve: resolve,
          reject: reject
        });

        mySocket.send({
          action: 'request',
          resource: generatedResource
        });
      }, false);

      if (!resource.$isResource) {
        promise = promise.then(function(res) {
          res = res.data;
          return $q.when(res);
        });
      }

      return promise;
    };

    return DataSource;
  });

// Lifted from $http as a helper method to parse '@params' in the url for $resource.
function buildUrl(url, params) {
  if (!params) {
    return url;
  }
  var parts = [];

  function forEachSorted(obj, iterator, context) {
    var keys = Object.keys(params).sort();
    for (var i = 0; i < keys.length; i++) {
      iterator.call(context, obj[keys[i]], keys[i]);
    }
    return keys;
  }

  function encodeUriQuery(val, pctEncodeSpaces) {
    return encodeURIComponent(val).
           replace(/%40/gi, '@').
           replace(/%3A/gi, ':').
           replace(/%24/g, '$').
           replace(/%2C/gi, ',').
           replace(/%3B/gi, ';').
           replace(/%20/g, (pctEncodeSpaces ? '%20' : '+'));
  }

  forEachSorted(params, function(value, key) {
    if (value === null || angular.isUndefined(value)) {
      return;
    }
    if (!angular.isArray(value)) {
      value = [value];
    }

    angular.forEach(value, function(v) {
      if (angular.isObject(v)) {
        if (angular.isDate(v)) {
          v = v.toISOString();
        } else {
          v = angular.toJson(v);
        }
      }
      parts.push(encodeUriQuery(key) + '=' + encodeUriQuery(v));
    });
  });
  if (parts.length > 0) {
    url += ((url.indexOf('?') === -1) ? '?' : '&') + parts.join('&');
  }
  return url;
}
