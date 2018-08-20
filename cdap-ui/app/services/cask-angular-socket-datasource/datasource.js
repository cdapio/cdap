/*
 * Copyright © 2015-2018 Cask Data, Inc.
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

var socketDataSource = angular.module(PKG.name+'.services');

  /**
    Example Usage:

    MyCDAPDataSource // usage in a controller:

    var dataSrc = new MyCDAPDataSource($scope);

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

  socketDataSource.factory('uuid', function ($window) {
    return $window.uuid;
  });


  socketDataSource.provider('MyDataSource', function () {

    this.defaultPollInterval = 10;

    this.$get = function($rootScope, caskWindowManager, mySocket, MYSOCKET_EVENT, $q, MyPromise, uuid, EventPipe) {

      var instances = {}; // keyed by scopeid

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
            id: resource.id,
            url: resource.url,
            json: resource.json,
            method: resource.method,
            suppressErrors: resource.suppressErrors || false
          };
          if (resource.interval) {
            re.interval = resource.interval;
          }
          if (resource.body) {
            re.body = resource.body;
          }
        }

        if (resource.headers) {
          re.headers = resource.headers;
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
            id: resource.id,
            url: resource.url,
            json: resource.json,
            method: resource.method,
            suppressErrors: resource.suppressErrors || false
          };
        }
        if (resource.header) {
          re.header = resource.header;
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

        this.scopeId = id;
        this.bindings = {};

        EventPipe.on(MYSOCKET_EVENT.message, function (data) {
          var hash;
          var isPoll;
          hash = data.resource.id;

          if(data.statusCode>299 || data.warning) {
            if (self.bindings[hash]) {
              if(self.bindings[hash].errorCallback) {
                $rootScope.$apply(self.bindings[hash].errorCallback.bind(null, data.error || data.response));
              } else if (self.bindings[hash].reject) {
                $rootScope.$apply(self.bindings[hash].reject.bind(null, {data: data.error || data.response, statusCode: data.statusCode }));
              }
            }
          } else if (self.bindings[hash]) {
            if (self.bindings[hash].callback) {
              data.response = data.response || {};
              data.response.__pollId__ = hash;
              scope.$apply(self.bindings[hash].callback.bind(null, data.response));
            } else if (self.bindings[hash].resolve) {
              // https://github.com/angular/angular.js/wiki/When-to-use-$scope.$apply%28%29
              scope.$apply(self.bindings[hash].resolve.bind(null, {data: data.response, id: hash, statusCode: data.statusCode }));
            }
            /*
              At first glance this condition check might be redundant with line 157,
              however in the resolve or callback function if the user initiates a stop-poll call then
              the execution goes to stopPoll function in line 264 and there we delete the entry from bindings
              as we no longer need it. After the stopPoll request has gone out the execution continues back
              here and we can do self.bindings[hash].poll as self.bindings[hash] is already deleted in stopPoll.
            */
            if (!self.bindings[hash]) {
              return;
            }
            isPoll = self.bindings[hash].poll;
            if (!isPoll) {
              // We can remove the entry from the self bindings if its not a poll.
              // Is not going to be used for anything else.
              delete self.bindings[hash];
            }
          }
          return;
        });

        scope.$on('$destroy', function () {
          Object.keys(self.bindings).forEach(function(key) {
            var b = self.bindings[key];
            if (b.poll) {
              _pollStop(b.resource);
            }
          });

          delete instances[self.scopeId];
        });

        scope.$on(caskWindowManager.event.blur, function () {
          Object.keys(self.bindings).forEach(function(key) {
            var b = self.bindings[key];
            if (b.poll) {
              _pollStop(b.resource);
            }
          });
        });

        scope.$on(caskWindowManager.event.focus, function () {
          Object.keys(self.bindings).forEach(function(key) {
            var b = self.bindings[key];
            if (b.poll) {
              _pollStart(b.resource);
            }
          });
        });

      }

      /**
       * Start polling of a resource when in scope.
       */
      DataSource.prototype.poll = function (resource, cb, errorCb) {
        var self = this;
        var generatedResource = {};
        var promise = new MyPromise(function(resolve, reject) {
          generatedResource = {
            json: resource.json,
            interval: resource.interval || (resource.options &&  resource.options.interval) || $rootScope.defaultPollInterval,
            body: resource.body,
            method: resource.method || 'GET',
            suppressErrors: resource.suppressErrors || false
          };

          if (resource.headers) {
            generatedResource.headers = resource.headers;
          }

          generatedResource.url = buildUrl(resource.url, resource.params || {});
          generatedResource.id = uuid.v4();
          self.bindings[generatedResource.id] = {
            poll: true,
            callback: cb,
            resource: generatedResource,
            errorCallback: errorCb,
            resolve: resolve,
            reject: reject
          };

          _pollStart(generatedResource);
        }, true);

        if (!resource.$isResource) {
          promise = promise.then(function(res) {
            res = res.data;
            res.__pollId__ = generatedResource.id;
            return $q.when(res);
          });
        }
        promise.__pollId__ = generatedResource.id;
        return promise;
      };

      /**
       * Stop polling of a resource when requested.
       * (when scope is destroyed Line 196 takes care of deleting the polling resource)
       */
      DataSource.prototype.stopPoll = function(resourceId) {
        // Duck Typing for angular's $resource.
        var defer = $q.defer();
        var id, resource;
        if (angular.isObject(resourceId)) {
          id = resourceId.params.pollId;
        } else {
          id = resourceId;
        }

        var match = this.bindings[resourceId];

        if (match) {
          resource = match.resource;
          _pollStop(resource);
          // We should probably be doing this once we get a confirmation from cdap node proxy server.
          // Deleting the entry from this.bindings is wrong here if the stop poll fails for some god forsaken reason.
          delete this.bindings[resourceId];
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
      DataSource.prototype.config = function (resource, cb, errorCb) {
        var deferred = $q.defer();

        resource.suppressErrors = true;
        resource.id = uuid.v4();
        this.bindings[resource.id] = {
          resource: resource,
          callback: function (result) {
            if (cb) {
              cb.apply(null, result);
            }
            deferred.resolve(result);
          },
          errorCallback: function(err) {
            if (errorCb) {
              errorCb.apply(null, err);
            }
            deferred.reject(err);
          }
        };

        mySocket.send({
          action: resource.actionName,
          resource: resource
        });
        return deferred.promise;
      };

      /**
       * Fetch a resource on-demand. Send the action 'request' to
       * the node backend.
       */
      DataSource.prototype.request = function (resource, cb, errorCb) {
        var self = this;
        var promise = new MyPromise(function(resolve, reject) {

          var generatedResource = {
            json: resource.json,
            method: resource.method || 'GET',
            suppressErrors: resource.suppressErrors || false
          };
          if (resource.body) {
            generatedResource.body = resource.body;
          }

          if (resource.data) {
            generatedResource.body = resource.data;
          }

          if (resource.headers) {
            generatedResource.headers = resource.headers;
          }
          if (resource.contentType) {
            generatedResource.headers['Content-Type'] = resource.contentType;
          }

          generatedResource.url = buildUrl(resource.url, resource.params || {});
          generatedResource.id = uuid.v4();
          self.bindings[generatedResource.id] = {
            callback: cb,
            errorCallback: errorCb,
            resource: generatedResource,
            resolve: resolve,
            reject: reject
          };

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

    };


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
