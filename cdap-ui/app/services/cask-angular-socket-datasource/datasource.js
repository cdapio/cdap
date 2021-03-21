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
      var CDAP_API_VERSION = 'v3';
      // FIXME (CDAP-14836): Right now this is scattered across node and client. Need to consolidate this.
      const REQUEST_ORIGIN_ROUTER = 'ROUTER';

      var instances = {}; // keyed by scopeid


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

          if (data.statusCode>299 || data.warning) {
            if (self.bindings[hash]) {
              if (self.bindings[hash].errorCallback) {
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
            } else {
              if (self.bindings[hash] && self.bindings[hash].type === 'POLL') {
                self.bindings[hash].resource.interval = startClientPoll(hash, self.bindings, self.bindings[hash].resource.intervalTime);
              }
            }
          }
          return;
        });

        EventPipe.on(MYSOCKET_EVENT.reconnected, () => {
          Object.keys(this.bindings).forEach((reqId) => {
            const req = self.bindings[reqId];

            if (req.poll) {
              pausePoll(self.bindings);
            }
            mySocket.send({
              action: 'request',
              resource: req.resource,
            });
          });
        });

        EventPipe.on(MYSOCKET_EVENT.closed, () => {
          pausePoll(self.bindings);
        });

        scope.$on('$destroy', function () {
          Object.keys(self.bindings).forEach(function(key) {
            var b = self.bindings[key];
            if (b.poll) {
              stopPoll(self.bindings, b.resource.id);
            }
          });

          delete instances[self.scopeId];
        });

        scope.$on(caskWindowManager.event.blur, function () {
          pausePoll(self.bindings);
        });

        scope.$on(caskWindowManager.event.focus, function () {
          resumePoll(self.bindings);
        });

      }

      function startClientPoll(resourceId, bindings, interval) {
        const intervalTimer = setTimeout(() => {
          const resource = bindings[resourceId]? bindings[resourceId].resource : undefined;
          if (!resource) {
            clearTimeout(intervalTimer);
            return;
          }
          mySocket.send({
            action: 'request',
            resource
          });
        }, interval);
        return intervalTimer;
      }

      function stopPoll(bindings, resourceId) {
        let id;
        if (typeof resourceId === 'object' && resourceId !== null) {
          id = resourceId.params.pollId;
        } else {
          id = resourceId;
        }

        if (bindings[id]) {
          clearTimeout(bindings[id].resource.interval);
          delete bindings[id];
        }
      }

      function pausePoll(bindings) {
        Object.keys(bindings)
          .filter(resourceId => bindings[resourceId].type === 'POLL')
          .forEach(resourceId => {
            clearTimeout(bindings[resourceId].resource.interval);
          });
      }

      function resumePoll(bindings) {
        Object.keys(bindings)
          .filter(resourceId => bindings[resourceId].type === 'POLL')
          .forEach(resourceId => {
            bindings[resourceId].resource.interval = startClientPoll(resourceId, bindings, bindings[resourceId].resource);
          });
      }

      /**
       * Start polling of a resource when in scope.
       */
      DataSource.prototype.poll = function (resource, cb, errorCb) {
        var self = this;
        var generatedResource = {};
        const intervalTime = resource.interval || (resource.options &&  resource.options.interval) || $rootScope.defaultPollInterval;
        var promise = new MyPromise(function(resolve, reject) {
          const resourceId = uuid.v4();
          generatedResource = {
            id: resourceId,
            json: resource.json,
            intervalTime,
            interval: startClientPoll(resourceId, self.bindings, intervalTime),
            body: resource.body,
            method: resource.method || 'GET',
            suppressErrors: resource.suppressErrors || false
          };

          if (resource.headers) {
            generatedResource.headers = resource.headers;
          }

          let apiVersion = resource.apiVersion || CDAP_API_VERSION;
          if (!resource.requestOrigin || resource.requestOrigin === REQUEST_ORIGIN_ROUTER) {
            resource.url = `/${apiVersion}${resource.url}`;
          }

          if (resource.requestOrigin) {
            generatedResource.requestOrigin = resource.requestOrigin;
          } else {
            generatedResource.requestOrigin = REQUEST_ORIGIN_ROUTER;
          }

          generatedResource.url = buildUrl(resource.url, resource.params || {});
          self.bindings[generatedResource.id] = {
            poll: true,
            type: 'POLL',
            callback: cb,
            resource: generatedResource,
            errorCallback: errorCb,
            resolve: resolve,
            reject: reject
          };

          mySocket.send({
            action: 'request',
            resource: generatedResource
          });
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
          stopPoll(this.bindings, resourceId);
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

          let apiVersion = resource.apiVersion || CDAP_API_VERSION;
          if (!resource.requestOrigin || resource.requestOrigin === REQUEST_ORIGIN_ROUTER) {
            resource.url = `/${apiVersion}${resource.url}`;
          }
          if (resource.requestOrigin) {
            generatedResource.requestOrigin = resource.requestOrigin;
          } else {
            generatedResource.requestOrigin = REQUEST_ORIGIN_ROUTER;
          }
          generatedResource.url = buildUrl(resource.url, resource.params || {});
          generatedResource.id = uuid.v4();
          self.bindings[generatedResource.id] = {
            type: 'REQUEST',
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
