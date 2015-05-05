angular.module(PKG.name+'.feature.datasets')
  .factory('QueryModel', function ($q, mySettings) {

      function QueryModel(dataSrc, key, data) {
        var self = this;
        this.dataSrc = dataSrc;
        this.key = key;
        this.queryHandles = {};
        this.data = data || [];
        if (this.data.length) {
          this.sync();
        }
      }

      QueryModel.prototype.init = function () {
        var self = this;
        var deferred = $q.defer();
        mySettings.get(this.key).then(function (resp) {
          self.data = resp;
          resp.forEach(function (entry) {
            self.queryHandles[entry.query_handle] = true;
          });
          deferred.resolve();
        }, function (err) {
          deferred.reject(err);
        });
        return deferred.promise;
      };

      QueryModel.prototype.add = function (entry) {
        if(this.data.indexOf(entry) === -1) {
          this.data.push(entry);
          this.sync();
        }
      };

      QueryModel.prototype.addHandle = function (queryHandle) {
        this.queryHandles[queryHandle] = true;
      };

      QueryModel.prototype.set = function (entries) {
        if(angular.isArray(entries)) {
          this.data = entries;
          this.sync();
        }
      };

      QueryModel.prototype.get = function () {
        var self = this;
        var deferred = $q.defer();
        var requests = [];
        requests.push(mySettings.get(this.key));
        requests.push(this.dataSrc.request({
          _cdapNsPath: '/data/explore/queries',
          })
        );
        $q.all(requests).then(function (responses) {
          self.data = self.mergeQueries(responses[0], responses[1]);
          deferred.resolve(self.data);
          self.sync();
        });
        return deferred.promise;
      };

      /**
       * Merges queries stored in user settings and queries pulled from server
       * using query_handle.
       */
      QueryModel.prototype.mergeQueries = function (userQueries, apiQueries) {
        var queryMap = {};
        if (userQueries) {
          userQueries.forEach(function (query) {
            queryMap[query.query_handle] = query;
          });
        }
        if (apiQueries) {
          apiQueries.forEach(function (query) {
            queryMap[query.query_handle] = query;
          });
        }
        var queries = [];
        for (var handle in queryMap) {
          if (this.queryHandles.hasOwnProperty(handle)) {
            var query = queryMap[handle];
            queries.push(query);
          }
        }
        return queries;
      };

      QueryModel.prototype.clear = function () {
        this.data = [];
        this.sync();
      };

      QueryModel.prototype.sync = function () {
        mySettings.set(this.key, this.data);
      };

      return QueryModel;

  });
