angular.module(PKG.name+'.feature.datasets')
  .factory('QueryModel', function ($q, mySettings) {

      function QueryModel(dataSrc, key, data) {
        this.dataSrc = dataSrc;
        this.key = key;
        this.data = data || [];
      }

      QueryModel.prototype.add = function (entry) {
        if(this.data.indexOf(entry) == -1) {
          this.data.push(entry);
          this.sync();
        }
      };

      QueryModel.prototype.set = function (entries) {
        if(angular.isArray(entries)) {
          this.data = entries;
          this.sync();
        }
      }

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
          var query = queryMap[handle];
          queries.push(query);
        }
        return queries;
      };

      QueryModel.prototype.remove = function (entry) {
        var index = this.data.indexOf(entry);
        if(index != -1) {
          this.data.splice(index, 1);
          this.sync();
        }
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
