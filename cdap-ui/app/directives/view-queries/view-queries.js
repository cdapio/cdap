angular.module(PKG.name + '.commons')
  .directive('myViewQueries', function () {

    return {
      restrict: 'E',
      scope: {
        panel: '='
      },
      templateUrl: 'view-queries/view-queries.html',
      controller: function ($scope, MyDataSource, $state, EventPipe, myExploreApi, $http, myCdapUrl) {
        $scope.downloading = {};

        var dataSrc = new MyDataSource($scope);
        $scope.queries = [];
        var params = {
          namespace: $state.params.namespace,
          scope: $scope
        };

        $scope.getQueries = function() {

          myExploreApi.getQueries(params)
            .$promise
            .then(function (queries) {

              $scope.queries = queries;

              // Polling for status
              angular.forEach($scope.queries, function(q) {
                q.isOpen = false;
                if (q.status !== 'FINISHED') {

                  // TODO: change to use myExploreApi once figure out how to manually stop poll with $resource
                  var promise = dataSrc.poll({
                    _cdapPath: '/data/explore/queries/' +
                                q.query_handle + '/status',
                    interval: 1000
                  }, function(res) {
                    q.status = res.status;

                    if (res.status === 'FINISHED') {
                      dataSrc.stopPoll(q.pollid);
                    }
                  });
                  q.pollid = promise.__pollId__;
                }
              });

            });
        };

        EventPipe.on('explore.newQuery', $scope.getQueries);

        $scope.getQueries();


        $scope.responses = {};

        $scope.fetchResult = function(query) {
          if (query.status !== 'FINISHED') {
            return;
          }

          // Close other accordion
          angular.forEach($scope.queries, function(q) {
            q.isOpen = false;
          });

          query.isOpen = !query.isOpen;

          if (query.isOpen) {
            $scope.responses.request = query;

            var queryParams = {
              queryhandle: query.query_handle,
              scope: $scope
            };

            myExploreApi.getQuerySchema(queryParams)
              .$promise
              .then(function (result) {
                angular.forEach(result, function(v) {
                  v.name = v.name.split('.')[1];
                });

                $scope.responses.schema = result;
              });

            myExploreApi.getQueryPreview(queryParams, {},
              function (result) {
                $scope.responses.results = result;
              });
          }

        };

        $scope.download = function(query) {
          $scope.downloading[query] = true;

          // Cannot use $resource: http://stackoverflow.com/questions/24876593/resource-query-return-split-strings-array-of-char-instead-of-a-string

          // The files are being store in the node proxy

          $http.post('/downloadQuery', {
            'backendUrl': myCdapUrl.constructUrl({_cdapPath: '/data/explore/queries/' + query.query_handle + '/download'}),
            'queryHandle': query.query_handle
          })
            .success(function(res) {

              var url = 'http://' + window.location.host + res;

              var element = angular.element('<a/>');
              element.attr({
                href: url,
                target: '_self'
              })[0].click();

              $scope.downloading[query] = false;
            })
            .error(function() {
              console.info('Error downloading query');
              $scope.downloading[query] = false;
            });

        };

      }

    };

  });
