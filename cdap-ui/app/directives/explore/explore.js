angular.module(PKG.name + '.commons')
  .directive('myExplore', function () {

    return {
      restrict: 'E',
      scope: {
        type: '=',
        name: '='
      },
      templateUrl: 'explore/explore.html',
      controller: myExploreCtrl
    };


    function myExploreCtrl ($scope, MyDataSource, myExploreApi, $http, $state, $bootstrapModal, myCdapUrl) {
        var dataSrc = new MyDataSource($scope);
        $scope.queries = [];
        var params = {
          namespace: $state.params.namespace,
          scope: $scope
        };

        $scope.$watch('name', function() {
          $scope.query = 'SELECT * FROM ' + $scope.type + '_' + $scope.name + ' LIMIT 5';
        });

        $scope.execute = function() {
          myExploreApi.postQuery(params, { query: $scope.query }, $scope.getQueries);
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

        $scope.getQueries();


        $scope.preview = function (query) {
          $bootstrapModal.open({
            templateUrl: 'explore/preview-modal.html',
            size: 'lg',
            resolve: {
              query: function () { return query; }
            },
            controller: ['$scope', 'myExploreApi', function ($scope, myExploreApi) {
              var params = {
                queryhandle: query.query_handle,
                scope: $scope
              };

              myExploreApi.getQuerySchema(params)
                .$promise
                .then(function (res) {
                  angular.forEach(res, function(v) {
                    v.name = v.name.split('.')[1];
                  });

                  $scope.schema = res;
                });

              myExploreApi.getQueryPreview(params, {},
                function (res) {
                  $scope.rows = res;
                });

            }]
          });
        };


        $scope.download = function(query) {
          query.downloading = true;
          query.is_active = false; // this will prevent user from previewing after download

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

              query.downloading = false;
            })
            .error(function() {
              console.info('Error downloading query');
              query.downloading = false;
            });
        };

        $scope.clone = function (query) {
          myExploreApi.postQuery(params, { query: query.statement }, $scope.getQueries);
        };

      }


  });
