angular.module(PKG.name + '.feature.services')
  .controller('StatusMakeRequestController', function($scope, $state, MyDataSource) {
    $scope.programId = $state.params.programId;
    $scope.requestUrl = $state.params.requestUrl;
    $scope.requestMethod = $state.params.requestMethod;
    $scope.urlParams = [];
    $scope.queryParams = [];

    var pattern = /\{([\s\S]*?)\}/g,
        dataSrc = new MyDataSource($scope);

    $scope.requestUrl.split('?')
      .forEach(function(item, index) {
        if (index === 0) {
          // url params
          item.split('/')
            .forEach(function(item) {
              if (item.length === 0) { return;}
              $scope.urlParams.push({
                /* If the url param matches the pattern {param1}
                    then add that as key,
                    otherwise mark key as null

                    (/count/{word} vs /count/stats/)
                    In the former case '{word}' will be added as a key and
                    will be recognised in the template to be replaced with
                    usertyped value where as in the latter example null will
                    be added to key and will be just displayed in the template.
                 */
                key: ( angular.isArray(item.match(pattern)) ? item.match(pattern): null),
                value: item
              });
            });
        } else {
          // query params
          item.split('&')
            .forEach(function(item) {
              var pat;
              if (item.length === 0) { return;}
              pat = item.match(pattern);
              // Could be dynamic or static query params.
              // 1. /count/{word}?filter={filterType} vs
              // 2. /count/{word}?filter={filterType}&aggregate=10
              // In the latter case we still need to include the query
              // param 'aggregate' to be a static value
              if (angular.isArray(pat)) {
                $scope.queryParams.push({
                  key: pat[0].substr(1, (pat[0].length - 2)),
                  value: pat[0]
                });
              } else {
                $scope.queryParams.push({
                  key: item.split('=')[0],
                  value: item.split('=')[1]
                });
              }
            });
        }
      });

      $scope.makeRequest = function() {
        var compiledUrl = '/apps/' +
          $state.params.appId + '/services/' +
          $state.params.programId + '/methods';

        angular.forEach($scope.urlParams, function(param) {
          compiledUrl = compiledUrl + '/' + param.value;
        });

        angular.forEach($scope.queryParams, function(param, index) {
          compiledUrl += (index === 0 ? '?': '=') +
                          param.key + '=' + param.value;
        });

        dataSrc.request({
          _cdapNsPath: compiledUrl,
          method: $scope.requestMethod.toUpperCase()
        })
          .then(function(res) {
            $scope.response = res;
          });
      };


  });
