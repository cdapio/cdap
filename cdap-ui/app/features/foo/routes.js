angular.module(PKG.name+'.feature.foo')
  .config(function ($stateProvider, MYAUTH_ROLE) {


    /**
     * State Configurations
     */
    $stateProvider

      .state('foo', {
        url: '/foo',
        templateUrl: '/assets/features/foo/foo.html'
      })
      .state('test-plumb', {
        url: '/foo/plumb',
        controller: 'PlumbController',
        controllerAs: 'PlumbController',
        templateUrl: '/assets/features/foo/plumb/plumb.html'
      })
      .state('test-edwin', {
        url: '/test/edwin',
        templateUrl: '/assets/features/foo/edwin.html',
        controller: function ($scope, $timeout) {
          $scope.settings = {
            size: {
              height: 100,
              width: 280
            },
            chartMetadata: {
              showx: true,
              showy: true,
              legend: {
                show: true,
                position: 'inset'
              }
            },
            color: {
              pattern: ['red', '#f4b400']
            },
            isLive: true,
            interval: 60*1000,
            aggregate: 5
          };

          $scope.data = {
            columns: [],
            keys: 'x',
            x: 'x',
            type: 'line'
          };

          var latestTime = 1435686300;

          function generateData() {
            var columns = [];

            columns.push(['x']);

            for (var i = 0; i < 5; i++) {
              columns[0].push(latestTime + 300);
              latestTime += 300;
            }

            columns.push(['data1']);
            columns.push(['data2']);
            for (var i = 0; i<5; i++) {
              columns[1].push(Math.floor(Math.random() * 101));
              columns[2].push(Math.floor(Math.random() * 101));
            }

            $scope.data.columns = columns;

            $timeout(function() {
              generateData();
            }, 3000);
          }

          generateData();

        }
      })

      .state('test-settings', {
        url: '/test/settings',
        templateUrl: '/assets/features/foo/settings.html',
        controller: 'FooPlaygroundController'
      });

  });
