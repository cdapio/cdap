angular.module(PKG.name + '.feature.explore')
  .controller('GlobalExploreController', function ($scope, $state, EventPipe, myExploreApi) {

    this.activeTab = 0;

    this.activePanel = [0];
    this.openGeneral = true;
    this.openSchema = false;
    this.openPartition = false;

    this.dataList = []; // combined datasets and streams

    var params = {
      namespace: $state.params.namespace,
      scope: $scope
    };

    myExploreApi.list(params)
      .$promise
      .then(function (res) {
        angular.forEach(res, function(v) {
          var split = v.table.split('_');
          v.type = split[0];
          split.splice(0,1); // removing the data type from the array
          v.name = split.join('_');
        });

        this.dataList = res;
        this.selectTable(res[0]);
      }.bind(this));

    EventPipe.on('explore.newQuery', function() {
      if (this.activePanel.indexOf(1) === -1) {
        this.activePanel = [0,1];
      }
    }.bind(this));

    this.selectTable = function (data) {
      // Passing this info to sql-query directive
      this.type = data.type;
      this.name = data.name;

      params.table = data.table;

      // Fetching info of the table
      myExploreApi.getInfo(params)
        .$promise
        .then(function (res) {
          this.selectedInfo = res;
        }.bind(this));

    };

  });
