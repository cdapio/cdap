angular.module(PKG.name + '.feature.streams')
  .controller('StreamExploreController', function($scope, $state, EventPipe, myStreamApi) {

    this.activePanel = [0];
    this.name = $state.params.streamId;

    EventPipe.on('explore.newQuery', function() {
      this.activePanel = [0,1,2];
    }.bind(this));

    var now = Date.now();

    this.eventSearch = {
      startMs: now-(60*60*1000*2), // two hours ago
      endMs: now,
      limit: 10,
      results: []
    };

    this.doEventSearch = function () {
      var params = {
        namespace: $state.params.namespace,
        streamId: $state.params.streamId,
        scope: $scope,
        start: this.eventSearch.startMs,
        end: this.eventSearch.endMs,
        limit: this.eventSearch.limit
      };
      myStreamApi.eventSearch(params)
        .$promise
        .then(function (res) {
          this.eventSearch.results = res;
        }.bind(this));
    };

    this.doEventSearch();

  });
