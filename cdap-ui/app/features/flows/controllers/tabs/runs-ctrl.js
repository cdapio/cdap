angular.module(PKG.name + '.feature.flows')
  .controller('FlowsRunsController', function($scope, $filter, $state, rRuns, $bootstrapModal, rFlowsDetail) {
  var fFilter = $filter('filter');
  this.runs = rRuns;
  this.$bootstrapModal = $bootstrapModal;
  this.description = rFlowsDetail.description;

   if ($state.params.runid) {
     var match = fFilter(rRuns, {runid: $state.params.runid});
     if (match.length) {
       this.runs.selected = angular.copy(match[0]);
     } else {
       // Wrong runid. 404.
       $state.go('404');
       return;
     }
   } else if (rRuns.length) {
     this.runs.selected = angular.copy(rRuns[0]);
   } else {
     this.runs.selected = {
       runid: 'No Runs'
     };
   }

    $scope.$watch(angular.bind(this, function() {
     return this.runs.selected.runid;
    }), function() {
      if ($state.params.runid) {
        return;
      } else {
        if (rRuns.length) {
          this.runs.selected = angular.copy(rRuns[0]);
        }
      }
   }.bind(this));

   this.tabs = [
   {
     title: 'Status',
     template: '/assets/features/flows/templates/tabs/runs/tabs/status.html'
   },
   {
    title: 'Flowlets',
    template: '/assets/features/flows/templates/tabs/runs/tabs/flowlets.html'
   },
   {
     title: 'Logs',
     template: '/assets/features/flows/templates/tabs/runs/tabs/log.html'
   }];

   this.activeTab = this.tabs[0];

   this.selectTab = function(tab, node) {
    if (tab.title === 'Flowlets') {
      this.activeFlowlet = node;
    }
    this.activeTab = tab;

   };

   this.openHistory = function() {
     this.$bootstrapModal.open({
       size: 'lg',
       windowClass: 'center cdap-modal',
       templateUrl: '/assets/features/flows/templates/tabs/history.html',
       controller: ['runs', '$scope', function(runs, $scope) {
         $scope.runs = runs;
       }],
       resolve: {
         runs: function() {
           return this.runs;
         }.bind(this)
       }
     });
   };

   this.openDatasets = function() {
     this.$bootstrapModal.open({
       size: 'lg',
       windowClass: 'center cdap-modal',
       templateUrl: '/assets/features/flows/templates/tabs/data.html'
     });
   };
 });
