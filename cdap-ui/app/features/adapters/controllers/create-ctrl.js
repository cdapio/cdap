angular.module(PKG.name + '.feature.adapters')
  .controller('AdapterCreateController', function ($scope, AdapterCreateModel, AdapterApiFactory, $q, $alert, $state, $rootScope, $timeout) {
    window.aa = $rootScope;
    this.model = new AdapterCreateModel();
    var defaultTabs = [
      {
        title: 'Default',
        icon: 'fa-cogs',
        isCloseable: false,
        partial: '/assets/features/adapters/templates/create/tabs/default.html'
      }
    ];

    this.tabs = defaultTabs.slice();

    this.closeTab = function(index) {
      this.tabs.splice(index, 1);
    };

    AdapterApiFactory.fetchTemplates()
      .$promise
      .then(function(res) {
        this.adapterTypes = res;
      }.bind(this));

    this.onMetadataChange = function() {
      this.tabs = defaultTabs.slice();
      this.model.resetPlugins();
      this.fetchDefaultPlugins();
    };

    function getIcon(plugin) {
      var iconMap = {
        'script': 'fa-code',
        'twitter': 'fa-twitter',
        'cube': 'fa-cubes',
        'data': 'fa-database',
        'database': 'fa-database',
        'table': 'fa-table',
        'kafka': 'icon-kafka',
        'stream': 'icon-plugin-stream',
        'avro': 'icon-avro',
        'jms': 'icon-jms'
      };

      var pluginName = plugin.toLowerCase(),
          icons = Object.keys(iconMap),
          icon = 'fa-plug';
      for(var i=0; i<icons.length; i++) {
        if (pluginName.indexOf(icons[i]) !== -1) {
          icon = iconMap[icons[i]];
          break;
        }
      }
      return icon;
    }

    this.fetchDefaultPlugins = function fetchDefaultPlugins() {
      var params = {scope: $scope, adapterType: this.model.metadata.type};
      $q.all([
        AdapterApiFactory.fetchSources(params).$promise,
        AdapterApiFactory.fetchSinks(params).$promise,
        AdapterApiFactory.fetchTransforms(params).$promise
      ])
        .then(function(res) {
          function setIcons(plugin) {
            plugin.icon = getIcon(plugin.name);
          }

          this.defaultSources = res[0];
          this.defaultSources.forEach(setIcons);
          this.defaultSinks = res[1];
          this.defaultSinks.forEach(setIcons);
          this.defaultTransforms = res[2];
          this.defaultTransforms.forEach(setIcons);
        }.bind(this));
    };
    this.fetchDefaultPlugins();

    this.publish = function() {
      this.model
          .save()
          .then(function() {
            $timeout(function() {
              $state.go('^.list', $state.params, {reload: true});
            });
            $alert({
              type: 'success',
              content: 'Adapter Template created successfully!'
            });
          }, function(err) {
            $alert({
              type: 'danger',
              content: err.message
            });
          });
    };

    this.saveAsDraft = function() {
      this.model
          .saveAsDraft()
          .then(
            function success() {
              $alert({
                type: 'success',
                content: 'The Adapter Template ' + this.model.metadata.name + ' has been saved as draft!'
              });
              $state.go('^.list');
            }.bind(this),
            function error(err) {
              $alert({
                type: 'info',
                content: err.message
              });
            }
          );
    };

    this.model.getDrafts()
      .then(function(res) {
        if ($state.params.data) {
          var draft = res[$state.params.data];
          if (draft) {
            this.model.setMetadata(draft.config.metadata);
            this.model.setSource(draft.config.source);
            this.model.setSink(draft.config.sink);
            this.model.setTransform(draft.config.transforms);
            this.model.setSchedule(draft.config.schedule);
          }
        }
      }.bind(this));
  });
