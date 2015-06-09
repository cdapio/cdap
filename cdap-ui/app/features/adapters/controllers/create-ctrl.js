angular.module(PKG.name + '.feature.adapters')
  .controller('AdapterCreateController', function ($scope, AdapterCreateModel, AdapterApiFactory, $q, $alert, $state, $timeout, EventPipe) {
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
      var tab = this.tabs[index];
      var type = tab.type;
      if (type === 'transform' && tab.transform.valid === false) {
        this.model.checkForValidRequiredField(tab.transform);
      } else if (this.model[type] && this.model[type].valid === false){
        this.model.checkForValidRequiredField(this.model[type]);
      }
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

      var pluginName = plugin.toLowerCase();
      var icon = iconMap[pluginName] ? iconMap[pluginName]: 'fa-plug';
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
            // Loading icon shown in model
            EventPipe.emit('hideLoadingIcon.immediate');
            $alert({
              type: 'success',
              content: 'Adapter Template created successfully!'
            });
          }, function(err) {
            // Loading icon shown in model
            EventPipe.emit('hideLoadingIcon.immediate');
            var errorObj = {
              type: 'danger',
              title: 'Error Creating Adapter',
              content: (angular.isArray(err.messages)? formatErrorMessages(err.messages): err.messages.data)
            };
            $alert(errorObj);
          });

          // TODO: Should move it to a template.
          // Constructing html in controller is bad.
          function formatErrorMessages(messages) {
            var formattedMessage = '';
            messages.forEach(function(message) {
              formattedMessage += '<div>';
              formattedMessage += '<strong>' + message.error + '</strong> '
              formattedMessage += '<span>' + message.message + '</span>';
              formattedMessage += '</div>';
            });

            return formattedMessage;
          }
    };

    this.saveAsDraft = function() {
      this.model
          .saveAsDraft()
          .then(
            function success() {
              // Loading icon shown in model
              EventPipe.emit('hideLoadingIcon.immediate');
              $alert({
                type: 'success',
                content: 'The Adapter Template ' + this.model.metadata.name + ' has been saved as draft!'
              });
              $state.go('^.list');
            }.bind(this),
            function error(err) {
              // Loading icon shown in model
              EventPipe.emit('hideLoadingIcon.immediate');
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
          var draft = angular.copy(res[$state.params.data]);
          if (draft) {
            this.model.setMetadata(draft.config.metadata);
            this.onMetadataChange();
            this.model.setSource(draft.config.source);
            this.model.setSink(draft.config.sink);
            this.model.setTransform(draft.config.transforms);
            this.model.setSchedule(draft.config.schedule);
            this.model.setInstance(draft.config.instance);
          }
        }
      }.bind(this));
  });
