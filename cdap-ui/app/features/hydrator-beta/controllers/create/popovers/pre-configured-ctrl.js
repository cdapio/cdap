class PreConfiguredControllerBeta {
  constructor (rTemplateType, GLOBALS, myPipelineTemplatesApi, ConfigStoreBeta, ConfigActionsFactoryBeta, HydratorServiceBeta, CanvasFactoryBeta, NodesActionsFactoryBeta) {
    this.currentPage = 1;
    this.templates = [];
    this.ConfigActionsFactoryBeta = ConfigActionsFactoryBeta;
    this.HydratorServiceBeta = HydratorServiceBeta;
    this.CanvasFactoryBeta = CanvasFactoryBeta;
    this.myPipelineTemplatesApi = myPipelineTemplatesApi;
    this.NodesActionsFactoryBeta = NodesActionsFactoryBeta;

    this.typeFilter = (rTemplateType === GLOBALS.etlBatch? GLOBALS.etlBatch: GLOBALS.etlRealtime);
    this.fetchTemplates().then((plugins) => {
      this.templates = plugins;
    });
  }

  selectTemplate(template) {
    let result = this.CanvasFactoryBeta.parseImportedJson(
      JSON.stringify(template._properties),
      template.type
    );
    if (result.error) {
      this.myAlertOnValium.show({
        type: 'danger',
        content: 'Imported pre-defined app has issues. Please check the JSON of the imported pre-defined app.'
      });
    } else {
      this.ConfigActionsFactoryBeta.initializeConfigStore(result);
      let configJson = result;
      if (!result.__ui__) {
        configJson = this.HydratorServiceBeta.getNodesAndConnectionsFromConfig(result);
        configJson['__ui__'] = {
          nodes: configJson.nodes.map( (node) => {
            node.properties = node.plugin.properties;
            node.label = node.plugin.label;
            return node;
          })
        };
        configJson.config = {
          connections : configJson.connections
        };
      }
      this.NodesActionsFactoryBeta.createGraphFromConfig(configJson.__ui__.nodes, configJson.config.connections, configJson.config.comments);
    }
  }

  fetchTemplates() {
    return this.myPipelineTemplatesApi.list({
      apptype: this.typeFilter
    })
      .$promise
      .then( (res) => {
        let plugins = res.map( (plugin) => {
          return {
            name: plugin.name,
            description: plugin.description,
            type: this.typeFilter
          };
        });

        angular.forEach(plugins, (plugin) => {
          this.myPipelineTemplatesApi.get({
            apptype: this.typeFilter,
            appname: plugin.name
          })
            .$promise
            .then( (res) => {
              plugin._properties = res;
            });
        });

        return plugins;
      });
  }

}

PreConfiguredControllerBeta.$inject = ['rTemplateType', 'GLOBALS', 'myPipelineTemplatesApi', 'ConfigStoreBeta', 'ConfigActionsFactoryBeta', 'HydratorServiceBeta', 'CanvasFactoryBeta', 'NodesActionsFactoryBeta'];
angular.module(`${PKG.name}.feature.hydrator-beta`)
  .controller('PreConfiguredControllerBeta', PreConfiguredControllerBeta);
