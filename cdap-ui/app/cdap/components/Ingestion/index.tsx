/*
 * Copyright © 2020 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

import * as React from 'react';
import withStyles, { WithStyles, StyleRules } from '@material-ui/core/styles/withStyles';
import PluginList from 'components/Ingestion/PluginList';
import PluginsTableView from 'components/Ingestion/PluginsTableView';
import { MyPipelineApi } from 'api/pipeline';
import SourceSinkConfig from 'components/Ingestion/SourceSinkConfigurator';
import { Button, TextField } from '@material-ui/core';
import Helmet from 'react-helmet';
import { objectQuery } from 'services/helpers';
import If from 'components/If';
import IconButton from '@material-ui/core/IconButton';
import CloseIcon from '@material-ui/icons/Close';
import Alert from 'components/Alert';
import { getPluginDisplayName } from 'components/Ingestion/helpers';
import { getCurrentNamespace } from 'services/NamespaceStore';
import VersionStore from 'services/VersionStore';
import VersionActions from 'services/VersionStore/VersionActions';
import MyCDAPVersionApi from 'api/version.js';
import PipelineConfigurationsStore from 'components/PipelineConfigurations/Store';

const styles = (theme): StyleRules => {
  return {
    root: {
      display: 'flex',
      flexDirection: 'column',
    },
    createJobHeader: {
      backgroundColor: theme.palette.grey[700],
      padding: '5px',
      paddingLeft: '20px',
      height: '60px',
      display: 'flex',
      alignItems: 'center',
    },
    selectionContainer: { padding: '20px' },
    propsRenderBlock: {
      margin: '0 40px',
      width: '40%',
      propsContainer: {
        display: 'flex',
      },
      flexDirection: 'row',
      justifyContent: 'center',
    },
    filterBox: {
      display: 'flex',
      flexDirection: 'column',
      maxWidth: '500px',
      marginBottom: '10px',
    },
    filterInput: {
      marginBottom: '10px',
    },
    jobInfo: { display: 'flex', flexDirection: 'column', margin: '10px' },
    instructionHeading: {
      marginTop: '10px',
    },
    modalContent: {
      height: '90%',
      margin: '100px',
    },
    countAndToggle: {
      display: 'flex',
      justifyContent: 'space-between',
      marginBottom: '10px',
      fontSize: '1rem',
    },
    toggleView: {
      color: theme.palette.blue[100],
      cursor: 'pointer',
    },
    configureHeader: {
      display: 'flex',
      flexDirection: 'row',
      justifyContent: 'space-between',
      alignItems: 'center',
      padding: '5px',
      paddingLeft: '20px',
      backgroundColor: theme.palette.grey[700],
    },
    transferDetailsContainer: {
      maxWidth: '500px',
      flexDirection: 'column',
      display: 'flex',
    },
    deployTransferBtn: {
      alignSelf: 'flex-end',
    },
  };
};

interface IIngestionProps extends WithStyles<typeof styles> {
  test: string;
  pluginsMap: any;
}
// TODO(itsanudeep) : Extract utility functions in the feature and make them reusable.
class IngestionView extends React.Component<IIngestionProps> {
  public state = {
    filterStr: '',
    filteredSources: [],
    batchsource: [],
    batchsink: [],
    selectedSource: null,
    selectedSink: null,
    sourceBP: null,
    sinkBP: null,
    pipelineName: '',
    publishingPipeline: false,
    pipelineDescription: '',
    modalOpen: false,
    tableView: true,
    showConfig: false,
    pipelineNameError: false,
    deployFailed: false,
    cdapVersion: '',
    currentNs: 'default',
  };

  public componentDidMount() {
    const currentNs = getCurrentNamespace();
    let cdapVersion = VersionStore.getState().version;
    if (!cdapVersion) {
      MyCDAPVersionApi.get().subscribe((res) => {
        cdapVersion = res.version;
        VersionStore.dispatch({
          type: VersionActions.updateVersion,
          payload: {
            version: res.version,
          },
        });
        this.fetchSourceSink(cdapVersion, currentNs);
        this.setState({ currentNs, cdapVersion });
      });
    } else {
      this.fetchSourceSink(cdapVersion, currentNs);
      this.setState({ currentNs, cdapVersion });
    }
  }

  public fetchSourceSink = (version, namespace) => {
    this.fetchPlugins('batchsource', version, namespace);
    this.fetchPlugins('batchsink', version, namespace);
  };

  public fetchPlugins = async (extensionType, version, namespace = 'default') => {
    const res = await MyPipelineApi.fetchPlugins({
      namespace,
      pipelineType: 'cdap-data-pipeline',
      version,
      extensionType,
    }).toPromise();
    const promises = res.map(async (plugin) => {
      const params = {
        namespace,
        artifactName: plugin.artifact.name,
        artifactVersion: plugin.artifact.version,
        scope: plugin.artifact.scope,
        keys: `widgets.${plugin.name}-${plugin.type}`,
      };
      const widgetJ = await MyPipelineApi.fetchWidgetJson(params).toPromise();
      const widgetJson = JSON.parse(widgetJ[`widgets.${plugin.name}-${plugin.type}`]);
      const pluginWithWidgetJson = { ...plugin, widgetJson };
      return pluginWithWidgetJson;
    });
    // TODO(itsanudeep): Refactor to use combineLatest instead of converting to promises.
    Promise.all(promises).then((data) => {
      this.setState(
        {
          [extensionType]: data.sort((a: any, b: any) => {
            const aName = getPluginDisplayName(a);
            const bName = getPluginDisplayName(b);
            return aName.localeCompare(bName, undefined, {
              sensitivity: 'accent',
            });
          }),
        },
        () => {
          if (extensionType === 'batchsource') {
            this.onFilter({ target: { value: '' } });
          }
        }
      );
    });
  };

  public onPluginSelect = (plugin) => {
    let type: string;
    if (plugin.type === 'batchsource') {
      if ((this.state.selectedSource && this.state.selectedSource.name) === plugin.name) {
        return;
      }
      type = 'selectedSource';
    } else if (plugin.type === 'batchsink') {
      if ((this.state.selectedSink && this.state.selectedSink.name) === plugin.name) {
        return;
      }
      type = 'selectedSink';
    } else {
      return;
    }
    const label = getPluginDisplayName(plugin);
    this.setState(
      {
        [type]: { ...plugin, label },
        modalOpen: type === 'selectedSink' ? true : false,
        showConfig: type === 'selectedSink' ? true : false,
      },
      () => {
        this.getPluginProps(plugin);
      }
    );
  };

  public onSourceSinkSelect = (selectedSource, selectedSink) => {
    const sourceLabel = getPluginDisplayName(selectedSource);
    const sinkLabel = getPluginDisplayName(selectedSink);
    this.setState(
      {
        selectedSink: { ...selectedSink, label: sinkLabel },
        selectedSource: { ...selectedSource, label: sourceLabel },
      },
      () => {
        this.getPluginProps(selectedSource);
        this.getPluginProps(selectedSink);
        this.setState({ modalOpen: true, showConfig: true });
      }
    );
  };
  public toggleView = () => {
    this.setState({ tableView: !this.state.tableView });
  };
  public getPluginProps = (plugin) => {
    const pluginParams = {
      namespace: this.state.currentNs,
      parentArtifact: 'cdap-data-pipeline',
      version: this.state.cdapVersion,
      extension: plugin.type,
      pluginName: plugin.name,
      scope: 'SYSTEM',
      artifactName: plugin.artifact.name,
      artifactScope: plugin.artifact.scope,
      limit: 1,
      order: 'DESC',
    };
    MyPipelineApi.getPluginProperties(pluginParams).subscribe((res) => {
      if (plugin.type === 'batchsource') {
        this.setState({ sourceBP: res[0] });
      } else if (plugin.type === 'batchsink') {
        this.setState({ sinkBP: res[0] });
      }
    });
  };

  public generatePipelineConfig = () => {
    let stages = [];
    let connections = [];
    if (this.state.selectedSource && this.state.selectedSink) {
      const sourceName =
        this.state.selectedSource.label ||
        objectQuery(this.state.selectedSource, 'plugin', 'name') ||
        objectQuery(this.state.selectedSource, 'name');
      const sinkName =
        this.state.selectedSink.label ||
        objectQuery(this.state.selectedSink, 'plugin', 'name') ||
        objectQuery(this.state.selectedSink, 'name');
      stages = [
        {
          name: sourceName,
          plugin: {
            name: this.state.selectedSource.name,
            type: this.state.selectedSource.type,
            label: this.state.selectedSource.label,
            artifact: this.state.selectedSource.artifact,
            properties: this.state.selectedSource.properties,
          },
        },
        {
          name: sinkName,
          plugin: {
            name: this.state.selectedSink.name,
            type: this.state.selectedSink.type,
            label: this.state.selectedSink.label,
            artifact: this.state.selectedSink.artifact,
            properties: this.state.selectedSink.properties,
          },
        },
      ];
      connections = [
        {
          to: sinkName,
          from: sourceName,
        },
      ];
    }
    const {
      engine,
      driverResources,
      resources,
      maxConcurrentRuns,
      numOfRecordsPreview,
      stageLoggingEnabled,
      processTimingEnabled,
      schedule,
    } = PipelineConfigurationsStore.getState();
    const configuration = {
      artifact: {
        name: 'cdap-data-pipeline',
        version: this.state.cdapVersion,
        scope: 'SYSTEM',
      },
      description: this.state.pipelineDescription,
      name: this.state.pipelineName,
      label: 'data-ingestion-job',
      config: {
        resources,
        driverResources,
        connections,
        properties: {},
        processTimingEnabled,
        stageLoggingEnabled,
        stages,
        schedule,
        engine,
        numOfRecordsPreview,
        maxConcurrentRuns,
      },
    };
    return configuration;
  };
  public publishPipeline = () => {
    if (this.state.pipelineName) {
      this.setState({ publishingPipeline: true });
      const configuration = this.generatePipelineConfig();
      MyPipelineApi.publish(
        {
          namespace: this.state.currentNs,
          appId: configuration.name,
        },
        configuration
      )
        .toPromise()
        .then(() => {
          const namespace = getCurrentNamespace();
          window.location.href = `../pipelines/ns/${namespace}/view/${this.state.pipelineName}`;
          this.setState({ publishingPipeline: false });
        })
        .catch((err) => {
          // tslint:disable-next-line: no-console
          console.log('publishing pipeline failed', err);
          this.setState({ publishingPipeline: false, deployFailed: true });
        });
    } else {
      this.setState({ pipelineNameError: true });
    }
  };
  public onFilter = (event) => {
    const { value } = event.target;
    const filteredSources = this.state.batchsource.filter((source) => {
      const displayName = getPluginDisplayName(source);
      return displayName && displayName.toLowerCase().includes(value);
    });
    this.setState({ filteredSources, filterStr: value });
  };

  public onSourceChange = (newSource) => {
    this.setState({ selectedSource: newSource });
  };
  public onSinkChange = (newSource) => {
    this.setState({ selectedSink: newSource });
  };
  public closeModal = () => {
    this.setState({
      modalOpen: false,
      showConfig: false,
      pipelineName: '',
      pipelineDescription: '',
    });
  };
  public render() {
    const { classes } = this.props;
    return (
      <div className={classes.root}>
        <Helmet title={'Ingestion'} />
        <Alert
          showAlert={this.state.deployFailed}
          type={'error'}
          message={'Failed to deploy transfer job'}
          onClose={() => this.setState({ deployFailed: false })}
        />
        <If condition={!this.state.showConfig}>
          <React.Fragment>
            <div className={classes.createJobHeader}>
              <h4>Create a transfer job.</h4>
            </div>
            <div className={classes.selectionContainer}>
              <h4 className={classes.instructionHeading}>
                Select a source and target for the transfer.
              </h4>
              <div className={classes.filterBox}>
                <TextField
                  className={classes.filterInput}
                  variant="outlined"
                  label="Search by source name"
                  margin="dense"
                  value={this.state.filterStr}
                  onChange={this.onFilter}
                />
              </div>
              <div className={classes.countAndToggle}>
                <span>{`${this.state.filteredSources.length} sources`}</span>
                <span className={classes.toggleView} onClick={this.toggleView}>
                  Toggle View
                </span>
              </div>

              {this.state.tableView ? (
                <PluginsTableView
                  onSourceSinkSelect={this.onSourceSinkSelect}
                  plugins={this.state.filteredSources}
                  sinks={this.state.batchsink}
                />
              ) : (
                <PluginList
                  title="Sources"
                  plugins={this.state.filteredSources}
                  onPluginSelect={this.onPluginSelect}
                  onSourceSinkSelect={this.onSourceSinkSelect}
                  sinks={this.state.batchsink}
                />
              )}
            </div>
          </React.Fragment>
        </If>
        <If condition={this.state.showConfig}>
          <div className={classes.configureHeader}>
            <h4>Configure the transfer job.</h4>
            <IconButton onClick={this.closeModal}>
              <CloseIcon fontSize="large" />
            </IconButton>
          </div>
          <div className={classes.jobInfo}>
            <div className={classes.transferDetailsContainer}>
              <TextField
                variant="outlined"
                label="Transfer Name"
                margin="dense"
                required={true}
                error={this.state.pipelineNameError}
                value={this.state.pipelineName}
                onChange={(event) =>
                  this.setState({
                    pipelineName: event.target.value,
                    pipelineNameError: false,
                  })
                }
              />
              <TextField
                variant="outlined"
                label="Description"
                margin="dense"
                value={this.state.pipelineDescription}
                onChange={(event) => this.setState({ pipelineDescription: event.target.value })}
              />
            </div>
            <Button
              className={classes.deployTransferBtn}
              disabled={this.state.publishingPipeline}
              color="primary"
              variant="contained"
              onClick={this.publishPipeline}
            >
              Deploy Transfer
            </Button>
          </div>
          <SourceSinkConfig
            sourceBP={this.state.sourceBP}
            selectedSource={this.state.selectedSource}
            sinkBP={this.state.sinkBP}
            selectedSink={this.state.selectedSink}
            onSourceChange={this.onSourceChange}
            onSinkChange={this.onSinkChange}
          />
        </If>
      </div>
    );
  }
}

const Ingestion = withStyles(styles)(IngestionView);
export default Ingestion;
