/*
 * Copyright © 2019 Cask Data, Inc.
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

import React from 'react';
import Button from '@material-ui/core/Button';
import { ConnectionType } from 'components/DataPrepConnections/ConnectionType';
import DataPrepConnection from 'components/DataPrepConnections';
import { Modal, ModalBody } from 'reactstrap';
import withStyles, { WithStyles } from '@material-ui/core/styles/withStyles';
import ThemeWrapper from 'components/ThemeWrapper';
import MyDataPrepApi from 'api/dataprep';
import ErrorBanner from 'components/ErrorBanner';
import { getCurrentNamespace } from 'services/NamespaceStore';
import { objectQuery } from 'services/helpers';
import LoadingSVGCentered from 'components/LoadingSVGCentered';
import If from 'components/If';
import ee from 'event-emitter';
import { IWidgetProps } from 'components/AbstractWidget';

const styles = (theme) => {
  return {
    modalBtnClose: {
      height: '50px',
      width: '50px',
      boxShadow: 'none',
      border: 0,
      background: 'transparent',
      borderLeft: `1px solid ${theme.palette.grey['300']}`,
      fontWeight: 'bold' as 'bold',
      fontSize: '1.5rem',
      '&:hover': {
        background: theme.palette.blue['40'],
        color: 'white',
      },
    },
  };
};
interface IPluginConnectionState {
  showBrowserModal: boolean;
  error: string | null;
  loading: boolean;
  defaultConnectionId: string | null;
  defaultConnectionType: string | null;
}

interface IConnectionBrowserWidgetProps {
  connectionType: string;
  label: string;
}

interface IPluginConnectionBrowserProps
  extends IWidgetProps<IConnectionBrowserWidgetProps>,
    WithStyles<typeof styles> {}

class PluginConnectionBrowser extends React.PureComponent<
  IPluginConnectionBrowserProps,
  IPluginConnectionState
> {
  public state = {
    showBrowserModal: false,
    error: null,
    loading: true,
    defaultConnectionId: null,
    defaultConnectionType: null,
  };

  private ee = ee(ee);

  public async openDefaultConnection() {
    let connections;
    try {
      connections = await MyDataPrepApi.listConnections({
        context: getCurrentNamespace(),
      }).toPromise();
    } catch (e) {
      this.setState({
        error: 'Unable to fetch connections list to browse',
      });
      return;
    }
    let { connectionType } = this.props.widgetProps;
    connectionType = connectionType && (connectionType.toUpperCase() as ConnectionType);
    const defaultConnection = connections.values.find(
      (connection) => connection.type === connectionType
    );
    if (defaultConnection) {
      const { id, type } = defaultConnection;
      this.setState({
        defaultConnectionId: id,
        defaultConnectionType: type,
        loading: false,
      });
    } else {
      this.setState(
        {
          loading: false,
        },
        () => {
          // ugh.. This delay is to load the DataPrepConnection
          // component so that AddConnection component loads and starts to listen to
          // this event.
          setTimeout(() => {
            this.ee.emit('CREATE_WRANGLER_CONNECTION', connectionType);
          }, 1000);
        }
      );
      return;
    }
  }
  private toggleConnectionBrowser = () => {
    this.setState(
      {
        showBrowserModal: !this.state.showBrowserModal,
        defaultConnectionId: !this.state.showBrowserModal ? null : this.state.defaultConnectionId,
        defaultConnectionType: !this.state.showBrowserModal
          ? null
          : this.state.defaultConnectionType,
      },
      () => {
        if (this.state.showBrowserModal) {
          this.openDefaultConnection();
        }
      }
    );
  };

  private getDatabaseSourceProperties = async (workspaceInfo) => {
    const namespace = getCurrentNamespace();
    const connectionId = objectQuery(workspaceInfo, 'properties', 'connectionid');
    let dbInfo;
    try {
      dbInfo = await MyDataPrepApi.getDatabaseSpecification({
        context: namespace,
        connectionId,
        tableId: workspaceInfo.properties.id,
      }).toPromise();
    } catch (e) {
      return Promise.reject(e);
    }
    return objectQuery(dbInfo, 'values', 0, 'Database', 'properties');
  };

  private getGCSSourceProperties = async (workspaceId: string, workspaceInfo) => {
    const namespace = getCurrentNamespace();
    const connectionId = objectQuery(workspaceInfo, 'properties', 'connectionid');
    let gcsInfo;
    try {
      gcsInfo = await MyDataPrepApi.getGCSSpecification({
        context: namespace,
        connectionId,
        wid: workspaceId,
      }).toPromise();
    } catch (e) {
      return Promise.reject(e);
    }
    let plugin = objectQuery(gcsInfo, 'values', 0);
    const pluginName = Object.keys(plugin)[0]; // this is because the plugin can be GCSFile or GCSFileBlob
    plugin = plugin[pluginName];
    return plugin.properties;
  };

  private getBigQuerySourceProperties = async (workspaceId: string, workspaceInfo) => {
    const namespace = getCurrentNamespace();
    const connectionId = objectQuery(workspaceInfo, 'properties', 'connectionid');
    let bigQueryInfo;
    try {
      bigQueryInfo = await MyDataPrepApi.getBigQuerySpecification({
        context: namespace,
        connectionId,
        wid: workspaceId,
      }).toPromise();
    } catch (e) {
      return Promise.reject(e);
    }
    let plugin = objectQuery(bigQueryInfo, 'values', 0);
    const pluginName = Object.keys(plugin)[0];
    plugin = plugin[pluginName];
    return plugin.properties;
  };

  private getS3SourceProperties = async (workspaceId: string, workspaceInfo) => {
    const namespace = getCurrentNamespace();
    const connectionId = objectQuery(workspaceInfo, 'properties', 'connectionid');
    let s3Info;
    try {
      s3Info = await MyDataPrepApi.getS3Specification({
        context: namespace,
        connectionId,
        activeBucket: objectQuery(workspaceInfo, 'properties', 'bucket-name'),
        key: objectQuery(workspaceInfo, 'properties', 'key'),
        wid: workspaceId,
      }).toPromise();
    } catch (e) {
      return Promise.reject(e);
    }
    return objectQuery(s3Info, 'values', 0, 'S3', 'properties');
  };

  private getSpannerSourceProperties = async (workspaceId: string) => {
    const namespace = getCurrentNamespace();
    let spannerInfo;
    try {
      spannerInfo = await MyDataPrepApi.getSpannerSpecification({
        context: namespace,
        workspaceId,
      }).toPromise();
    } catch (e) {
      return Promise.reject(e);
    }
    let plugin = objectQuery(spannerInfo, 'values', 0);
    const pluginName = Object.keys(plugin)[0];
    plugin = plugin[pluginName];
    return plugin.properties;
  };

  private getKafkaSourceProperties = async (workspaceInfo) => {
    const namespace = getCurrentNamespace();
    const connectionId = objectQuery(workspaceInfo, 'properties', 'connectionid');
    let kafkaInfo;
    try {
      kafkaInfo = await MyDataPrepApi.getKafkaSpecification({
        context: namespace,
        connectionId,
        topic: objectQuery(workspaceInfo, 'properties', 'topic'),
      }).toPromise();
    } catch (e) {
      return Promise.reject(e);
    }
    let plugin = objectQuery(kafkaInfo, 'values', '0');
    const pluginName = Object.keys(plugin)[0];
    plugin = plugin[pluginName];
    return plugin.properties;
  };

  private getSpecification = async (workspaceId: string, workspaceInfo) => {
    let connectionType = this.props.widgetProps.connectionType;
    if (connectionType) {
      connectionType = connectionType.toUpperCase() as ConnectionType;
    }
    let properties;
    switch (connectionType) {
      case ConnectionType.GCS:
        properties = await this.getGCSSourceProperties(workspaceId, workspaceInfo);
        break;
      case ConnectionType.BIGQUERY:
        properties = await this.getBigQuerySourceProperties(workspaceId, workspaceInfo);
        break;
      case ConnectionType.DATABASE:
        properties = await this.getDatabaseSourceProperties(workspaceInfo);
        break;
      case ConnectionType.S3:
        properties = await this.getS3SourceProperties(workspaceId, workspaceInfo);
        break;
      case ConnectionType.SPANNER:
        properties = await this.getSpannerSourceProperties(workspaceId);
        break;
      case ConnectionType.KAFKA:
        properties = await this.getKafkaSourceProperties(workspaceInfo);
        break;
      default:
        return null;
    }
    return properties;
  };

  private resetError = () => {
    this.setState({ error: null });
  };

  private onWorkspaceCreate = async (workspaceId: string) => {
    const namespace = getCurrentNamespace();
    let workspaceInfo;
    try {
      workspaceInfo = await MyDataPrepApi.getWorkspace({
        context: namespace,
        workspaceId,
      }).toPromise();
    } catch (e) {
      this.setState({
        error:
          objectQuery(e, 'response', 'message') || objectQuery(e, 'message') || JSON.stringify(e),
      });
      return;
    }
    workspaceInfo = workspaceInfo.values[0];
    let properties;
    try {
      properties = await this.getSpecification(workspaceId, workspaceInfo);
      this.toggleConnectionBrowser();
    } catch (e) {
      this.setState({
        error:
          objectQuery(e, 'response', 'message') || objectQuery(e, 'message') || JSON.stringify(e),
      });
    }
    try {
      await MyDataPrepApi.delete({
        context: namespace,
        workspaceId,
      }).toPromise();
    } catch (e) {
      // tslint:disable-next-line:no-console
      console.error('Unable to clean up workspace post browse: ', workspaceId, e);
    }

    this.props.updateAllProperties(properties);
  };

  public render() {
    const { widgetProps, classes } = this.props;
    const { label = 'Browse' } = widgetProps;
    return (
      <React.Fragment>
        <Button variant="contained" color="primary" onClick={this.toggleConnectionBrowser}>
          {label}
        </Button>
        <Modal
          isOpen={this.state.showBrowserModal}
          toggle={this.toggleConnectionBrowser}
          size="lg"
          modalClassName="wrangler-modal"
          backdrop="static"
          zIndex="1061"
        >
          <div className="modal-header">
            <h5 className="modal-title">{label}</h5>
            <button className={classes.modalBtnClose} onClick={this.toggleConnectionBrowser}>
              <span>{String.fromCharCode(215)}</span>
            </button>
          </div>
          <ModalBody>
            <If condition={!this.state.loading}>
              <DataPrepConnection
                enableRouting={false}
                singleWorkspaceMode={true}
                sidePanelExpanded={true}
                scope="plugin-browser-source"
                onWorkspaceCreate={this.onWorkspaceCreate}
                defaultConnectionId={this.state.defaultConnectionId}
                defaultConnectionType={this.state.defaultConnectionType}
              />
            </If>
            <If condition={this.state.loading}>
              <LoadingSVGCentered />
            </If>
          </ModalBody>
        </Modal>
        <ErrorBanner error={this.state.error} onClose={this.resetError} />
      </React.Fragment>
    );
  }
}
const StyledPluginConnectionBrowser = withStyles(styles)(PluginConnectionBrowser);
function PluginConnectionBrowserWrapper(props) {
  return (
    <ThemeWrapper>
      <StyledPluginConnectionBrowser {...props} />
    </ThemeWrapper>
  );
}

export default PluginConnectionBrowserWrapper;
