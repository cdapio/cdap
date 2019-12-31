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
import withStyles from '@material-ui/core/styles/withStyles';
import { IAbstractNodeProps, AbstractNode } from 'components/DAG/Nodes/AbstractNode';
import { genericNodeStyles } from 'components/DAG/Nodes/utilities';
import If from 'components/If';
import ButtonBase from '@material-ui/core/ButtonBase';
const styles = genericNodeStyles({
  border: `1px solid #4586f3`,
  '&.drag-hover': {
    backgroundColor: 'rgba(69, 134, 243, 0.1)',
  },
});
interface ITransformNodeProps extends IAbstractNodeProps<typeof styles> {}
class TransformNodeComponent extends AbstractNode<ITransformNodeProps> {
  public type = 'transform';
  public alertEndpointRef: HTMLElement | null;
  public errorEndpointRef: HTMLElement | null;
  public getEndpointParams = () => {
    const endPointConfigs = this.getEndpointConfig();
    const alertEndpointConfigs = this.getAlertEndpointConfig();
    const errorEndpointConfigs = this.getErrorEndpointConfig();
    return [
      {
        element: this.regularEndpointRef,
        params: endPointConfigs,
        referenceParams: {},
      },
      {
        element: this.alertEndpointRef,
        params: alertEndpointConfigs,
        referenceParams: {},
      },
      {
        element: this.errorEndpointRef,
        params: errorEndpointConfigs,
        referenceParams: {},
      },
    ];
  };
  public checkForValidIncomingConnection = (connObj) => {
    if (connObj.connection.target.getAttribute('data-node-type') !== 'transform') {
      return true;
    }
    return (
      ['alertpublisher', 'sink'].indexOf(
        connObj.connection.source.getAttribute('data-node-type')
      ) === -1
    );
  };

  public render() {
    const {
      classes,
      config: { showAlert = false, showError = false },
    } = this.props;
    const RegularEndpoint = () => (
      <div
        className={`${classes.endpointCircle} ${classes.regularEndpointCircle}`}
        ref={(ref) => (this.regularEndpointRef = ref)}
        data-node-type={this.type}
        data-endpoint-type={AbstractNode.ENDPOINT.REGULAR}
      >
        <div className={classes.endpointCaret} />
      </div>
    );
    const AlertEndpoint = () => (
      <div
        className={`${classes.endpointCircle} ${classes.bottomEndpointCircle} ${
          classes.alertEndpointCircle
        }`}
        ref={(ref) => (this.alertEndpointRef = ref)}
        data-node-type={this.type}
        data-endpoint-type={AbstractNode.ENDPOINT.ALERT}
      >
        <div className={classes.bottomEndpointLabel} data-type="endpoint-label">
          Alert
        </div>
        <div className={classes.bottomEndpointCaret} />
      </div>
    );
    const ErrorEndpoint = () => (
      <div
        className={`${classes.endpointCircle} ${classes.bottomEndpointCircle} ${
          classes.errorEndpointCircle
        }`}
        ref={(ref) => (this.errorEndpointRef = ref)}
        data-node-type={this.type}
        data-endpoint-type={AbstractNode.ENDPOINT.ERROR}
      >
        <div className={classes.bottomEndpointLabel} data-type="endpoint-label">
          Error
        </div>
        <div className={classes.bottomEndpointCaret} />
      </div>
    );
    return (
      <ButtonBase id={this.props.id} className={classes.root} data-node-type={this.type}>
        <span>
          {this.props.config && this.props.config.label ? this.props.config.label : this.props.id}
        </span>
        <RegularEndpoint />
        <If condition={showAlert}>
          <AlertEndpoint />
        </If>
        <If condition={showError}>
          <ErrorEndpoint />
        </If>
      </ButtonBase>
    );
  }
}

const TransformNode = withStyles(styles)(TransformNodeComponent);
export { TransformNode };
