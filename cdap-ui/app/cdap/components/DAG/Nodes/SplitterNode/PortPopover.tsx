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
import withStyles, { StyleRules, WithStyles } from '@material-ui/core/styles/withStyles';
import { santizeStringForHTMLID } from 'services/helpers';
import { IPort } from 'components/DAG/Nodes/SplitterNode';
import { AbstractNode } from 'components/DAG/Nodes/AbstractNode';
import { endpointCircle, endpointCaret } from 'components/DAG/Nodes/utilities';
import xorWith from 'lodash/xorWith';
import isEqual from 'lodash/isEqual';
import isEmpty from 'lodash/isEmpty';

const styles = (theme): StyleRules => {
  const endpointCircleStyles = endpointCircle(theme);
  return {
    endpointCircle: {
      ...endpointCircleStyles.root,
    },
    regularEndpointCircle: {
      ...endpointCircleStyles.regularEndpointCircle,
    },
    endpointCaret: {
      ...endpointCaret(theme).root,
    },
    root: {
      display: 'block',
      backgroundColor: theme.palette.grey[500],
    },
    portContainer: {
      display: 'block',
      backgroundColor: theme.palette.grey[500],
      position: 'absolute',
      width: 'max-content',
      minHeight: '50px',
      left: '24px',
      padding: '5px 10px 5px 5px',
      borderRadius: '4px',
      border: `1px solid ${theme.palette.blue[100]}`,
      '&:after': {
        content: '""',
        display: 'block',
        position: 'absolute',
        width: 0,
        height: 0,
        borderStyle: 'solid',
        top: 'calc(50% - 10px)',
        right: '99%',
        borderColor: `transparent ${theme.palette.grey[500]} transparent transparent`,
        borderWidth: '10px',
      },
      '&:before': {
        content: '""',
        display: 'block',
        position: 'absolute',
        width: 0,
        height: 0,
        borderStyle: 'solid',
        top: 'calc(50% - 11px)',
        right: '100%',
        borderColor: `transparent ${theme.palette.blue[100]} transparent transparent`,
        borderWidth: '11px',
      },
    },
    portEndpoint: {
      top: 'calc(25% - 2px)',
      left: 'calc(100% + 9px)',
    },
    individualPortContainer: {
      position: 'relative',
      margin: '5px',
    },
  };
};

interface IPortPopoverProps extends WithStyles<typeof styles> {
  nodeName: string;
  nodeType: string;
  ports: IPort[];
  addEndpoint: (
    element: HTMLElement | string | null,
    params: EndpointParams,
    referenceParams: EndpointParams
  ) => void;
  removeEndpoint: (endpointId: string) => void;
}

export function getPortEndpointId(nodeName, portName) {
  return `endpoint_${santizeStringForHTMLID(nodeName)}_port_${santizeStringForHTMLID(portName)}`;
}

export function getPortEndpointElementId(nodeName, portName) {
  return `splitter_node_${santizeStringForHTMLID(nodeName)}_port_${santizeStringForHTMLID(
    portName
  )}`;
}

function PortPopoverComponent({
  nodeName,
  nodeType,
  ports = [],
  classes,
  addEndpoint,
  removeEndpoint,
}: IPortPopoverProps) {
  const [portsBackup, setPortsBackup] = React.useState(ports);
  React.useEffect(() => {
    ports.forEach((port) => {
      const endPointConfigs = AbstractNode.getEndpointConfig();
      endPointConfigs.uuid = getPortEndpointId(nodeName, port.name);
      const endpointElementId = getPortEndpointElementId(nodeName, port.name);
      addEndpoint(endpointElementId, endPointConfigs, {});
    });
  }, []);

  React.useEffect(() => {
    if (isEmpty(xorWith(portsBackup, ports, isEqual))) {
      return;
    }
    portsBackup.forEach((oldPort) => removeEndpoint(getPortEndpointId(nodeName, oldPort.name)));
    ports.forEach((port) => {
      const endPointConfigs = AbstractNode.getEndpointConfig();
      endPointConfigs.uuid = getPortEndpointId(nodeName, port.name);
      const endpointElementId = getPortEndpointElementId(nodeName, port.name);
      addEndpoint(endpointElementId, endPointConfigs, {});
    });
    setPortsBackup(ports);
  }, [ports]);

  return (
    <div className={classes.portContainer}>
      <div className={`${classes.root}`}>
        {ports.map((port) => {
          return (
            <div key={port.name} className={classes.individualPortContainer}>
              <span>{port.label}</span>
              <div
                className={`${classes.endpointCircle} ${classes.portEndpoint}`}
                id={getPortEndpointElementId(nodeName, port.name)}
                data-node-type={nodeType}
                data-endpoint-type={AbstractNode.ENDPOINT.REGULAR}
              >
                <div className={classes.endpointCaret} />
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}

const PortPopover = withStyles(styles)(PortPopoverComponent);
export { PortPopover };
