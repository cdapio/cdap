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
import * as d3 from 'd3';
import withStyles, { WithStyles } from '@material-ui/core/styles/withStyles';
import { render } from 'react-testing-library';

const styles = (theme) => {
  return {
    root: {
      position: 'absolute' as 'absolute',
    },
  };
};

interface ILink {
  source: string;
  destination: string;
}

interface IConnectionsProps extends WithStyles<typeof styles> {
  links: ILink[];
}

class FllConnections extends React.Component<IConnectionsProps> {
  public componentDidMount() {}

  public render() {
    const links = this.props.links;
    return (
      <svg id="connection=container">
        <g>
          {links.map((link) => {
            return <path ref={} />;
          })}
        </g>
      </svg>
    );
  }
}

const StyledConnections = withStyles(styles)(FllConnections);

export default StyledConnections;
