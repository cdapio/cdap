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

import React, { useState } from 'react';
import withStyles, { WithStyles, StyleRules } from '@material-ui/core/styles/withStyles';
import classnames from 'classnames';

const styles = (): StyleRules => {
  return {
    separator: {
      marginLeft: '5px',
      marginRight: '5px',
    },
    active: {
      fontWeight: 'bold',
      textDecoration: 'underline',
    },
    switcher: {
      cursor: 'pointer',
    },
    switcherContainer: {
      position: 'absolute',
      left: 0,
      right: 0,
    },
    switcherPosition: {
      position: 'absolute',
      right: '15px',
      top: '-25px',
    },
  };
};

interface IChartTableSwitcherProps extends WithStyles<typeof styles> {
  chart: React.ReactNode;
  table: React.ReactNode;
}

const ChartTableSwitcherView: React.FC<IChartTableSwitcherProps> = ({ classes, chart, table }) => {
  const [isTable, setIsTable] = useState(false);

  function handleClick(val) {
    return () => {
      setIsTable(val);
    };
  }

  return (
    <div className={classes.root}>
      <div className={classes.switcherContainer}>
        <div className={classes.switcherPosition}>
          <span
            onClick={handleClick(false)}
            className={classnames(classes.switcher, { [classes.active]: !isTable })}
          >
            Chart
          </span>
          <span className={classes.separator}>|</span>
          <span
            onClick={handleClick(true)}
            className={classnames(classes.switcher, { [classes.active]: isTable })}
          >
            Table
          </span>
        </div>
      </div>
      <div>{isTable ? table : chart}</div>
    </div>
  );
};

const ChartTableSwitcher = withStyles(styles)(ChartTableSwitcherView);
export default ChartTableSwitcher;
