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
import React, { Component } from 'react';
import withStyles, { WithStyles } from '@material-ui/core/styles/withStyles';
import DataModelViewer from 'components/DataPrep/DataPrepSidePanel/TargetTab/DataModelViewer';

const styles = () => {
  return {
    root: {
      margin: '20px 10px',
      height: '100%',
    },
  };
};

interface ITargetTabProps extends WithStyles<typeof styles> {
  className: string;
  children: React.ReactNode;
}

class TargetTab extends React.PureComponent<ITargetTabProps> {
  constructor(props: Readonly<ITargetTabProps>) {
    super(props);
  }

  public render() {
    const { classes } = this.props;
    return (
      <div className={classes.root}>
        <DataModelViewer />
      </div>
    );
  }
}

export default withStyles(styles)(TargetTab);
