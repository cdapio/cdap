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
import { Theme } from '@material-ui/core/styles';
import withStyles, { WithStyles } from '@material-ui/core/styles/withStyles';
import T from 'i18n-react';
import InputLabel from '@material-ui/core/InputLabel';
import MenuItem from '@material-ui/core/MenuItem';
import Select from '@material-ui/core/Select';

const PREFIX = 'features.DataPrep.DataPrepSidePanel.TargetTab';

interface IDataModelViewerState {
  selectedDataModelId: string;
}

const styles = (theme: Theme) => {
  return {
    root: {
      height: '100%',
      width: '100%',
    },
    select: {
      width: '100%',
    },
  };
};
interface IDataModelViewerProps extends WithStyles<typeof styles> {}

class DataModelViewer extends React.Component<IDataModelViewerProps, IDataModelViewerState> {
  public state = {
    selectedDataModelId: 'OMOP',
  };

  public render() {
    const { classes } = this.props;
    return (
      <div className={classes.root}>
        <p>{T.translate(`${PREFIX}.selectDataModel`)}</p>
        <InputLabel>{T.translate(`${PREFIX}.targetDataModel`)}</InputLabel>
        <Select value={this.state.selectedDataModelId} className={classes.select} disabled>
          <MenuItem value="OMOP">{'OMOP'}</MenuItem>
        </Select>
      </div>
    );
  }
}

export default withStyles(styles)(DataModelViewer);
