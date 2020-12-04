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
import Table from '@material-ui/core/Table';
import TableBody from '@material-ui/core/TableBody';
import TableCell from '@material-ui/core/TableCell';
import TableHead from '@material-ui/core/TableHead';
import TableRow from '@material-ui/core/TableRow';
import Paper from '@material-ui/core/Paper';
import Switch from '@material-ui/core/Switch';
import FormControlLabel from '@material-ui/core/FormControlLabel';
import { Typography, TextField } from '@material-ui/core';
import NewReleasesRoundedIcon from '@material-ui/icons/NewReleasesRounded';
import experimentsList from './experiment-list';
import If from 'components/If';
import { getExperimentValue } from 'services/helpers';

const styles = (): StyleRules => {
  return {
    root: {
      display: 'flex',
      alignItems: 'center',
      flexDirection: 'column',
      paddingTop: '5%',
    },
    paperContainer: {
      display: 'flex',
      height: 'fit-content',
    },
    pageDescription: {
      display: 'flex',
      alignItems: 'flex-start',
      maxWidth: 900,
      width: '100%',
      margin: '20px 0px',
    },
    experimentsTable: {
      maxWidth: 900,
    },
    screenshot: {
      maxWidth: 256,
    },
    defaultExperimentIcon: {
      display: 'block',
      fontSize: 64,
      margin: '0 auto',
    },
    switchCell: {
      width: 145,
    },
  };
};

interface ILabProps extends WithStyles<typeof styles> {}
interface IExperiment {
  experimentId: string;
  enabled: boolean;
  screenshot: string | null;
  name: string;
  description: string;
  showValue?: boolean;
  valueLabel?: string;
  valueType?: string;
  force?: boolean;
}
interface ILabState {
  experiments: IExperiment[];
}

class Lab extends React.Component<ILabProps, ILabState> {
  public componentDidMount() {
    experimentsList.forEach((experiment: IExperiment) => {
      // If the experiment is forcefully disabled do not check
      // the localStorage. Update localStorage with disabled state.
      if (experiment.force && !experiment.enabled) {
        window.localStorage.setItem(experiment.experimentId, experiment.enabled.toString());
        return;
      }
      // If experiment preference is present in storage, use it.
      // If not, use the default value and set it in storage and use it.
      const experimentStatusFromStorage = window.localStorage.getItem(experiment.experimentId);
      if (experimentStatusFromStorage === null) {
        window.localStorage.setItem(experiment.experimentId, experiment.enabled.toString());
      } else {
        experiment.enabled = experimentStatusFromStorage === 'true';
      }
    });
    this.setState({ experiments: experimentsList });
  }

  public updatePreference = (event: React.ChangeEvent<HTMLInputElement>) => {
    const experiments = this.state.experiments.map((experiment: IExperiment) => {
      if (experiment.experimentId === event.target.name) {
        experiment.enabled = !experiment.enabled;
        window.localStorage.setItem(event.target.name, experiment.enabled.toString());
      }
      return experiment;
    });
    this.setState({ experiments });
  };

  public updateExperimentValue = (event: React.ChangeEvent<HTMLInputElement>) => {
    window.localStorage.setItem(event.target.name, event.target.value);
  };

  public render() {
    const { classes } = this.props;

    return (
      <div className={classes.root}>
        <div className={classes.pageDescription}>
          <Typography variant="h3">Lab - Experimental Features</Typography>
        </div>
        <Paper className={classes.paperContainer}>
          <Table className={classes.experimentsTable}>
            <TableHead>
              <TableRow>
                <TableCell></TableCell>
                <TableCell>
                  <Typography variant="h5">Experiment</Typography>
                </TableCell>
                <TableCell>
                  <Typography variant="h5">Enable/Disable</Typography>
                </TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {this.state &&
                this.state.experiments.map((experiment: IExperiment) => (
                  <TableRow key={experiment.experimentId}>
                    <TableCell>
                      {experiment.screenshot ? (
                        <img className={classes.screenshot} src={experiment.screenshot} />
                      ) : (
                        <NewReleasesRoundedIcon className={classes.defaultExperimentIcon} />
                      )}
                    </TableCell>
                    <TableCell>
                      <Typography variant="h5">{experiment.name}</Typography>
                      <br />
                      <Typography variant="body1">{experiment.description}</Typography>
                      <br />
                      <Typography variant="caption">ID: {experiment.experimentId}</Typography>
                      <br />
                      <If condition={experiment.showValue}>
                        <TextField
                          data-cy={`${experiment.experimentId}-field`}
                          variant="outlined"
                          margin="dense"
                          label={experiment.valueLabel || 'Experiment value'}
                          type={experiment.valueType}
                          onChange={this.updateExperimentValue}
                          name={`${experiment.experimentId}-value`}
                          defaultValue={getExperimentValue(experiment.experimentId)}
                        ></TextField>
                      </If>
                    </TableCell>
                    <TableCell className={classes.switchCell}>
                      <FormControlLabel
                        label={experiment.enabled ? 'Enabled' : 'Disabled'}
                        control={
                          <Switch
                            data-cy={`${experiment.experimentId}-switch`}
                            name={experiment.experimentId}
                            color="primary"
                            onChange={this.updatePreference}
                            checked={experiment.enabled}
                            value={experiment.enabled}
                          />
                        }
                      />
                    </TableCell>
                  </TableRow>
                ))}
            </TableBody>
          </Table>
        </Paper>
      </div>
    );
  }
}

function loadDefaultExperiments() {
  experimentsList.forEach((experiment) => {
    if (window.localStorage.getItem(experiment.experimentId) === null) {
      window.localStorage.setItem(experiment.experimentId, experiment.enabled.toString());
    }
  });
}

const StyledLab = withStyles(styles)(Lab);
export default StyledLab;
export { loadDefaultExperiments, IExperiment };
