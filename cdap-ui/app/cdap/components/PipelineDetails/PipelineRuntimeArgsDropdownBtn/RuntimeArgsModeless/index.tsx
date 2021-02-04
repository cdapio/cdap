/*
 * Copyright © 2018 Cask Data, Inc.
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

import React, { PureComponent } from 'react';
import RuntimeArgsTabContent from 'components/PipelineDetails/PipelineRuntimeArgsDropdownBtn/RuntimeArgsKeyValuePairWrapper';
import {
  updatePreferences,
  runPipeline,
  fetchAndUpdateRuntimeArgs,
} from 'components/PipelineConfigurations/Store/ActionCreator';
import BtnWithLoading from 'components/BtnWithLoading';
import PipelineRunTimeArgsCounter from 'components/PipelineDetails/PipelineRuntimeArgsCounter';
import { connect } from 'react-redux';
import { convertKeyValuePairsToMap, preventPropagation } from 'services/helpers';
import Popover from 'components/Popover';
import T from 'i18n-react';
import If from 'components/If';
import LoadingSVGCentered from 'components/LoadingSVGCentered';
import withStyles, { StyleRules, WithStyles } from '@material-ui/styles/withStyles';
import Button from '@material-ui/core/Button';

const styles = (theme): StyleRules => {
  return {
    container: {
      display: 'grid',
      gridTemplateRows: 'auto',
      margin: '0',
      textAlign: 'left',
      width: '100%',
      padding: '10px 30px 30px',
    },
    loading: {
      minHeight: '200px',
    },
    tabFooter: {
      borderTop: `1px solid ${theme.palette.grey[400]}`,
      display: 'flex',
      justifyContent: 'space-between',
      paddingTop: '20px',
    },
    argumentLabel: {
      marginTop: '10px',
    },
    btnsContainer: {
      display: 'grid',
      gridTemplateColumns: 'auto auto',
      gridGap: '10px',
    },
    errorContainer: {
      color: theme.palette.red[200],
    },
  };
};

interface IRuntimeArgsModelessProps extends WithStyles<typeof styles> {
  runtimeArgs: any;
  onClose: () => void;
}

const I18N_PREFIX =
  'features.PipelineDetails.PipelineRuntimeArgsDropdownBtn.RuntimeArgsTabContent.RuntimeArgsModeless';
class RuntimeArgsModeless extends PureComponent<IRuntimeArgsModelessProps> {
  public state = {
    saving: false,
    savingAndRun: false,
    error: null,
    initialPropsLoading: true,
  };

  public toggleSaving = () => {
    this.setState({
      saving: !this.state.saving,
    });
  };

  public toggleSavingAndRun = () => {
    this.setState({
      savingAndRun: !this.state.savingAndRun,
    });
  };

  public saveRuntimeArgs = (e) => {
    preventPropagation(e);
    this.toggleSaving();
    updatePreferences().subscribe(
      () => {
        this.setState(
          {
            saving: false,
          },
          this.props.onClose
        );
      },
      (err) => {
        // Timeout to prevent janky experience when the response fails quick.
        setTimeout(() => {
          this.setState({
            error: err.response || JSON.stringify(err),
            saving: false,
          });
        }, 1000);
      }
    );
  };

  public runPipelineWithArguments = () => {
    this.toggleSavingAndRun();
    const { runtimeArgs } = this.props;
    // Arguments with empty values are assumed to be provided from the pipeline
    runtimeArgs.pairs = runtimeArgs.pairs.filter((runtimeArg) => runtimeArg.value);
    const runtimeArgsMap = convertKeyValuePairsToMap(runtimeArgs.pairs);
    runPipeline(runtimeArgsMap, false).subscribe(
      () => {
        this.props.onClose();
      },
      (err) => {
        // Timeout to prevent janky experience when the response fails quick.
        setTimeout(() => {
          this.setState({
            savingAndRun: !this.state.savingAndRun,
            error: err.response || JSON.stringify(err),
          });
        }, 1000);
      }
    );
  };

  public onRuntimeArgsChanged = () => {
    this.setState({
      error: null,
    });
  };

  public componentDidMount() {
    fetchAndUpdateRuntimeArgs().subscribe(() => {
      this.setState({ initialPropsLoading: false });
    });
  }

  public render() {
    const { classes } = this.props;
    const SaveBtn = () => {
      return (
        <BtnWithLoading
          loading={this.state.saving}
          className="btn btn-primary"
          data-cy="save-runtime-args-btn"
          onClick={this.saveRuntimeArgs}
          disabled={this.state.saving || this.state.savingAndRun}
          label="Save"
        />
      );
    };
    const RunBtn = () => {
      return (
        <BtnWithLoading
          loading={this.state.savingAndRun}
          className="btn btn-secondary"
          onClick={this.runPipelineWithArguments}
          disabled={this.state.saving || this.state.savingAndRun}
          label="Run"
          data-cy="run-deployed-pipeline-modal-btn"
        />
      );
    };
    return (
      <div className={classes.container} data-cy="runtime-args-modeless">
        <If condition={this.state.initialPropsLoading}>
          <div className={classes.loading}>
            <LoadingSVGCentered data-cy="runtime-args-modeless-loading" />
          </div>
        </If>
        <If condition={!this.state.initialPropsLoading}>
          <div className={classes.argumentsLabel}>{T.translate(`${I18N_PREFIX}.specifyArgs`)}</div>
          <RuntimeArgsTabContent
            runtimeArgs={this.props.runtimeArgs}
            onRuntimeArgsChange={this.onRuntimeArgsChanged}
          />
          <div className={classes.tabFooter}>
            <div className={classes.btnsContainer}>
              <Popover target={SaveBtn} placement="left" showOn="Hover">
                {T.translate(`${I18N_PREFIX}.saveBtnPopover`)}
              </Popover>
              <Popover target={RunBtn} showOn="Hover" placement="right">
                {T.translate(`${I18N_PREFIX}.runBtnPopover`)}
              </Popover>
            </div>
            <PipelineRunTimeArgsCounter />
          </div>
          <If condition={this.state.error}>
            <div className={classes.errorContainer}>{this.state.error}</div>
          </If>
        </If>
      </div>
    );
  }
}

const mapStateToProps = (state) => {
  return {
    runtimeArgs: state.runtimeArgs,
  };
};

const ConnectedRuntimeArgsModeless = connect(mapStateToProps)(
  withStyles(styles)(RuntimeArgsModeless)
);

export default ConnectedRuntimeArgsModeless;
