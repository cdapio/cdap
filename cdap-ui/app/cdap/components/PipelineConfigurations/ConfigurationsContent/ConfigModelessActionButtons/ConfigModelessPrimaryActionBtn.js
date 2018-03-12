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

import React, {Component} from 'react';
import PropTypes from 'prop-types';
import {connect} from 'react-redux';
import IconSVG from 'components/IconSVG';
import {convertKeyValuePairsObjToMap} from 'components/KeyValuePairs/KeyValueStoreActions';
import isEmpty from 'lodash/isEmpty';
import Clipboard from 'clipboard';

const mapStateToProps = (state, ownProps) => {
  return {
    runtimeArgs: state.runtimeArgs,
    validToSave: state.validToSave,
    pipelineEdited: state.pipelineEdited,
    updatingPipelineAndRunOrSchedule: ownProps.updatingPipelineAndRunOrSchedule,
    saveAndRunOrSchedule: ownProps.saveAndRunOrSchedule,
    actionLabel: ownProps.actionLabel,
    isHistoricalRun: ownProps.isHistoricalRun
  };
};

const PrimaryActionBtn = ({runtimeArgs, validToSave, pipelineEdited, updatingPipelineAndRunOrSchedule, saveAndRunOrSchedule, actionLabel, isHistoricalRun}) => {
  let runtimeArgsObj = convertKeyValuePairsObjToMap(runtimeArgs);
  let noRuntimeArgs = isEmpty(runtimeArgsObj);

  return (
    <button
      className="btn btn-primary apply-action"
      disabled={updatingPipelineAndRunOrSchedule || !validToSave || (isHistoricalRun && noRuntimeArgs)}
      onClick={saveAndRunOrSchedule.bind(this, pipelineEdited)}
      data-clipboard-text={JSON.stringify(runtimeArgsObj)}
    >
      <span>{actionLabel}</span>
      {
        updatingPipelineAndRunOrSchedule ?
          <IconSVG
            name="icon-spinner"
            className="fa-spin"
          />
        :
          null
      }
    </button>
  );
};

PrimaryActionBtn.propTypes = {
  runtimeArgs: PropTypes.object,
  validToSave: PropTypes.bool,
  pipelineEdited: PropTypes.bool,
  updatingPipelineAndRunOrSchedule: PropTypes.bool,
  saveAndRunOrSchedule: PropTypes.func,
  actionLabel: PropTypes.string,
  isHistoricalRun: PropTypes.bool
};

const ConnectedPrimaryActionBtn = connect(mapStateToProps)(PrimaryActionBtn);

export default class ConfigModelessPrimaryActionBtn extends Component {
  static propTypes = {
    updatingPipelineAndRunOrSchedule: PropTypes.bool,
    saveAndRunOrSchedule: PropTypes.func,
    actionLabel: PropTypes.string,
    isHistoricalRun: PropTypes.bool
  };

  componentDidMount() {
    if (this.props.isHistoricalRun) {
      new Clipboard('.apply-action');
    }
  }

  render() {
    return (
      <ConnectedPrimaryActionBtn
        updatingPipelineAndRunOrSchedule={this.props.updatingPipelineAndRunOrSchedule}
        saveAndRunOrSchedule={this.props.saveAndRunOrSchedule}
        actionLabel={this.props.actionLabel}
        isHistoricalRun={this.props.isHistoricalRun}
      />
    );
  }
}
