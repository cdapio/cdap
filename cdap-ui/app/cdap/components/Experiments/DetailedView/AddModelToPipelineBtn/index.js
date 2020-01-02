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

import PropTypes from 'prop-types';
import React, { Component } from 'react';
import { MyArtifactApi } from 'api/artifact';
import { getCurrentNamespace } from 'services/NamespaceStore';
import { isNilOrEmpty, objectQuery } from 'services/helpers';
import getPipelineConfig from 'components/Experiments/DetailedView/AddModelToPipelineBtn/PipelineSkeleton';
import { GLOBALS } from 'services/global-constants';
import Popover from 'components/Popover';
import IconSVG from 'components/IconSVG';
import { connect } from 'react-redux';
import uuidV4 from 'uuid/v4';
import { myExperimentsApi } from 'api/experiments';
import {
  createWorkspace,
  applyDirectives,
} from 'components/Experiments/store/CreateExperimentActionCreator';
import classnames from 'classnames';
require('./AddModelToPipelineBtn.scss');

const MMDS_PLUGINS_ARTIFACT_NAME = 'mmds-plugins';
const ERROR_MSG = 'Unable to find plugins to create a scoring pipeline';

class AddModelToPipelineBtn extends Component {
  static propTypes = {
    experimentId: PropTypes.string,
    modelId: PropTypes.string,
    modelName: PropTypes.string,
    srcPath: PropTypes.string,
    directives: PropTypes.arrayOf(PropTypes.string),
    splitId: PropTypes.string,
    disabled: PropTypes.bool,
  };

  static defaultProps = {
    disabled: false,
  };

  state = {
    disabled: this.props.disabled,
    error: null,
    mmdsPluginsArtifact: null,
    datapipelineArtifact: null,
    wranglerArtifact: null,
    corepluginsArtifact: null,
    schema: null,
  };

  cloneId = uuidV4();
  batchPipelineUrl = window.getHydratorUrl({
    stateName: 'hydrator.create',
    stateParams: {
      namespace: getCurrentNamespace(),
      cloneId: this.cloneId,
      artifactType: GLOBALS.etlDataPipeline,
    },
  });

  componentDidMount() {
    this.setWorkspaceId();
    this.fetchArtifactForPipelines();
  }

  setWorkspaceId = () => {
    let { srcPath } = this.props;
    createWorkspace(srcPath).subscribe((res) => {
      let workspaceId;
      let { directives } = this.props;
      workspaceId = res.values[0].id;
      this.setState({
        workspaceId,
      });
      applyDirectives(workspaceId, directives).subscribe();
    });
  };

  fetchArtifactForPipelines() {
    let { experimentId, splitId } = this.props;
    MyArtifactApi.list({
      namespace: getCurrentNamespace(),
    })
      .combineLatest([
        myExperimentsApi.getSplitDetails({
          namespace: getCurrentNamespace(),
          experimentId,
          splitId,
        }),
      ])
      .subscribe(
        ([artifacts, splitDetails]) => {
          let mmdsPluginsArtifact, datapipelineArtifact, wranglerArtifact, corepluginsArtifact;
          artifacts.forEach((artifact) => {
            switch (artifact.name) {
              case MMDS_PLUGINS_ARTIFACT_NAME:
                mmdsPluginsArtifact = artifact;
                break;
              case GLOBALS.etlDataPipeline:
                datapipelineArtifact = artifact;
                break;
              case GLOBALS.wrangler.pluginArtifactName:
                wranglerArtifact = artifact;
                break;
              case 'core-plugins':
                // FIXME: We need to move this to use some constant.
                corepluginsArtifact = artifact;
                break;
            }
          });
          let schema = splitDetails.schema;
          if (
            isNilOrEmpty(mmdsPluginsArtifact) ||
            isNilOrEmpty(datapipelineArtifact) ||
            isNilOrEmpty(wranglerArtifact) ||
            isNilOrEmpty(corepluginsArtifact) ||
            isNilOrEmpty(schema)
          ) {
            this.setState({
              error: ERROR_MSG,
            });
          } else {
            this.setState({
              mmdsPluginsArtifact,
              datapipelineArtifact,
              wranglerArtifact,
              corepluginsArtifact,
              schema,
              disabled: this.props.disabled || false,
            });
          }
        },
        () => {
          this.setState({
            error: ERROR_MSG,
          });
        }
      );
  }
  generatePipelineConfig = () => {
    let { experimentId, modelId, modelName, srcPath, directives } = this.props;
    let {
      mmdsPluginsArtifact,
      wranglerArtifact,
      datapipelineArtifact,
      corepluginsArtifact,
      workspaceId,
      schema,
    } = this.state;
    let pipelineConfig = getPipelineConfig({
      mmds: {
        mmdsPluginsArtifact,
        experimentId,
        modelId,
      },
      wrangler: {
        wranglerArtifact,
        directives,
        schema,
        workspaceId,
      },
      file: {
        corepluginsArtifact,
        srcPath,
      },
    });
    pipelineConfig = {
      ...pipelineConfig,
      name: `Scoring_Pipeline_${experimentId}_${modelName}`,
      description: `Scoring pipeline for ${modelName} under experiment ${experimentId}.`,
      artifact: datapipelineArtifact,
    };
    window.localStorage.setItem(this.cloneId, JSON.stringify(pipelineConfig));
  };

  render() {
    const AnchorTag = this.state.disabled ? 'button' : 'a';
    return (
      <fieldset className="add-model-to-pipeline" disabled={this.state.disabled}>
        <AnchorTag
          className={classnames('btn btn-primary btn-sm', {
            disabled: this.state.disabled,
          })}
          onClick={this.generatePipelineConfig}
          href={this.state.disabled ? null : this.batchPipelineUrl}
        >
          <span>Create a scoring pipeline</span>
          {this.state.error ? (
            <Popover target={() => <IconSVG name="icon-exclamation-triangle" />} showOn="Hover">
              {this.state.error}
            </Popover>
          ) : null}
        </AnchorTag>
      </fieldset>
    );
  }
}

const mapStateToProps = (state, ownProps) => {
  let modelObj = state.models.find((model) => model.id === ownProps.modelId);
  return {
    experimentId: state.name,
    modelId: ownProps.modelId,
    modelName: objectQuery(modelObj, 'name'),
    directives: objectQuery(modelObj, 'directives'),
    srcPath: state.srcpath,
    splitId: objectQuery(modelObj, 'split'),
  };
};

const ConnectedAddModelToPipelineBtn = connect(mapStateToProps)(AddModelToPipelineBtn);
export default ConnectedAddModelToPipelineBtn;
