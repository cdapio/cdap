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

import React, { Component } from 'react';
import PropTypes from 'prop-types';
import classnames from 'classnames';
import IconSVG from 'components/IconSVG';
import PipelineConfigurations from 'components/PipelineConfigurations';
import T from 'i18n-react';
import { fetchAndUpdateRuntimeArgs } from 'components/PipelineConfigurations/Store/ActionCreator';

const PREFIX = 'features.PipelineDetails.TopPanel';

export default class PipelineConfigureButton extends Component {
  static propTypes = {
    pipelineType: PropTypes.string,
    pipelineName: PropTypes.string,
    resolvedMacros: PropTypes.object,
    runtimeArgs: PropTypes.array,
  };

  state = {
    showModeless: false,
  };

  constructor(props) {
    super(props);

    this.buttonRef = React.createRef();
  }

  getRuntimeArgumentsAndToggleModeless = (open) => {
    if (!this.state.showModeless) {
      fetchAndUpdateRuntimeArgs().subscribe(this.toggleModeless.bind(this, open));
    } else {
      this.toggleModeless(open);
    }
  };

  toggleModeless = (open) => {
    this.setState({
      showModeless: open,
    });
  };

  renderConfigureButton() {
    return (
      <div
        onClick={this.getRuntimeArgumentsAndToggleModeless.bind(this, true)}
        className="btn pipeline-action-btn pipeline-configure-btn"
        data-cy="pipeline-configure-btn"
        ref={this.buttonRef}
      >
        <div className="btn-container">
          <IconSVG name="icon-sliders" className="configure-icon" />
          <div className="button-label">{T.translate(`${PREFIX}.configure`)}</div>
        </div>
      </div>
    );
  }

  render() {
    return (
      <div
        className={classnames('pipeline-action-container pipeline-configure-container', {
          active: this.state.showModeless,
        })}
      >
        {this.renderConfigureButton()}
        <PipelineConfigurations
          open={this.state.showModeless}
          anchorEl={this.buttonRef.current}
          onClose={this.toggleModeless.bind(this, false)}
          isDetailView={true}
          pipelineType={this.props.pipelineType}
          pipelineName={this.props.pipelineName}
        />
      </div>
    );
  }
}
