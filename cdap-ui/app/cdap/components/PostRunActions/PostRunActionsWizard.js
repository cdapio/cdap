/*
 * Copyright © 2018-2019 Cask Data, Inc.
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
import IconSVG from 'components/IconSVG';
import { getCurrentNamespace } from 'services/NamespaceStore';
import { MyArtifactApi } from 'api/artifact';
import { Modal, ModalBody } from 'reactstrap';
import { preventPropagation } from 'services/helpers';
import { MyPipelineApi } from 'api/pipeline';
import ConfigurationGroup from 'components/ConfigurationGroup';

export default class PostRunActionsWizard extends Component {
  static propTypes = {
    action: PropTypes.object,
    isOpen: PropTypes.bool,
    toggleModal: PropTypes.func,
  };

  state = {
    widgetJson: {},
  };

  componentDidMount() {
    this.pluginFetch(this.props.action);
  }

  // Fetching Backend Properties
  pluginFetch(action) {
    let { name, version, scope } = action.plugin.artifact;
    let params = {
      namespace: getCurrentNamespace(),
      artifactId: name,
      version,
      scope,
      extensionType: action.plugin.type,
      pluginName: action.plugin.name,
    };

    MyArtifactApi.fetchPluginDetails(params).subscribe((res) => {
      this.props.action._backendProperties = res[0].properties;
      this.fetchWidgets(action);
    });
  }

  // Fetching Widget JSON for the plugin
  fetchWidgets(action) {
    const { name, version, scope } = action.plugin.artifact;
    const widgetKey = `widgets.${action.plugin.name}-${action.plugin.type}`;
    const widgetParams = {
      namespace: getCurrentNamespace(),
      artifactName: name,
      scope: scope,
      artifactVersion: version,
      keys: widgetKey,
    };
    MyPipelineApi.fetchWidgetJson(widgetParams).subscribe((res) => {
      try {
        const parsedWidget = JSON.parse(res[widgetKey]);
        this.setState({
          widgetJson: parsedWidget,
        });
      } catch (e) {
        console.log(`Cannot parse widget JSON for ${action.plugin.name}`, e);
      }
    });
  }

  toggleAndPreventPropagation = (e) => {
    this.props.toggleModal();
    preventPropagation(e);
  };

  renderBody = () => {
    return (
      <ConfigurationGroup
        values={this.props.action.plugin.properties}
        pluginProperties={this.props.action._backendProperties}
        widgetJson={this.state.widgetJson}
        disabled={true}
      />
    );
  };

  render() {
    let action = this.props.action;

    return (
      <Modal
        isOpen={this.props.isOpen}
        toggle={this.toggleAndPreventPropagation}
        backdrop="static"
        modalClassName="post-run-actions-modal hydrator-modal"
      >
        {/* Not using <ModalHeader> here because it wraps the entire header in an h4 */}
        <div className="modal-header">
          <h4 className="modal-title float-left">
            <span>{action.plugin.name || action.name}</span>
            <small className="plugin-version">
              {action.version || action.plugin.artifact.version}
            </small>
            <p>
              <small>{action.description}</small>
            </p>
          </h4>
          <div className="btn-group float-right">
            <a className="btn" onClick={this.toggleAndPreventPropagation}>
              <IconSVG name="icon-close" />
            </a>
          </div>
        </div>
        <ModalBody>
          {this.renderBody()}
          <div
            className="btn btn-blue float-right close-button"
            onClick={this.toggleAndPreventPropagation}
          >
            Close
          </div>
        </ModalBody>
      </Modal>
    );
  }
}
