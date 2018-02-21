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
import IconSVG from 'components/IconSVG';
import {UncontrolledTooltip} from 'components/UncontrolledComponents';
import {getCurrentNamespace} from 'services/NamespaceStore';
import AvailablePluginsStore from 'services/AvailablePluginsStore';
import {MyArtifactApi} from 'api/artifact';
import {generateNodeConfig} from 'services/HydratorPluginConfigFactory';
import { ModalBody } from 'reactstrap';
import HydratorModal from 'components/HydratorModal';
import SelectWithOptions from 'components/SelectWithOptions';
import KeyValuePairs from 'components/KeyValuePairs';
import DSVEditor from 'components/DSVEditor';
import {convertMapToKeyValuePairsObj} from 'services/helpers';
import shortid from 'shortid';

export default class PostRunActionsWizard extends Component {
  static propTypes = {
    action: PropTypes.object,
    isOpen: PropTypes.bool,
    toggleModal: PropTypes.func
  };

  state = {
    groupsConfig: {}
  };

  componentWillMount() {
    if (!Object.keys(this.props.action._backendProperties || {}).length) {
      this.pluginFetch(this.props.action);
    } else {
      this.fetchWidgets(this.props.action);
    }
  }

  // Fetching Backend Properties
  pluginFetch(action) {
    let {name, version, scope} = action.plugin.artifact;
    let params = {
      namespace: getCurrentNamespace(),
      artifactId: name,
      version,
      scope,
      extensionType: action.plugin.type,
      pluginName: action.plugin.name
    };

    MyArtifactApi.fetchPluginDetails(params)
      .subscribe(res => {
        this.props.action._backendProperties = res[0].properties;
        this.fetchWidgets(action);
      });
  }

  // Fetching Widget JSON for the plugin
  fetchWidgets(action) {
    let pluginsMap = AvailablePluginsStore.getState().plugins.pluginsMap;
    let {name, version, scope} = action.plugin.artifact;
    let actionNameType = ''.concat(action.plugin.name, '-').concat(action.plugin.type);
    let postRunActionPluginKey = ''.concat(actionNameType, '-').concat(name, '-').concat(version, '-').concat(scope);
    let pluginJson = pluginsMap[postRunActionPluginKey];
    let groupsConfig = generateNodeConfig(this.props.action._backendProperties, pluginJson.widgets);
    this.setState({
      groupsConfig
    });
  }

  getComponentFromWidgetType(type, name, attributes) {
    let Component;
    let props;
    let value = this.props.action.plugin.properties[name];
    let onChangeFn = () => {};
    switch (type) {
      case 'select':
        Component = SelectWithOptions;
        props = {
          value,
          options: attributes.values,
          className: 'form-control disabled'
        };
        break;
      case 'textbox':
        Component = 'input';
        props = {
          type: 'text',
          value,
          className: 'form-control'
        };
        break;
      case 'textarea':
        Component = 'input';
        props = {
          type: 'textarea',
          value,
          className: 'form-control'
        };
        break;
      case 'password':
        Component = 'input';
        props = {
          type: 'password',
          value,
          className: 'form-control'
        };
        break;
      case 'csv':
      case 'dsv': {
        Component = DSVEditor;
        let values = value.split(attributes.delimiter || ',');
        values = values.map(value => ({
          property: value,
          uniqueId: shortid.generate()
        }));
        props = {
          values,
          placeholder: attributes['value-placeholder'],
          onChange: onChangeFn
        };
        break;
      }
      case 'keyvalue': {
        Component = KeyValuePairs;
        let keyValuePairsMap = {};
        let keyValuePairs = value.split('\n');
        keyValuePairs.forEach(keyValuePair => {
          let keyAndValue = keyValuePair.split(':');
          keyValuePairsMap[keyAndValue[0]] = keyAndValue[1];
        });
        props = {
          keyValues: convertMapToKeyValuePairsObj(keyValuePairsMap),
          onKeyValueChange: onChangeFn
        };
        break;
      }

      default:
        Component = 'input';
        props = {
          type: 'text',
          className: 'form-control'
        };
    }
    return <Component {...props} />;
  }


  renderBody() {
    if (!this.state.groupsConfig.groups) {
      return null;
    }

    return (
      <fieldset disabled>
        <div className="confirm-step-content">
          {
            this.state.groupsConfig.groups.map(group => {
              return (
                <div>
                  <div className="widget-group-container">
                    {
                      group.fields.map(field => {
                        return (
                          <div>
                            <div className="form-group">
                              <label className="control-label">
                                <span>{field.label}</span>
                                <IconSVG
                                  name="icon-info-circle"
                                  id={`post-run-action-${field.name}`}
                                />
                                <UncontrolledTooltip
                                  target={`post-run-action-${field.name}`}
                                  delay={{show: 250, hide: 0}}
                                  placement="right"
                                >
                                  {field.description}
                                </UncontrolledTooltip>
                                { this.props.action._backendProperties[field.name].required ? <IconSVG name = "icon-asterisk" /> : null}
                              </label>
                              {this.getComponentFromWidgetType(field['widget-type'], field.name, field['widget-attributes'])}
                            </div>
                          </div>
                        );
                      })
                    }
                  </div>
                </div>
              );
            })
          }
        </div>
      </fieldset>
    );
  }

  render() {
    let action = this.props.action;

    return (
      <HydratorModal
        isOpen={this.props.isOpen}
        toggle={this.props.toggleModal}
        modalClassName="post-run-actions-modal hydrator-modal"
        backdrop='static'
        zIndex={1061}
      >
        {/* Not using <ModalHeader> here because it wraps the entire header in an h4 */}
        <div className="modal-header">
          <h4 className="modal-title float-xs-left">
            <span>{action.plugin.name || action.name}</span>
            <small className="plugin-version">
              {action.version || action.plugin.artifact.version}
            </small>
            <p>
              <small>{action.description}</small>
            </p>
          </h4>
          <div className="btn-group float-xs-right">
            <a
              className="btn"
              onClick={this.props.toggleModal}
            >
              <IconSVG name = "icon-close" />
            </a>
          </div>
        </div>
        <ModalBody>
          {this.renderBody()}
          <div className="btn btn-blue">
            Close
          </div>
        </ModalBody>
      </HydratorModal>
    );
  }
}
