/*
 * Copyright © 2017 Cask Data, Inc.
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

import React, {Component, PropTypes} from 'react';
import isObject from 'lodash/isObject';
import upperFirst from 'lodash/upperFirst';
import orderBy from 'lodash/orderBy';
import isEmpty from 'lodash/isEmpty';
import T from 'i18n-react';
import shortid from 'shortid';
import { Modal, ModalHeader, ModalBody } from 'reactstrap';
import myPreferenceApi from 'api/preference';
import {convertProgramToApi} from 'services/program-api-converter';
import KeyValuePairs from 'components/KeyValuePairs';
import NamespaceStore from 'services/NamespaceStore';

export default class SetPreferenceModal extends Component {
  constructor(props) {
    super(props);

    this.state = {
      saving: false,
      fieldsResetted: false,
      keyValues: {},
      inheritedPreferences: [],
      sortByAttribute: 'key',
      sortOrder: 'asc'
    };

    let namespace = NamespaceStore.getState().selectedNamespace;

    this.params = {};
    this.setAtSystemLevel = this.props.setAtSystemLevel;

    if (this.setAtSystemLevel) {
      this.getSpecifiedPreferencesApi = myPreferenceApi.getSystemPreferences;
      this.setPreferencesApi = myPreferenceApi.setSystemPreferences;
    } else {
      this.params.namespace = namespace;
      this.getSpecifiedPreferencesApi = myPreferenceApi.getNamespacePreferences;
      this.getInheritedPreferencesApi = myPreferenceApi.getSystemPreferences;
      this.setPreferencesApi = myPreferenceApi.setNamespacePreferences;

      if (this.props.entity) {
        if (this.props.entity.type === 'application') {
          this.params.appId = this.props.entity.id;
          this.getSpecifiedPreferencesApi = myPreferenceApi.getAppPreferences;
          this.getInheritedPreferencesApi = myPreferenceApi.getNamespacePreferencesResolved;
          this.setPreferencesApi = myPreferenceApi.setAppPreferences;
        } else {
          this.params.appId = this.props.entity.applicationId;
          this.params.programId = this.props.entity.id;
          this.params.programType = convertProgramToApi(this.props.entity.programType);
          this.getSpecifiedPreferencesApi = myPreferenceApi.getProgramPreferences;
          this.getInheritedPreferencesApi = myPreferenceApi.getAppPreferencesResolved;
          this.setPreferencesApi = myPreferenceApi.setProgramPreferences;
        }
      }

      this.subscription = NamespaceStore.subscribe(() => {
        this.params.namespace = NamespaceStore.getState().selectedNamespace;
        this.getSpecifiedPreferences();
        this.getInheritedPreferences();
      });
    }

    this.apiSubscriptions = [];
    this.onKeyValueChange = this.onKeyValueChange.bind(this);
    this.preventPropagation = this.preventPropagation.bind(this);
    this.setPreferences = this.setPreferences.bind(this);
    this.resetFields = this.resetFields.bind(this);
  }

  componentWillMount() {
    this.getSpecifiedPreferences();
    this.getInheritedPreferences();
  }

  componentWillUnmount() {
    if (this.subscription) {
      this.subscription();
    }
    this.apiSubscriptions.forEach(apiSubscription => apiSubscription.dispose());
  }

  onKeyValueChange(keyValues) {
    if (!this.state.fieldsResetted) {
      this.setState({keyValues});
    }
    else {
      this.setState({fieldsResetted: false});
    }
  }

  getSpecifiedPreferences() {
    this.apiSubscriptions.push(
      this.getSpecifiedPreferencesApi(this.params)
        .subscribe(
          (res) => {
            if (isEmpty(res)) {
              this.setState({
                keyValues: {
                  'pairs': [{
                    'key':'',
                    'value':'',
                    'uniqueId': shortid.generate()
                  }]
                }
              });
            } else {
              this.setState({
                keyValues: {'pairs': this.getKeyValPair(res)},
                fieldsResetted: true
              });
            }
          },
          (error) => {
            this.setState({
              error: isObject(error) ? error.response : error
            });
          }
        )
    );
  }

  getInheritedPreferences() {
    if (!this.getInheritedPreferencesApi) {
      return;
    }
    this.apiSubscriptions.push(
      this.getInheritedPreferencesApi(this.params)
        .subscribe(
          (res) => {
            let resolvedPrefArray = this.getKeyValPair(res);
            resolvedPrefArray = orderBy(resolvedPrefArray, [this.state.sortByAttribute], [this.state.sortOrder]);
            this.setState({
             inheritedPreferences: resolvedPrefArray
            });
          },
          (error) => {
            this.setState({
             error: isObject(error) ? error.response : error
            });
          }
        )
    );
  }

  setPreferences() {
    this.setState({saving: true});
    this.apiSubscriptions.push(
      this.setPreferencesApi(this.params, this.getKeyValObject())
        .subscribe(
          () => {
            if (this.props.onPreferencesSaved) {
              this.props.onPreferencesSaved();
            }
            this.props.toggleModal();
            this.setState({saving: false});
          },
          (error) => {
            this.setState({
              error: isObject(error) ? error.response : error
            });
          }
        )
    );
  }

  getKeyValPair(prefObj) {
    let prefArray = [];
    for (let key in prefObj) {
      if (prefObj.hasOwnProperty(key)) {
        prefArray.push({
          key: key,
          value: prefObj[key],
          uniqueId: shortid.generate()
        });
      }
    }
    return prefArray;
  }

  getKeyValObject() {
    let keyValArr = this.state.keyValues.pairs;
    let keyValObj = {};
    keyValArr.forEach((pair) => {
      if (pair.key.length > 0 && pair.value.length > 0) {
        keyValObj[pair.key] = pair.value;
      }
    });
    return keyValObj;
  }

  toggleSorted(attribute) {
    let sortOrder = 'asc';
    if (this.state.sortByAttribute != attribute) {
      this.setState({sortOrder});
    } else {
      if (this.state.sortOrder === 'asc') {
        sortOrder = 'desc';
      }
      this.setState({sortOrder});
    }
    this.setState({sortByAttribute: attribute});
    let newInheritedPreferences = orderBy(this.state.inheritedPreferences, attribute, sortOrder);
    this.setState({inheritedPreferences: newInheritedPreferences});
  }

  allFieldsFilled() {
    if (this.state.keyValues.pairs) {
      return this.state.keyValues.pairs.every((keyValuePair) => {
        return (keyValuePair.key.length > 0 && keyValuePair.value.length > 0);
      });
    }
    return true;
  }

  resetFields(event) {
    event.preventDefault();
    this.getSpecifiedPreferences();
  }

  preventPropagation(event) {
    event.stopPropagation();
    event.nativeEvent.stopImmediatePropagation();
  }

  renderSpecifyPreferences() {
    const actionLabel = T.translate('features.FastAction.setPreferencesActionLabel');
    let entity, description;
    if (this.setAtSystemLevel) {
      entity = 'CDAP';
      description = T.translate('features.FastAction.setPreferencesDescriptionLabel.system');
    } else {
      entity = `Namespace ${this.params.namespace}`;
      description = T.translate('features.FastAction.setPreferencesDescriptionLabel.namespace');
      if (this.props.entity) {
        entity = `${upperFirst(this.props.entity.type)} ${this.props.entity.id}`;
        if (this.props.entity.type === 'application') {
          description = T.translate('features.FastAction.setPreferencesDescriptionLabel.app');
        } else {
          description = T.translate('features.FastAction.setPreferencesDescriptionLabel.program');
        }
      }
    }
    const title = `${actionLabel} for ${entity}`;
    const keyLabel = T.translate('features.FastAction.setPreferencesColumnLabel.key');
    const valueLabel = T.translate('features.FastAction.setPreferencesColumnLabel.value');
    return (
      <div>
        <div className="specify-preferences-description">
          <h4>{title}</h4>
          <p>{description}</p>
        </div>
        <div className="specify-preferences-list">
          <div className="specify-preferences-labels">
            <span className="key-label">{keyLabel}</span>
            <span className="value-label">{valueLabel}</span>
          </div>
          <div className="specify-preferences-values">
            {
              !isEmpty(this.state.keyValues) ?
                (
                  <KeyValuePairs
                    keyValues = {this.state.keyValues}
                    onKeyValueChange = {this.onKeyValueChange}
                    fieldsResetted = {this.state.fieldsResetted}
                  />
                )
              :
                (
                  <span className = "fa fa-spinner fa-spin" />
                )
            }
          </div>
        </div>
      </div>
    );
  }

  renderInheritedPreferencesColumnHeader(column) {
    let columnToAttribute = column.toLowerCase();
    return (
      <th>
        <span
          className="toggleable-columns"
          onClick={this.toggleSorted.bind(this, columnToAttribute)}
        >
          {
            this.state.sortByAttribute === columnToAttribute ?
              <span>
                <span className="text-underline">{column}</span>
                <span>
                  {
                    this.state.sortOrder === 'asc' ?
                      <i className="fa fa-caret-down fa-lg"></i>
                    :
                      <i className="fa fa-caret-up fa-lg"></i>
                  }
                </span>
              </span>
            :
              <span>{column}</span>
          }
        </span>
      </th>
    );
  }

  renderInheritedPreferences() {
    if (!this.getInheritedPreferencesApi) {
      return null;
    }
    const titleLabel = T.translate('features.FastAction.setPreferencesInheritedPrefsLabel');
    const keyLabel = T.translate('features.FastAction.setPreferencesColumnLabel.key');
    const valueLabel = T.translate('features.FastAction.setPreferencesColumnLabel.value');
    const originLabel = T.translate('features.FastAction.setPreferencesColumnLabel.origin');
    let numInheritedPreferences = this.state.inheritedPreferences.length;
    return (
      <div>
        <div className="inherited-preferences-label">
          <h4>{titleLabel} ({numInheritedPreferences})</h4>
        </div>
        <div className="inherited-preferences-list">
        {
           numInheritedPreferences ?
            <div>
              <table>
                <thead>
                  <tr>
                    {this.renderInheritedPreferencesColumnHeader(keyLabel)}
                    {this.renderInheritedPreferencesColumnHeader(valueLabel)}
                    <th>{originLabel}</th>
                  </tr>
                </thead>
                <tbody>
                  {
                    this.state
                      .inheritedPreferences
                      .map((inheritedPreference, index) => {
                        return (
                          <tr className="inherited-preference" key={index}>
                            <td>{inheritedPreference.key}</td>
                            <td>{inheritedPreference.value}</td>
                            <td>N/A</td>
                          </tr>
                        );
                      })
                  }
                </tbody>
              </table>
            </div>
          :
            <div className="text-xs-center">
              No Inherited Preferences
            </div>
        }
        </div>
      </div>
    );
  }

  render() {
    const modalLabel = T.translate('features.FastAction.setPreferencesModalLabel');
    const savingLabel = T.translate('features.FastAction.setPreferencesButtonLabel.saving');
    const saveAndCloseLabel = T.translate('features.FastAction.setPreferencesButtonLabel.saveAndClose');
    const resetLink = T.translate('features.FastAction.setPreferencesReset');
    return (
      <Modal
        isOpen={this.props.isOpen}
        toggle={this.props.toggleModal}
        className="confirmation-modal set-preference-modal"
        size="lg"
      >
        <ModalHeader
          className="modal-header"
          onClick={this.preventPropagation}
        >
          <div className="float-xs-left">
            <span className="button-icon fa fa-wrench" />
            <span className="button-icon title">
              {modalLabel}
            </span>
          </div>
          <div className="float-xs-right">
            <div
              className="close-modal-btn"
              onClick={this.props.toggleModal.bind(this)}
            >
              <span className="button-icon fa fa-times" />
            </div>
          </div>
        </ModalHeader>
        <ModalBody
          className="modal-body"
          onClick={this.preventPropagation}
        >
          <div className="preferences-container">
            <div className="specify-preferences-container">
              {this.renderSpecifyPreferences()}
              <div className="clearfix">
                {
                  this.state.saving ?
                    <button
                      className="btn btn-primary float-xs-left"
                      disabled="disabled"
                    >
                      <span className="fa fa-spinner fa-spin" />
                      <span>{savingLabel}</span>
                    </button>
                  :
                    <button
                      className="btn btn-primary float-xs-left"
                      onClick={this.setPreferences}
                      disabled={(!this.allFieldsFilled() || this.state.error) ? 'disabled' : null}
                    >
                      <span>{saveAndCloseLabel}</span>
                    </button>
                }
                <span className="float-xs-left reset">
                  <a onClick = {this.resetFields}>{resetLink}</a>
                </span>
              </div>
              <div className="preferences-error">
                {
                  this.state.error ?
                    <div className="bg-danger text-white">{this.state.error}</div>
                  :
                    null
                }
              </div>
            </div>
            {
              !this.setAtSystemLevel ?
                <hr />
              :
                null
            }
            <div className="inherited-preferences-container">
              {this.renderInheritedPreferences()}
            </div>
          </div>
        </ModalBody>
      </Modal>
    );
  }
}

SetPreferenceModal.propTypes = {
  entity: PropTypes.shape({
    id: PropTypes.string.isRequired,
    applicationId: PropTypes.string,
    uniqueId: PropTypes.string,
    type: PropTypes.oneOf(['application', 'program']).isRequired,
    programType: PropTypes.string
  }),
  isOpen: PropTypes.func.isRequired,
  toggleModal: PropTypes.func.isRequired,
  setAtNamespaceLevel: PropTypes.bool,
  setAtSystemLevel: PropTypes.bool,
  onPreferencesSaved: PropTypes.func
};

SetPreferenceModal.defaultProps = {
  setAtNamespaceLevel: false,
  setAtSystemLevel: false,
  onPreferencesSaved: () => {}
};
