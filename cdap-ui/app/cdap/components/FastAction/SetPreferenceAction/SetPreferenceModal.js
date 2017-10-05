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

import PropTypes from 'prop-types';

import React, { Component } from 'react';
import isObject from 'lodash/isObject';
import upperFirst from 'lodash/upperFirst';
import orderBy from 'lodash/orderBy';
import isEmpty from 'lodash/isEmpty';
import T from 'i18n-react';
import shortid from 'shortid';
import { Modal, ModalHeader, ModalBody, Tooltip } from 'reactstrap';
import myPreferenceApi from 'api/preference';
import {convertProgramToApi} from 'services/program-api-converter';
import KeyValuePairs from 'components/KeyValuePairs';
import KeyValueStore from 'components/KeyValuePairs/KeyValueStore';
import KeyValueStoreActions from 'components/KeyValuePairs/KeyValueStoreActions';
import NamespaceStore from 'services/NamespaceStore';

export default class SetPreferenceModal extends Component {
  constructor(props) {
    super(props);

    this.state = {
      saving: false,
      showResetMessage: false,
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
    this.toggleTooltip = this.toggleTooltip.bind(this);
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
    this.setState({keyValues});
  }

  getSpecifiedPreferences() {
    this.apiSubscriptions.push(
      this.getSpecifiedPreferencesApi(this.params)
        .subscribe(
          (res) => {
            let keyValues;
            if (isEmpty(res)) {
              keyValues = {
                'pairs': [{
                    'key':'',
                    'value':'',
                    'uniqueId': shortid.generate()
                  }]
              };
            } else {
              keyValues = {'pairs': this.getKeyValPair(res)};
            }
            this.setState({
              keyValues
            });
            KeyValueStore.dispatch({
              type: KeyValueStoreActions.onUpdate,
              payload: {pairs: keyValues.pairs}
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
            if (this.props.onSuccess) {
              this.props.onSuccess();
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

  toggleTooltip() {
    this.setState({ tooltipOpen : !this.state.tooltipOpen });
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

  oneFieldMissing() {
    if (this.state.keyValues.pairs) {
      return this.state.keyValues.pairs.some((keyValuePair) => {
        let emptyKeyField = (keyValuePair.key.length === 0);
        let emptyValueField = (keyValuePair.value.length === 0);
        return (emptyKeyField && !emptyValueField) || (!emptyKeyField && emptyValueField);
      });
    }
    return false;
  }

  isTitleOverflowing() {
    let modalTitle = document.querySelector(".specify-preferences-description h4");
    if (modalTitle) {
      return (modalTitle.offsetWidth < modalTitle.scrollWidth);
    }
    return false;
  }

  resetFields(event) {
    event.preventDefault();
    this.setState({
      showResetMessage: true
    });
    this.getSpecifiedPreferences();
    setTimeout(() => {
      this.setState({
        showResetMessage: false
      });
    }, 3000);
  }

  preventPropagation(event) {
    event.stopPropagation();
    event.nativeEvent.stopImmediatePropagation();
  }

  renderSpecifyPreferences() {
    const actionLabel = T.translate('features.FastAction.setPreferencesActionLabel');
    let entity, entityWithType, description, tooltipID;
    if (this.setAtSystemLevel) {
      entityWithType = 'CDAP';
      description = T.translate('features.FastAction.setPreferencesDescriptionLabel.system');
      tooltipID = `${entityWithType}-title`;
    } else {
      entity = this.params.namespace;
      entityWithType = `Namespace "${entity}"`;
      description = T.translate('features.FastAction.setPreferencesDescriptionLabel.namespace');
      tooltipID = `${entity}-title`;
      if (this.props.entity) {
        entity = this.props.entity.id;
        entityWithType = `${upperFirst(this.props.entity.type)} "${entity}"`;
        tooltipID = `${this.props.entity.uniqueId}-title`;
        if (this.props.entity.type === 'application') {
          description = T.translate('features.FastAction.setPreferencesDescriptionLabel.app');
        } else {
          description = T.translate('features.FastAction.setPreferencesDescriptionLabel.program');
        }
      }
    }
    const title = `${actionLabel} for ${entityWithType}`;
    const keyLabel = T.translate('features.FastAction.setPreferencesColumnLabel.key');
    const valueLabel = T.translate('features.FastAction.setPreferencesColumnLabel.value');
    return (
      <div>
        {
          this.isTitleOverflowing() ?
            (
              <Tooltip
                placement="top"
                isOpen={this.state.tooltipOpen}
                target={tooltipID}
                toggle={this.toggleTooltip}
                className="entity-preferences-modal"
                delay={0}
              >
                {entity}
              </Tooltip>
            )
          :
            null
        }
        <div className='specify-preferences-description'>
          <h4 id={tooltipID}>{title}</h4>
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
        backdrop='static'
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
                      className="btn btn-primary float-xs-left saving"
                      disabled="disabled"
                    >
                      <span className="fa fa-spinner fa-spin" />
                      <span>{savingLabel}</span>
                    </button>
                  :
                    <button
                      className="btn btn-primary float-xs-left not-saving"
                      onClick={this.setPreferences}
                      disabled={(this.oneFieldMissing() || this.state.error) ? 'disabled' : null}
                    >
                      <span>{saveAndCloseLabel}</span>
                    </button>
                }
                <span className="float-xs-left reset">
                  <a onClick = {this.resetFields}>{resetLink}</a>
                </span>
                {
                  this.state.showResetMessage ?
                    (
                      <span className="float-xs-left text-success reset-success">
                        Reset Successful
                      </span>
                    )
                  :
                    null
                }
                {
                  this.state.keyValues.pairs ?
                    (
                      <span className="float-xs-right num-rows">
                        {
                          this.state.keyValues.pairs.length === 1 ?
                            <span>{this.state.keyValues.pairs.length} row</span>
                          :
                            <span>{this.state.keyValues.pairs.length} rows</span>
                        }
                      </span>
                    )
                  :
                    null

                }
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
  onSuccess: PropTypes.func
};

SetPreferenceModal.defaultProps = {
  setAtNamespaceLevel: false,
  setAtSystemLevel: false,
  onSuccess: () => {}
};
