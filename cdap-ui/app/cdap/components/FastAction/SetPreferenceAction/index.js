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
import FastActionButton from '../FastActionButton';
import isObject from 'lodash/isObject';
import upperFirst from 'lodash/upperFirst';
import debounce from 'lodash/debounce';
import orderBy from 'lodash/orderBy';
import T from 'i18n-react';
import shortid from 'shortid';
import { Modal, Tooltip, ModalHeader, ModalBody } from 'reactstrap';
import myPreferenceApi from 'api/preference';
import {convertProgramToApi} from 'services/program-api-converter';
import KeyValuePairs from 'components/KeyValuePairs';
import NamespaceStore from 'services/NamespaceStore';
require('./SetPreferenceAction.less');

export default class SetPreferenceAction extends Component {
  constructor(props) {
    super(props);

    this.state = {
      modal: false,
      savedMessage: null,
      keyValues: {},
      inheritedPreferences: [],
      sortByAttribute: 'key',
      sortOrder: 'asc',
    };

    this.params = {
      namespace: NamespaceStore.getState().selectedNamespace,
      appId: this.props.entity.id,
    };

    if (this.props.entity.type == 'program') {
      this.params.appId = this.props.entity.applicationId;
      this.params.programId = this.props.entity.id;
      this.params.programType = convertProgramToApi(this.props.entity.programType);
    }

    this.debouncedSetPreferences = debounce(this.debounceSetPreferences, 1000);
    this.eventText = '';
    this.toggleModal = this.toggleModal.bind(this);
    this.onKeyValueChange = this.onKeyValueChange.bind(this);
    this.toggleTooltip = this.toggleTooltip.bind(this);

  }

  toggleTooltip() {
    this.setState({ tooltipOpen : !this.state.tooltipOpen });
  }

  toggleModal() {
    this.setState({
      modal: !this.state.modal,
      savedMessage: null
    });
  }

  getSpecifiedPreferences() {
    let getSpecifiedPreferencesApi = myPreferenceApi.getAppPreferences;
    if (this.props.entity.type === 'program') {
      getSpecifiedPreferencesApi = myPreferenceApi.getProgramPreferences;
    }
    getSpecifiedPreferencesApi(this.params)
    .subscribe(
      (res) => {
        if (!Object.keys(res).length) {
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
          this.setState({keyValues: {'pairs': this.getKeyValPair(res)}});
        }
      },
      (error) => {
        this.setState({
          error: isObject(error) ? error.response : error
        });
      }
    );
  }

  getInheritedPreferences() {
    let getInheritedPreferencesApi = myPreferenceApi.getNamespacePreferencesResolved;
    if (this.props.entity.type === 'program') {
      getInheritedPreferencesApi = myPreferenceApi.getAppPreferencesResolved;
    }
    getInheritedPreferencesApi(this.params)
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
    );
  }

  setPreferences() {
    let setPreferencesApi = myPreferenceApi.setAppPreferences;
    if (this.props.entity.type === 'program') {
      setPreferencesApi = myPreferenceApi.setProgramPreferences;
    }
    setPreferencesApi(this.params, this.getKeyValObject())
    .subscribe(
      () => {
        this.setState({savedMessage: 'Saved'});
        setTimeout(() => {
          this.setState({savedMessage: null});
        }, 3000);
      },
      (error) => {
        this.setState({
          error: isObject(error) ? error.response : error
        });
      }
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

  onKeyValueChange(keyValues) {
    this.setState({keyValues});
    if (this.allFieldsFilled()) {
      this.debouncedSetPreferences();
    }
  }

  allFieldsFilled() {
    return this.state.keyValues.pairs.every((keyValuePair) => {
      return (keyValuePair.key.length > 0 && keyValuePair.value.length > 0);
    });
  }

  debounceSetPreferences() {
    this.setState({savedMessage: 'Saving...'});
    this.setPreferences();
  }

  preventPropagation(event) {
    event.stopPropagation();
    event.nativeEvent.stopImmediatePropagation();
  }

  componentWillMount() {
    this.getSpecifiedPreferences();
    this.getInheritedPreferences();
  }

  renderSpecifyPreferences() {
    const actionLabel = T.translate('features.FastAction.setPreferencesActionLabel');
    const entityType = upperFirst(this.props.entity.type);
    const title = `${actionLabel} for ${entityType} ${this.props.entity.id}`;
    const keyLabel = T.translate('features.FastAction.setPreferencesColumnLabel.key');
    const valueLabel = T.translate('features.FastAction.setPreferencesColumnLabel.value');
    let description = T.translate('features.FastAction.setPreferencesAppDescriptionLabel');
    if (entityType === 'Program') {
      description = T.translate('features.FastAction.setPreferencesProgramDescriptionLabel');
    }
    return (
      <div>
        <div className='specify-preferences-description'>
          <h4>{title}</h4>
          <p>{description}</p>
        </div>
        <div className='specify-preferences-list'>
          <div className='specify-preferences-labels'>
            <span className='key-label'>{keyLabel}</span>
            <span className='value-label'>{valueLabel}</span>
          </div>
          <div className='specify-preferences-values'>
            <KeyValuePairs
              keyValues = {this.state.keyValues}
              onKeyValueChange = {this.onKeyValueChange}
            />
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
          className='toggleable-columns'
          onClick={this.toggleSorted.bind(this, columnToAttribute)}
        >
          {
            this.state.sortByAttribute === columnToAttribute ?
              <span>
                <span className='text-underline'>{column}</span>
                <span>
                  {
                    this.state.sortOrder === 'asc' ?
                      <i className='fa fa-caret-down fa-lg'></i>
                    :
                      <i className='fa fa-caret-up fa-lg'></i>
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
    const keyLabel = T.translate('features.FastAction.setPreferencesColumnLabel.key');
    const valueLabel = T.translate('features.FastAction.setPreferencesColumnLabel.value');
    const originLabel = T.translate('features.FastAction.setPreferencesColumnLabel.origin');
    const statusLabel = T.translate('features.FastAction.setPreferencesColumnLabel.status');
    return (
      <div>
        <div className='inherited-preferences-label'>
          <h4>{T.translate('features.FastAction.setPreferencesInheritedPrefsLabel')}</h4>
        </div>
        <div className='inherited-preferences-list'>
        {
          this.state.inheritedPreferences.length ?
            <div>
              <table>
                <thead>
                  <tr>
                    {this.renderInheritedPreferencesColumnHeader(keyLabel)}
                    {this.renderInheritedPreferencesColumnHeader(valueLabel)}
                    <th>{originLabel}</th>
                    <th>{statusLabel}</th>
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
                            <td><i className='fa fa-check fa-lg'></i></td>
                          </tr>
                        );
                      })
                  }
                </tbody>
              </table>
            </div>
          :
            <div className="text-center">
              No Inherited Preferences
            </div>
        }
        </div>
      </div>
    );
  }

  render() {
    let tooltipID = `${this.props.entity.uniqueId}-setpreferences`;
    return (
      <span>
        <FastActionButton
          icon="fa fa-wrench"
          action={this.toggleModal}
          id={tooltipID}
        />
        <Tooltip
          placement="top"
          isOpen={this.state.tooltipOpen}
          target={tooltipID}
          toggle={this.toggleTooltip}
          delay={0}
        >
          {T.translate('features.FastAction.setPreferencesActionLabel')}
        </Tooltip>

        {
          this.state.modal ? (
            <Modal
              isOpen={this.state.modal}
              toggle={this.toggleModal}
              className="confirmation-modal set-preference-modal"
              size="lg"
            >
              <ModalHeader
                className="modal-header"
                onClick={this.preventPropagation.bind(this)}
              >
                <div className="pull-left">
                  <span
                    className={"button-icon fa fa-wrench"}
                  />
                  <span className={"button-icon title"}>
                    {T.translate('features.FastAction.setPreferencesModalLabel')}
                  </span>
                </div>
                <div className="pull-right">
                  <div className="close-modal-btn"
                    onClick={this.toggleModal.bind(this)}
                  >
                    <span
                      className={"button-icon fa fa-times"}
                    />
                  </div>
                </div>
              </ModalHeader>
              <ModalBody
                className="modal-body"
                onClick={this.preventPropagation.bind(this)}
              >
                <div className="preferences-container">
                  <div className="specify-preferences-container">
                    {this.renderSpecifyPreferences()}
                    <div className="clearfix">
                      <button
                        className="btn btn-primary pull-left"
                        onClick={() => {this.toggleModal(); this.setPreferences();}}
                      >
                        <span>{T.translate('features.FastAction.setPreferencesButtonLabel')}</span>
                      </button>
                      {
                        this.state.error ?
                          <span className="pull-left text-danger">{this.state.error}</span>
                        :
                          null
                      }
                      {
                        this.state.savedMessage ?
                          <span className="pull-left text-success">{this.state.savedMessage}</span>
                        :
                          null
                      }
                    </div>
                  </div>
                  <hr />
                  <div className="inherited-preferences-container">
                    {this.renderInheritedPreferences()}
                  </div>
                </div>
              </ModalBody>
            </Modal>
          ) : null
        }
      </span>
    );
  }
}

SetPreferenceAction.propTypes = {
  entity: PropTypes.shape({
    id: PropTypes.string.isRequired,
    applicationId: PropTypes.string,
    uniqueId: PropTypes.string,
    type: PropTypes.oneOf(['application', 'program']).isRequired,
    programType: PropTypes.string
  }),
};
