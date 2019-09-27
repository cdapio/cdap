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
import React, { PureComponent } from 'react';
import LoadingSVG from 'components/LoadingSVG';
import Accordion, { AccordionContent, AccordionTitle, AccordionPane } from 'components/Accordion';
import { MyCloudApi } from 'api/cloud';
import uuidV4 from 'uuid/v4';
import { extractProfileName } from 'components/Cloud/Profiles/Store/ActionCreator';
import IconSVG from 'components/IconSVG';
import cloneDeep from 'lodash/cloneDeep';
import classnames from 'classnames';
import { WrappedWidgetWrapper } from 'components/ConfigurationGroup/WidgetWrapper';

require('./ProfileCustomizeContent.scss');

export default class ProfileCustomizeContent extends PureComponent {
  static propTypes = {
    profileName: PropTypes.string,
    profileLabel: PropTypes.string,
    customizations: PropTypes.object,
    provisioner: PropTypes.object,
    onSave: PropTypes.func,
    disabled: PropTypes.bool,
    onClose: PropTypes.func,
  };
  static defaultProps = {
    customizations: {},
    disabled: false,
  };

  state = {
    loading: true,
    provisionerspec: null,
  };

  customization = cloneDeep(this.props.customizations);

  componentDidMount() {
    MyCloudApi.getProvisionerDetailSpec({
      provisioner: this.props.provisioner.name,
    }).subscribe(
      (provisionerspec) => {
        this.setState({
          provisionerspec,
          loading: false,
        });
      },
      (err) => {
        this.setState({
          loading: false,
        });
        console.log('Failed to fetch provisioner spec ', err);
      }
    );
  }

  componentWillUnmount() {
    this.customization = {};
  }

  onPropertyUpdate = (propertyName, value) => {
    this.customization[propertyName] = value.toString();
  };

  onSave = () => {
    if (this.props.onSave) {
      this.props.onSave(this.customization);
    }
  };

  onClose = () => {
    if (this.props.onClose) {
      this.props.onClose(false);
    }
  };

  getProfilePropValue = (property) => {
    return this.customization[property.name] || property.value;
  };

  render() {
    if (this.state.loading) {
      return <LoadingSVG />;
    }
    let groups = this.state.provisionerspec['configuration-groups'];
    let editablePropertiesFromProfile = this.props.provisioner.properties.filter(
      (property) => property.isEditable !== false
    );

    const propertiesFromProfileMap = {};
    this.props.provisioner.properties.forEach((property) => {
      propertiesFromProfileMap[property.name] = property;
    });

    let editablePropertiesMap = {};
    editablePropertiesFromProfile.forEach((property) => {
      if (property.name in this.props.customizations) {
        editablePropertiesMap[property.name] = this.props.customizations[property.name];
      } else {
        editablePropertiesMap[property.name] = property.value;
      }
    });
    let profileName = this.props.profileLabel || extractProfileName(this.props.profileName);

    return (
      <div className="profile-customize-content">
        <div>
          <div className="profile-customize-metadata">
            <div className="profile-customize-name">
              <strong title={profileName}>{profileName}</strong>
              {editablePropertiesFromProfile.length ? (
                <small>Customize the values for the runs started by this schedule</small>
              ) : null}
            </div>
            <IconSVG name="icon-close" onClick={this.onClose} />
          </div>
          <Accordion size="small" active="0">
            {groups.map((group, i) => {
              const editableProperties = group.properties
                .filter(
                  (property) =>
                    !propertiesFromProfileMap[property.name] ||
                    propertiesFromProfileMap[property.name].isEditable !== false
                )
                .map((property) => ({
                  ...property,
                  value: editablePropertiesMap[property.name],
                }));
              return (
                <AccordionPane id={i}>
                  <AccordionTitle>
                    <strong>
                      {group.label} ({group.properties.length})
                    </strong>
                  </AccordionTitle>
                  <AccordionContent>
                    {editableProperties.map((property) => {
                      let uniqueId = `provisioner-${uuidV4()}`;
                      return (
                        <div key={uniqueId} className="profile-group-content">
                          <div>
                            <WrappedWidgetWrapper
                              pluginProperty={{
                                name: property.name,
                                description: property.description,
                                required: property.required,
                              }}
                              widgetProperty={property}
                              value={this.getProfilePropValue.bind(this, property)}
                              onChange={this.onPropertyUpdate.bind(this, property.name)}
                            />
                          </div>
                        </div>
                      );
                    })}
                    {!editableProperties.length ? (
                      <strong> Properties cannot be customized</strong>
                    ) : null}
                  </AccordionContent>
                </AccordionPane>
              );
            })}
          </Accordion>
        </div>
        {this.props.disabled ? null : (
          <div>
            {!editablePropertiesFromProfile.length ? (
              <small className="text-danger">
                Properties of this profile cannot be customized.
              </small>
            ) : null}
            <div
              className={classnames('btn btn-primary', {
                disabled: editablePropertiesFromProfile.length === 0,
              })}
              onClick={editablePropertiesFromProfile.length === 0 ? undefined : this.onSave}
            >
              Done
            </div>
          </div>
        )}
      </div>
    );
  }
}
