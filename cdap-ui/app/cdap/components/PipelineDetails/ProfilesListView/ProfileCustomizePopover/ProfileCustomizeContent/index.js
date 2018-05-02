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
import React, {PureComponent} from 'react';
import LoadingSVG from 'components/LoadingSVG';
import Accordion, {AccordionContent, AccordionTitle, AccordionPane} from 'components/Accordion';
import {MyCloudApi} from 'api/cloud';
import AbstractWidget from 'components/AbstractWidget';
import uuidV4 from 'uuid/v4';
import {extractProfileName} from 'components/PipelineDetails/ProfilesListView';
import IconSVG from 'components/IconSVG';
import cloneDeep from 'lodash/cloneDeep';

require('./ProfileCustomizeContent.scss');

export default class ProfileCustomizeContent extends PureComponent {
  static propTypes = {
    profileName: PropTypes.string,
    customizations: PropTypes.object,
    provisioner: PropTypes.object,
    onSave: PropTypes.func,
    disabled: PropTypes.bool,
    onClose: PropTypes.func
  };
  static defaultProps = {
    customizations: {},
    disabled: false
  };

  state = {
    loading: true,
    provisionerspec: null
  };

  customization = cloneDeep(this.props.customizations);

  componentDidMount() {
    MyCloudApi.getProvisionerDetailSpec({
      provisioner: this.props.provisioner.name
    })
    .subscribe(
      provisionerspec => {
        this.setState({
          provisionerspec,
          loading: false
        });
      },
      err => {
        this.setState({
          loading: false
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

  render() {
    if (this.state.loading) {
      return <LoadingSVG />;
    }
    let groups = this.state.provisionerspec['configuration-groups'];
    let editablePropertiesFromProfile = this.props.provisioner.properties.filter(property => property.isEditable);
    let editablePropertiesMap = {};
    editablePropertiesFromProfile.forEach(property => {
      if (property.name in this.props.customizations) {
        editablePropertiesMap[property.name] = this.props.customizations[property.name];
      } else {
        editablePropertiesMap[property.name] = property.value;
      }
    });

    return (
      <div className="profile-customize-content">
        <div>
          <div className="profile-customize-metadata">
            <div>
              <div>
                <strong>{extractProfileName(this.props.profileName)}</strong>
              </div>
              <small>Customize the values for the runs started by this schedule</small>
            </div>
            <IconSVG
              name="icon-close"
              onClick={this.onClose}
            />
          </div>
          <Accordion size="small" active="0">
            {
              groups.map((group, i) => {
                let editableProperties = group.properties
                  .filter(property => property.name in editablePropertiesMap)
                  .map(property => ({
                    ...property,
                    value: editablePropertiesMap[property.name]
                  }));
                return (
                  <AccordionPane id={i}>
                    <AccordionTitle>
                      <strong>{group.label} ({group.properties.length})</strong>
                    </AccordionTitle>
                    <AccordionContent>
                      {
                        editableProperties.map(property => {
                          let uniqueId = `provisioner-${uuidV4()}`;
                          return (
                            <div key={uniqueId} className="profile-group-content">
                              <div>
                                <strong
                                  id={uniqueId}
                                >
                                  {property.label}
                                </strong>
                              </div>
                              <div>
                                <AbstractWidget
                                  type={property['widget-type']}
                                  value={this.customization[property.name] || property.value}
                                  onChange={this.onPropertyUpdate.bind(this, property.name)}
                                  widgetProps={property['widget-attributes']}
                                />
                              </div>
                            </div>
                          );
                        })
                      }
                      {
                        !editableProperties.length ?
                          <strong> No properties available to customize</strong>
                        :
                          null
                      }
                    </AccordionContent>
                  </AccordionPane>
                );
              })
            }
          </Accordion>
        </div>
        {
          this.props.disabled ?
            null
          :
            <div
              className="btn btn-primary"
              onClick={this.onSave}
            >
              Done
            </div>
        }
      </div>
    );
  }
}
