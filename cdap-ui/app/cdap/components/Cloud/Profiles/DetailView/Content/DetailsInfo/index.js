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
import T from 'i18n-react';
import Popover from 'components/Popover';
import { SECURE_KEY_PREFIX, SECURE_KEY_SUFFIX } from 'services/global-constants';

require('./DetailsInfo.scss');

const PREFIX = 'features.Cloud.Profiles.DetailView';

export default class ProfileDetailViewDetailsInfo extends Component {
  state = {
    viewDetails: false,
  };

  static propTypes = {
    profile: PropTypes.object,
    provisioners: PropTypes.array,
  };

  toggleViewDetails = () => {
    this.setState({
      viewDetails: !this.state.viewDetails,
    });
  };

  renderViewDetailsLabel() {
    return (
      <span className="view-details-label" onClick={this.toggleViewDetails}>
        <IconSVG name={this.state.viewDetails ? 'icon-caret-down' : 'icon-caret-right'} />
        <span>
          {this.state.viewDetails
            ? T.translate(`${PREFIX}.hideDetails`)
            : T.translate(`${PREFIX}.viewDetails`)}
        </span>
      </span>
    );
  }

  getSecureKeyValue(value) {
    const beginIndex = value.indexOf(SECURE_KEY_PREFIX);
    const endIndex = value.lastIndexOf(SECURE_KEY_SUFFIX);

    if (beginIndex === -1 || endIndex === -1) {
      return value;
    }

    const stringLength = endIndex - beginIndex;
    const secureKey = value.slice(SECURE_KEY_PREFIX.length, stringLength);

    return (
      <span className="secure-key">
        {secureKey}

        <Popover
          target={() => <IconSVG name="icon-shield" />}
          targetDimension={{
            width: 16,
            height: 21,
          }}
          placement="top"
          showOn="Hover"
        >
          {T.translate(`${PREFIX}.secureKeyPopover`)}
        </Popover>
      </span>
    );
  }

  convertValueBasedOnWidget = (widgetType, value) => {
    // Since memory-textbox is specifically used for RAM this conversion is safe.
    if (widgetType == 'memory-textbox') {
      let numberValue = parseInt(value, 10);
      if (isNaN(numberValue)) {
        return value;
      }
      return Math.floor(numberValue / 1024);
    }
    return value;
  };

  renderDetailsTable(profile) {
    if (!profile.provisioner.properties.length) {
      return <span>{T.translate(`${PREFIX}.noProperties`)}</span>;
    }

    const propertyToLabelWidgetMap = {};
    this.props.provisioners.forEach((provisioner) => {
      if (provisioner.name === this.props.profile.provisioner.name) {
        provisioner['configuration-groups'].forEach((provisionerGroup) => {
          provisionerGroup.properties.forEach((prop) => {
            propertyToLabelWidgetMap[prop.name] = {
              label: prop.label,
              widget: prop['widget-type'],
            };
          });
        });
      }
    });

    return (
      <div className="details-table">
        {profile.provisioner.properties.map((property) => {
          let propertyLabel = propertyToLabelWidgetMap[property.name].label || property.name;
          const widgetType = propertyToLabelWidgetMap[property.name].widget;
          let value = this.convertValueBasedOnWidget(widgetType, property.value);

          if (property.name === 'accountKey') {
            value = this.getSecureKeyValue(value);
          }

          return (
            <div className="details-row" key={property.name}>
              <strong className="label-holder" title={propertyLabel}>
                {`${propertyLabel}:`}
              </strong>
              <span className="value-holder" title={property.value}>
                {value}
              </span>
            </div>
          );
        })}
      </div>
    );
  }

  renderDetailsContent() {
    if (!this.state.viewDetails) {
      return null;
    }

    return (
      <div className="details-content">
        <h5>
          <strong>{T.translate(`${PREFIX}.profileDetails`)}</strong>
        </h5>
        {this.renderDetailsTable(this.props.profile)}
      </div>
    );
  }

  render() {
    return (
      <div className="detail-view-details-info">
        {this.renderViewDetailsLabel()}
        {this.renderDetailsContent()}
      </div>
    );
  }
}
