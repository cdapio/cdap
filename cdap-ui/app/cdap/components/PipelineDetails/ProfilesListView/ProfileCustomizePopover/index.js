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
import Popover from 'components/Popover';
import ProfileCustomizeContent from 'components/PipelineDetails/ProfilesListView/ProfileCustomizePopover/ProfileCustomizeContent';
import { getProfileNameWithScope } from 'components/Cloud/Profiles/Store/ActionCreator';
require('./ProfileCustomizePopover.scss');

export default class ProfileCustomizePopover extends PureComponent {
  static propTypes = {
    profile: PropTypes.object,
    onProfileSelect: PropTypes.func,
    customizations: PropTypes.object,
    disabled: PropTypes.bool,
  };

  static defaultProps = {
    customizations: {},
  };

  state = {
    showPopover: false,
  };

  onTogglePopover = (showPopover) => {
    this.setState({
      showPopover,
    });
  };

  onProfileSelect = (profileName, customizations) => {
    if (this.props.onProfileSelect) {
      this.props.onProfileSelect(profileName, customizations);
    }
    this.onTogglePopover(false);
  };

  render() {
    let { name, provisioner, scope, label: profileLabel } = this.props.profile;
    let profileName = getProfileNameWithScope(name, scope);
    let customizeLink = () => <div className="btn-link">Customize</div>;
    return (
      <Popover
        target={customizeLink}
        placement="left"
        enableInteractionInPopover={true}
        injectOnToggle={true}
        className="profile-customize-popover"
        bubbleEvent={false}
        showPopover={this.state.showPopover}
        onTogglePopover={this.onTogglePopover}
        modifiers={{
          offset: {
            enabled: true,
            offset: '70,0',
          },
        }}
      >
        <ProfileCustomizeContent
          profileName={profileName}
          profileLabel={profileLabel}
          customizations={this.props.customizations}
          provisioner={provisioner}
          onSave={this.onProfileSelect.bind(this, profileName)}
          disabled={this.props.disabled}
          onClose={this.onTogglePopover}
        />
      </Popover>
    );
  }
}
