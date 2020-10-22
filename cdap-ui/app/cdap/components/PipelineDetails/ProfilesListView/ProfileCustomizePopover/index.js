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
import Popover from '@material-ui/core/Popover';
import ProfileCustomizeContent from 'components/PipelineDetails/ProfilesListView/ProfileCustomizePopover/ProfileCustomizeContent';
import { getProfileNameWithScope } from 'components/Cloud/Profiles/Store/ActionCreator';
import { withStyles } from '@material-ui/core/styles';
require('./ProfileCustomizePopover.scss');

const CustomizedPopover = withStyles({
  paper: {
    padding: '20px',
  },
})(Popover);

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

  onTogglePopover = (e) => {
    this.setState({
      showPopover: !this.state.showPopover,
      anchorEl: e.currentTarget,
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
    return (
      <div className="profile-customize-popover">
        <div className="btn-link" onClick={this.onTogglePopover}>
          Customize
        </div>
        <CustomizedPopover
          open={this.state.showPopover}
          anchorEl={this.state.anchorEl}
          onClose={this.onTogglePopover}
          anchorOrigin={{
            vertical: 'center',
            horizontal: 'left',
          }}
          transformOrigin={{
            vertical: 'center',
            horizontal: 'right',
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
        </CustomizedPopover>
      </div>
    );
  }
}
