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

/*
  This is a direct copy paste from reactstrap's source. There is only one reason to do this.
  We can't upgrade reactstrap to 4.* yet as it has upgraded to bootstrap that defaults to flexbox
  now and we can't make that change at this point in the release. So this artifact stays here until we have
  upgraded reactstrap.
*/
import React, { PureComponent } from 'react';
import {Tooltip, Dropdown} from 'reactstrap';

const components = {
  UncontrolledDropdown: Dropdown,
  UncontrolledTooltip: Tooltip
};

Object.keys(components).forEach(key => {
  const Tag = components[key];
  const defaultValue = false;

  class Uncontrolled extends PureComponent {
    constructor(props) {
      super(props);

      this.state = { isOpen: defaultValue };

      this.toggle = this.toggle.bind(this);
    }

    toggle() {
      this.setState({ isOpen: !this.state.isOpen });
    }

    render() {
      return <Tag isOpen={this.state.isOpen} toggle={this.toggle} {...this.props} />;
    }
  }

  Uncontrolled.displayName = key;

  components[key] = Uncontrolled;
});

const UncontrolledDropdown = components.UncontrolledDropdown;
const UncontrolledTooltip = components.UncontrolledTooltip;
export {
  UncontrolledTooltip,
  UncontrolledDropdown
};
