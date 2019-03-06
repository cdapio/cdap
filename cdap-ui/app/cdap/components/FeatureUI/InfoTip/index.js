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

/* eslint react/prop-types: 0 */
import React from 'react';
import { UncontrolledTooltip } from 'reactstrap';

require("./InfoTip.scss");

class InfoTip extends React.Component {
  constructor(props) {
    super(props);
  }

  render() {
    return (
        <i className="fa fa-info-circle info-tip" id = {this.props.id}>
          <UncontrolledTooltip placement="right" target = {this.props.id}>
              {this.props.description}
          </UncontrolledTooltip>
        </i>
    );
  }
}
export default InfoTip;
