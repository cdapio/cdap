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

import React, {PropTypes, Component} from 'react';
import {UncontrolledTooltip} from 'components/UncontrolledComponents';
import Clipboard from 'clipboard';
import IconSVG from 'components/IconSVG';
import T from 'i18n-react';

require('./CopyableRunID.scss');

const PREFIX = `features.PipelineSummary.CopyableRunID`;

export default class CopyableRunID extends Component {
  constructor(props) {
    super(props);
    this.state = {
      showTooltip: false
    };
  }
  onIDClickHandler() {
    this.setState({
      showTooltip: !this.state.showTooltip
    });
  }
  render() {
    let runidlabel = `A-${this.props.runid}`;
    if (this.props.idprefix) {
      runidlabel = `${this.props.idprefix}-${this.props.runid}`;
    }
    // FIXME: Not sure how else to do this. Looks adhoc. Need this for copy to clipboard.
    new Clipboard(`#${runidlabel}`);
    let tetherConfig = {
      classPrefix: 'run-id-tooltip'
    };
    let tooltipProps = {
      target: runidlabel,
      placement: 'right',
      tether: tetherConfig,
      delay: 0
    };
    if (this.state.showTooltip) {
      tooltipProps.isOpen = true;
    }
    return (
      <span
        className="btn-link"
        id={runidlabel}
        onClick={this.onIDClickHandler.bind(this, this.props.runid)}
        onMouseOut={() => {
          this.state.showTooltip ? this.onIDClickHandler() : null;
        }}
        data-clipboard-text={this.props.runid}
      >
        <span>{T.translate(`${PREFIX}.label`)}</span>
        <UncontrolledTooltip
          {...tooltipProps}
        >
          <span>{this.props.runid}</span>
          {
            this.state.showTooltip ?
              <span className="copied-label text-success">
                <IconSVG name="icon-check-circle" />
                <span>{T.translate(`${PREFIX}.copiedLabel`)}</span>
              </span>
            :
              null
          }
        </UncontrolledTooltip>
      </span>
    );
  }
}
CopyableRunID.propTypes = {
  runid: PropTypes.string.isRequired,
  idprefix: PropTypes.string
};
