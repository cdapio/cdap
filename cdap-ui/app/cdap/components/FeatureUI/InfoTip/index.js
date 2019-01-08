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
