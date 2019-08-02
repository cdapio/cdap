/*
 * Copyright © 2017-2018 Cask Data, Inc.
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

import React from 'react';
import PropTypes from 'prop-types';
import {
  FormGroup,
  Input,
  FormFeedback,
  InputGroup,
  InputGroupAddon,
  InputGroupText,
  UncontrolledTooltip,
} from 'reactstrap';
require('./ValidatedInput.scss');

export default function ValidatedInput(props) {
  const {validationError, inputInfo, label, ...moreProps} = props;
  const isInvalid = validationError ? true : false;

  return (
      <FormGroup row className="validateInput">
        <InputGroup>
          <Input {...moreProps} invalid={isInvalid}/>
          <InputGroupAddon addonType="append" className="input-group-info">
            <InputGroupText id={label.replace(/\s/g,'')}>
              <span className="fa fa-info-circle help"></span>
              <UncontrolledTooltip placement="left" target={label.replace(/\s/g,'')}>
                {inputInfo}
              </UncontrolledTooltip>
            </InputGroupText>
          </InputGroupAddon>
        </InputGroup>
        <FormFeedback className="feedback-error">{validationError}</FormFeedback>
      </FormGroup>
  );
}

ValidatedInput.propTypes = {
  type: PropTypes.string,
  size: PropTypes.string,
  label: PropTypes.string,
  required: PropTypes.bool,
  placeholder: PropTypes.string,
  defaultValue: PropTypes.string,
  value: PropTypes.string,
  disabled: PropTypes.bool,
  className: PropTypes.string,
  validationError: PropTypes.string,
  inputInfo: PropTypes.string,
  onChange: PropTypes.func,
  readOnly: PropTypes.bool,
};
