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

import React, {PropTypes} from 'react';
import RenderObjectAsTable from 'components/RenderObjectAsTable';
import capitalize from 'lodash/capitalize';
import classnames from 'classnames';

require('./Genericdetails.scss');

export default function Genericdetails({details, className}) {
  delete details.name;
  delete details.url;
  delete details.version;
  delete details.logs;
  return (
    <div className={classnames(className, "generic-details")}>
      {
        Object
          .keys(details)
          .map((detail, i) => {
            return (
              <div key={i}>
                <strong>{capitalize(detail)}</strong>
                <RenderObjectAsTable obj={details[detail]} />
              </div>
            );
          })
      }
    </div>
  );
}
Genericdetails.propTypes = {
  details: PropTypes.object,
  className: PropTypes.string
};
