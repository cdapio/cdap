/*
 * Copyright © 2016 Cask Data, Inc.
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
import Card from '../Card';
require('./MarketPlaceEntity.less');
export default function MarketPlaceEntity({ style, name, subtitle, icon, onClick}) {
  return (
    <Card
      size="LG"
      closeable
      cardClass="market-place-package-card"
      style={style}
      onClose={() => console.log('Closing the card')}
    >
      <div
        className="package-icon-container"
        onClick={onClick}>
        <img src={icon} />
      </div>
      <div onClick={onClick}>
        <div>{subtitle}</div>
        <div>{name}</div>
      </div>
    </Card>
  );
}

MarketPlaceEntity.propTypes = {
  className: PropTypes.string,
  style: PropTypes.object,
  name: PropTypes.string,
  subtitle: PropTypes.string,
  icon: PropTypes.string,
  onClick: PropTypes.func
};
