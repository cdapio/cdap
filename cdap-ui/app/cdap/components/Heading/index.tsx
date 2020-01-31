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

import * as React from 'react';
export enum HeadingTypes {
  h1 = 'h1',
  h2 = 'h2',
  h3 = 'h3',
  h4 = 'h4',
  h5 = 'h5',
  h6 = 'h6',
}
export interface IHeadingProps {
  type: HeadingTypes;
  label: string | React.ReactNode;
  className?: string;
  id?: string;
}
function Heading({ type, label, className, id, ...restProps }: IHeadingProps) {
  let HtmlHeading;
  switch (type) {
    case HeadingTypes.h2:
      HtmlHeading = 'h2';
      break;
    case HeadingTypes.h3:
      HtmlHeading = 'h3';
      break;
    case HeadingTypes.h4:
      HtmlHeading = 'h4';
      break;
    case HeadingTypes.h5:
      HtmlHeading = 'h5';
      break;
    case HeadingTypes.h6:
      HtmlHeading = 'h6';
      break;
    default:
      HtmlHeading = 'h1';
  }
  const props: Partial<IHeadingProps> = {
    className,
  };
  if (id) {
    props.id = id;
  }

  return (
    <HtmlHeading {...props} {...restProps}>
      {label}
    </HtmlHeading>
  );
}

export default Heading;
