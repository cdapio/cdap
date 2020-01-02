/*
 * Copyright © 2019 Cask Data, Inc.
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
import React from 'react';
import { MarkdownTable } from 'components/Markdown/MarkdownTable';
import { MarkdownHeading } from 'components/Markdown/MarkdownHeading';
import { HeadingTypes } from 'components/Heading';

const MarkdownToReactMapping = {
  h1({ id, children }) {
    return <MarkdownHeading type={HeadingTypes.h1} id={id} label={children} />;
  },
  h2({ id, children }) {
    return <MarkdownHeading type={HeadingTypes.h2} id={id} label={children} />;
  },
  h3({ id, children }) {
    return <MarkdownHeading type={HeadingTypes.h3} id={id} label={children} />;
  },
  h4({ id, children }) {
    return <MarkdownHeading type={HeadingTypes.h4} id={id} label={children} />;
  },
  h5({ id, children }) {
    return <MarkdownHeading type={HeadingTypes.h5} id={id} label={children} />;
  },
  table({ children }) {
    return <MarkdownTable children={children} />;
  },
};

export { MarkdownToReactMapping };
