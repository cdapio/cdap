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

import React from 'react';
import { cleanup, render, fireEvent, wait, waitForElement } from 'react-testing-library';
import Tags from 'components/Tags';
import 'jest-dom/extend-expect';

afterEach(cleanup);

jest.mock('api/metadata');

const testEntity = {
  type: 'application',
  id: 'test',
};

describe('Tags', () => {
  it('renders correctly', () => {
    const { container } = render(<Tags entity={testEntity} />);
    expect(container.firstChild).toMatchSnapshot();
  });

  it('adds new tag when user inputs a tag and presses Enter', async () => {
    const TEST_TAG = 'UI_TEST_TAG';
    const { getByTestId, queryByTestId, getByText, container } = render(
      <Tags entity={testEntity} />
    );
    await wait(() => !queryByTestId('loading-icon')); // wait until loading icon is gone, which means async call is done

    const plusButtonElem = getByTestId('tag-plus-button');
    fireEvent.click(plusButtonElem, { container }); // click on plus button to show input box
    await waitForElement(() => getByTestId('tag-input'));

    const inputElem = getByTestId('tag-input');
    expect(inputElem).toHaveTextContent('');
    fireEvent.change(inputElem, { target: { value: TEST_TAG } });
    fireEvent.keyDown(inputElem, {
      key: 'Enter',
      keyCode: 13,
      which: 13,
    }); // enter value, and then press Enter to add tag
    await waitForElement(() => getByText(TEST_TAG));

    expect(inputElem).not.toBeInTheDocument(); // input elem is hidden after user presses Enter
    expect(getByText(TEST_TAG)).toBeVisible();
  });
});
