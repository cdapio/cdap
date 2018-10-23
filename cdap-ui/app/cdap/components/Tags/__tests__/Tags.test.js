import React from 'react';
import { cleanup, render, fireEvent, wait, waitForElement } from 'react-testing-library';
import Tags from 'components/Tags';
import Mousetrap from 'mousetrap';
import 'jest-dom/extend-expect';

afterEach(cleanup);

jest.mock('api/metadata');

const testEntity = {
  type: 'application',
  id: 'test'
};

describe('Tags', () => {
  it('renders correctly', () => {
    const { container } = render(<Tags entity={testEntity} />);
    expect(container.firstChild).toMatchSnapshot();
  });

  it('adds new tag when user inputs a tag and presses Enter', async () => {
    const TEST_TAG = 'UI_TEST_TAG';
    const { getByTestId, queryByTestId, getByText, container } = render(<Tags entity={testEntity} />);
    await wait(() => !queryByTestId('loading-icon')); // wait until loading icon is gone, which means async call is done

    const plusButtonElem = getByTestId('tag-plus-button');
    fireEvent.click(plusButtonElem, {container}); // click on plus button to show input box
    await waitForElement(() => getByTestId('tag-input'));

    const inputElem = getByTestId('tag-input');
    expect(inputElem).toHaveTextContent('');
    fireEvent.change(inputElem, {target: {value: TEST_TAG}});
    Mousetrap.trigger('return'); // enter value, and then press Enter to add tag
    await waitForElement(() => getByText(TEST_TAG));

    expect(inputElem).not.toBeInTheDocument(); // input elem is hidden after user presses Enter
    expect(getByText(TEST_TAG)).toBeVisible();
  });
});

