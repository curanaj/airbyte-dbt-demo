# MIT License
#
# Copyright (c) 2020 Airbyte
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.


from typing import Any, Iterable, List, Mapping

import pytest
from airbyte_cdk.models import AirbyteStream, SyncMode
from airbyte_cdk.sources.streams import Stream


class StreamStubFullRefresh(Stream):
    """
    Stub full refresh class to assist with testing.
    """

    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: List[str] = None,
        stream_slice: Mapping[str, Any] = None,
        stream_state: Mapping[str, Any] = None,
    ) -> Iterable[Mapping[str, Any]]:
        pass

    primary_key = None


def test_as_airbyte_stream_full_refresh(mocker):
    """
    Should return an full refresh AirbyteStream with information matching the
    provided Stream interface.
    """
    test_stream = StreamStubFullRefresh()

    mocker.patch.object(StreamStubFullRefresh, "get_json_schema", return_value={})
    airbyte_stream = test_stream.as_airbyte_stream()

    exp = AirbyteStream(name="stream_stub_full_refresh", json_schema={}, supported_sync_modes=[SyncMode.full_refresh])
    assert exp == airbyte_stream


class StreamStubIncremental(Stream):
    """
    Stub full incremental class to assist with testing.
    """

    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: List[str] = None,
        stream_slice: Mapping[str, Any] = None,
        stream_state: Mapping[str, Any] = None,
    ) -> Iterable[Mapping[str, Any]]:
        pass

    cursor_field = "test_cursor"
    primary_key = "primary_key"


def test_as_airbyte_stream_incremental(mocker):
    """
    Should return an incremental refresh AirbyteStream with information matching
    the provided Stream interface.
    """
    test_stream = StreamStubIncremental()

    mocker.patch.object(StreamStubIncremental, "get_json_schema", return_value={})
    airbyte_stream = test_stream.as_airbyte_stream()

    exp = AirbyteStream(
        name="stream_stub_incremental",
        json_schema={},
        supported_sync_modes=[SyncMode.full_refresh, SyncMode.incremental],
        default_cursor_field=["test_cursor"],
        source_defined_cursor=True,
        source_defined_primary_key=[["primary_key"]],
    )
    assert exp == airbyte_stream


def test_supports_incremental_cursor_set():
    """
    Should return true if cursor is set.
    """
    test_stream = StreamStubIncremental()
    test_stream.cursor_field = "test_cursor"

    assert test_stream.supports_incremental


def test_supports_incremental_cursor_not_set():
    """
    Should return false if cursor is not.
    """
    test_stream = StreamStubFullRefresh()

    assert not test_stream.supports_incremental


@pytest.mark.parametrize(
    "test_input, expected",
    [("key", [["key"]]), (["key1", "key2"], [["key1"], ["key2"]]), ([["key1", "key2"], ["key3"]], [["key1", "key2"], ["key3"]])],
)
def test_wrapped_primary_key_various_argument(test_input, expected):
    """
    Should always wrap primary key into list of lists.
    """

    wrapped = Stream._wrapped_primary_key(test_input)

    assert wrapped == expected
