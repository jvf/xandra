defmodule Xandra.Connection.UtilsTest do
  use ExUnit.Case, async: true

  alias Xandra.Connection.Utils

  test "select_protocol_version valid inputs" do
    assert {:ok, 4} =
             Utils.select_protocol_version(%{"PROTOCOL_VERSIONS" => ["3/v3", "4/v4", "5/v5-beta"]})

    assert {:ok, 3} = Utils.select_protocol_version(%{})
  end

  test "select_protocol_version invalid inputs" do
    assert {:error, {:unsupported_protocol_version, ["2/v2"]}} =
             Utils.select_protocol_version(%{"PROTOCOL_VERSIONS" => ["2/v2"]})

    assert {:error, {:unsupported_protocol_version, ["proto3"]}} =
             Utils.select_protocol_version(%{"PROTOCOL_VERSIONS" => ["proto3"]})
  end
end
