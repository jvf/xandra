defmodule Xandra.Simple do
  @moduledoc false

  defstruct [:statement, :values, :default_consistency, :protocol_version]

  @opaque t :: %__MODULE__{
            statement: Xandra.statement(),
            values: Xandra.values() | nil,
            default_consistency: atom() | nil,
            protocol_version: Xandra.protocol_version()
          }

  defimpl DBConnection.Query do
    alias Xandra.{Frame, Protocol}

    def parse(query, _options) do
      query
    end

    def encode(query, values, options) do
      Frame.new(:query)
      |> Protocol.encode_request(%{query | values: values}, options, query.protocol_version)
      |> Frame.encode(query.protocol_version, options[:compressor])
    end

    def decode(query, %Frame{} = frame, options) do
      Protocol.decode_response(frame, query, options, query.protocol_version)
    end

    def describe(query, _options) do
      query
    end
  end
end
