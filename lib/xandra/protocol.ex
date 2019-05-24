defmodule Xandra.Protocol do
  @moduledoc false

  alias Xandra.Protocol

  def encode_request(frame, params, options \\ [], protocol_version)

  def encode_request(frame, params, options, :v3) do
    Protocol.V3.encode_request(frame, params, options)
  end

  def encode_request(frame, params, options, :v4) do
    Protocol.V4.encode_request(frame, params, options)
  end

  def decode_response(frame, query \\ nil, options \\ [], protocol_version)

  def decode_response(frame, query, options, :v3) do
    Protocol.V3.decode_response(frame, query, options)
  end

  def decode_response(frame, query, options, :v4) do
    Protocol.V4.decode_response(frame, query, options)
  end
end
