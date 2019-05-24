defmodule Xandra.Connection.Utils do
  @moduledoc false

  alias Xandra.{ConnectionError, Error, Frame, Protocol}

  @spec recv_frame(:gen_tcp | :ssl, term, Xandra.protocol_version(), nil | module) ::
          {:ok, Frame.t()} | {:error, :closed | :inet.posix()}
  def recv_frame(transport, socket, protocol_version, compressor \\ nil)
      when is_atom(compressor) do
    length = Frame.header_length()

    with {:ok, header} <- transport.recv(socket, length) do
      case Frame.body_length(header) do
        0 ->
          Frame.decode(header, protocol_version)

        body_length ->
          with {:ok, body} <- transport.recv(socket, body_length),
               do: Frame.decode(header, body, protocol_version, compressor)
      end
    end
  end

  @spec request_options(:gen_tcp | :ssl, term, Xandra.protocol_version(), nil | module) ::
          {:ok, term} | {:error, ConnectionError.t()}
  def request_options(transport, socket, protocol_version, compressor \\ nil) do
    payload =
      Frame.new(:options)
      |> Protocol.encode_request(nil, protocol_version)
      |> Frame.encode(protocol_version)

    with :ok <- transport.send(socket, payload),
         {:ok, %Frame{} = frame} <- recv_frame(transport, socket, protocol_version, compressor),
         %{"CQL_VERSION" => _} = response <- Protocol.decode_response(frame, protocol_version) do
      {:ok, response}
    else
      {:error, reason} ->
        {:error, ConnectionError.new("request options", reason)}

      %Error{} = error ->
        {:error, ConnectionError.new("request options", error)}
    end
  end

  @spec startup_connection(:gen_tcp | :ssl, term, map, Xandra.protocol_version(), nil | module) ::
          :ok | {:error, ConnectionError.t()}
  def startup_connection(
        transport,
        socket,
        requested_options,
        protocol_version,
        compressor \\ nil,
        options \\ []
      )
      when is_map(requested_options) and is_atom(compressor) do
    # We have to encode the STARTUP frame without compression as in this frame
    # we tell the server which compression algorithm we want to use.
    payload =
      Frame.new(:startup)
      |> Protocol.encode_request(requested_options, protocol_version)
      |> Frame.encode(protocol_version)

    # However, we need to pass the compressor module around when we
    # receive the response to this frame because if we said we want to use
    # compression, this response is already compressed.
    with :ok <- transport.send(socket, payload),
         {:ok, frame} <- recv_frame(transport, socket, protocol_version, compressor) do
      case frame do
        %Frame{body: <<>>} ->
          :ok

        %Frame{kind: :authenticate} ->
          authenticate_connection(
            transport,
            socket,
            requested_options,
            protocol_version,
            compressor,
            options
          )

        %Frame{kind: :error} ->
          # errors like
          # %Xandra.Error{
          #   message: "Invalid message version. Got 4/v4 but previous messages on this connection had version 3/v3",
          #   reason: :protocol_violation
          # }
          error = %Error{} = Protocol.decode_response(frame, protocol_version)
          raise error

        _ ->
          raise "protocol violation, got unexpected frame: #{inspect(frame)}"
      end
    else
      {:error, reason} ->
        {:error, ConnectionError.new("startup connection", reason)}
    end
  end

  defp authenticate_connection(
         transport,
         socket,
         requested_options,
         protocol_version,
         compressor,
         options
       ) do
    payload =
      Frame.new(:auth_response)
      |> Protocol.encode_request(requested_options, options, protocol_version)
      |> Frame.encode(protocol_version)

    with :ok <- transport.send(socket, payload),
         {:ok, frame} <- recv_frame(transport, socket, protocol_version, compressor) do
      case frame do
        %Frame{kind: :auth_success} -> :ok
        %Frame{kind: :error} -> {:error, Protocol.decode_response(frame, protocol_version)}
        _ -> raise "protocol violation, got unexpected frame: #{inspect(frame)}"
      end
    else
      {:error, reason} ->
        {:error, ConnectionError.new("authenticate connection", reason)}
    end
  end
end
