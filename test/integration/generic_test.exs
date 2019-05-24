defmodule GenericTest do
  use XandraTest.IntegrationCase, async: true

  test "Xandra.run/3", %{conn: conn, keyspace: keyspace} do
    assert %Xandra.SetKeyspace{} = Xandra.run(conn, [], &Xandra.execute!(&1, "USE #{keyspace}"))
  end

  describe "basic tests" do
    setup %{conn: conn, start_options: start_options, keyspace: keyspace} do
      Xandra.execute!(conn, "CREATE TABLE shows (id int PRIMARY KEY, name text)")

      on_exit(fn ->
        {:ok, conn} = Xandra.start_link(start_options)
        Xandra.execute!(conn, "USE #{keyspace}")
        Xandra.execute!(conn, "DROP TABLE shows")
      end)

      :ok
    end

    test "simple insert", %{conn: conn} do
      Xandra.execute(conn, "INSERT INTO shows (id, name) VALUES (1, 'GoT)")
    end

    test "simple select", %{conn: conn} do
      Xandra.execute(conn, "SELECT * FROM shows")
    end

    test "prepare simple insert", %{conn: conn} do
      Xandra.prepare(conn, "INSERT INTO shows (id, name) VALUES (1, 'GoT)")
    end
  end
end
