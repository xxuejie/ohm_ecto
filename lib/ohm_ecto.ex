defmodule Ohm.Ecto do
  @moduledoc """
  Ecto adapter module for Redis following Ohm patterns.
  """

  alias Ohm.Ecto.Utils

  @behaviour Ecto.Adapter

  defmacro __before_compile__(_opts), do: :ok

  def child_spec(_repo, opts) do
    redis_url = Keyword.get(opts, :redis_url)
    # TODO: pooling & sharding
    Supervisor.Spec.worker(Redix, [redis_url, [name: :redix]])
  end

  def ensure_all_started(_repo, _type) do
    {:ok, []}
  end

  def loaders(primitive, _type), do: [primitive]

  def dumpers(primitive, _type), do: [primitive]

  def prepare(func, query), do: {:nocache, {func, query}}

  def execute(repo, meta, {_cache, {func, query}}, params, preprocess, _options) do
    case match_get_by_id_query(func, query, params) do
      :not_matched ->
        raise "Query by index is not implemented yet!"
      id ->
        key = Utils.a(table(query), id)
        case Redix.command(:redix, ["HGETALL", key]) do
          {:ok, []} ->
            {0, []}
          {:ok, data} ->
            record = Enum.chunk(data, 2) |>
              Enum.map(fn [field, serialized_value] ->
                {field, Poison.decode!(serialized_value)}
              end)
              |> Enum.into(%{})
              |> Map.put("id", id)
            {1, [process_record(record, meta.fields, preprocess)]}
          {:error, error} ->
            raise error
        end
    end
  end

  def insert(_repo, %{source: {_prefix, table_name}}, fields, _on_conflict, _returning, _options) do
    # TODO: returning support, on conflict support, CAS support
    key = Utils.a(table_name, Keyword.get(fields, :id))
    case Redix.command(:redix, ["HMSET", key | packed_values(fields)]) do
      {:ok, _} ->
        {:ok, fields}
      {:error, error} ->
        raise error
    end
  end

  def update(_repo, %{source: {_prefix, table_name}}, fields, filters, _returning, _options) do
    # TODO: returning support, on conflict support, CAS support
    key = Utils.a(table_name, Keyword.get(filters, :id))
    # TODO: move those 2 commmands to a single Redis script for atomic operation
    case Redix.command(:redix, ["TYPE", key]) do
      {:ok, "hash"} ->
        case Redix.command(:redix, ["HMSET", key | packed_values(fields)]) do
          {:ok, _} ->
            {:ok, fields}
          {:error, error} ->
            raise error
        end
      _ ->
        {:error, :stale}
    end
  end

  def delete(_repo, %{source: {_prefix, table_name}}, filters, _options) do
    key = Utils.a(table_name, Keyword.get(filters, :id))
    # TODO: move those 2 commmands to a single Redis script for atomic operation
    case Redix.command(:redix, ["TYPE", key]) do
      {:ok, "hash"} ->
        case Redix.command(:redix, ["DEL", key]) do
          {:ok, _} ->
            {:ok, filters}
          {:error, error} ->
            raise error
        end
      _ ->
        {:error, :stale}
    end
  end

  defp match_get_by_id_query(:all,
    %{wheres: [
         %{expr: {:==, _,
                  [
                    {{:., _, [{:&, _, [0]}, :id]}, _, _},
                    {:^, _, [index]}
                  ]
                 }
          }
       ]
    },
    params), do: Enum.at(params, index)
  defp match_get_by_id_query(_func, _query, _params), do: :not_matched

  defp table(%{from: {t, _model}}), do: t

  defp process_record(record, ast, preprocess) do
    Enum.map(ast, fn {:&, _, [_, fields, _]} = expr when is_list(fields) ->
      data =
        fields
        |> Enum.map(&Atom.to_string/1)
        |> Enum.map(&Map.get(record, &1))
      preprocess.(expr, data, nil)
    end)
  end

  defp packed_values(fields) do
    Keyword.delete(fields, :id) |>
      Enum.flat_map(fn {field, val} ->
        # This would raise error immediately if value cannot be serialized to JSON
        [field, Poison.encode!(val)]
      end)
  end
end
