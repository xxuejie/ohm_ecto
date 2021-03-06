defmodule Ohm.Ecto do
  @moduledoc """
  Ecto adapter module for Redis following Ohm patterns.
  """

  alias Ohm.Ecto.Redis, as: OhmRedis
  alias Ohm.Ecto.Utils

  @behaviour Ecto.Adapter
  @behaviour Ecto.Adapter.Storage

  #
  # Adapter callbacks
  #

  defmacro __before_compile__(_opts), do: :ok

  def child_spec(_repo, opts) do
    redis_url = Keyword.get(opts, :redis_url)
    # TODO: pooling & sharding
    Supervisor.Spec.worker(Redix, [redis_url, [name: :redix]])
  end

  def ensure_all_started(_repo, _type) do
    {:ok, []}
  end

  def autogenerate(:id), do: nil

  def autogenerate(:embed_id), do: Ecto.UUID.generate()

  def autogenerate(:binary_id), do: Ecto.UUID.generate()

  def loaders(:uuid, _type), do: [&Ecto.UUID.dump/1]

  def loaders(:naive_datetime, _type) do
    [fn(iso8601_datetime) ->
      Ecto.DateTime.cast(iso8601_datetime)
    end]
  end

  def loaders(primitive, _type), do: [primitive]

  def dumpers(:uuid, type), do: [type, &Ecto.UUID.load/1]

  def dumpers(:naive_datetime, type) do
    [type, fn(datetime) ->
      case Ecto.DateTime.cast(datetime) do
        {:ok, ecto_datetime} ->
          {:ok, Ecto.DateTime.to_iso8601(ecto_datetime)}
        error ->
          error
      end
    end]
  end

  def dumpers(primitive, _type), do: [primitive]

  def prepare(func, query), do: {:nocache, {func, query}}

  def execute(_repo, meta, {_cache, {func, query}}, params, preprocess, _options) do
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
    save(table_name, Keyword.get(fields, :id), fields)
  end

  def insert_all(_, _, _, _, _, _, _), do: raise "Not implemented yet"

  def update(_repo, %{source: {_prefix, table_name}}, fields, filters, _returning, _options) do
    # TODO: returning support, on conflict support, CAS support
    # NOTE: update is exactly insert here, if we want to make sure the model is created before updating,
    # we should use CAS token to validate that.
    save(table_name, Keyword.get(filters, :id), fields)
  end

  def delete(_repo, %{source: {_prefix, table_name}}, filters, _options) do
    key = Utils.a(table_name, Keyword.get(filters, :id))
    case OhmRedis.delete(key) do
      {:ok, _} ->
        # TODO: use the results obtained here to refresh indices
        {:ok, filters}
      {:error, :stale} ->
        {:error, :stale}
      {:error, error} ->
        raise error
    end
  end

  #
  # Storage callbacks
  #

  def storage_down(_options) do
    case Redix.command(:redix, ["FLUSHDB"]) do
      {:ok, _} ->
        :ok
      _ ->
       {:error, :redix_error}
    end
  end

  def storage_up(_options) do
    :ok
  end

  #
  # Helper functions
  #

  defp save(table_name, nil, fields) do
    save(table_name, generate_id(table_name), fields)
  end

  defp save(table_name, id, fields) do
    key = Utils.a(table_name, id)
    case OhmRedis.save(key, packed_values(fields)) do
      {:ok, _} ->
        {:ok, Keyword.put(fields, :id, id)}
      {:error, error} ->
        raise error
    end
  end

  defp generate_id(table_name) do
    case Redix.command(:redix, ["INCR", Utils.a(table_name, "_id")]) do
      {:ok, id} ->
        id
      {:error, error} ->
        raise error
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
