defmodule Ohm.Ecto.Utils do
  def a(key, [head | tail]), do: a(a(key, head), tail)

  def a(key, []), do: key

  def a(key, segment), do: "#{key}:#{segment}"
end
