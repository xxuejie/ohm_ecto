defmodule Ohm.Ecto.Redis do
  @save_lua """
local key = KEYS[1]
local attrs = cjson.decode(ARGV[1])
local cas_token = ARGV[2]
local next_token

if math.mod(#attrs, 2) == 1 then
  error("Wrong number of attribute/value pairs")
end

if cas_token ~= nil then
  local current_token = redis.call('HGET', key, '_cas')
  if (not current_token) or current_token == cas_token then
    if current_token then
      next_token = tonumber(current_token) + 1
    else
      next_token = 1
    end
    redis.call('HSET', key, '_cas', next_token)
  else
    error('cas_error')
  end
end

if #attrs > 0 then
  redis.call("HMSET", key, unpack(attrs))
end

return next_token
  """

  @save_lua_digest :crypto.hash(:sha, @save_lua) |> Base.encode16

  def save(key, packed_values, cas_token \\ nil) do
    attr_arg = Poison.encode!(packed_values)
    args = case cas_token do
             nil ->
               [attr_arg]
             _ ->
               [attr_arg, cas_token]
           end
    script(@save_lua, @save_lua_digest, [key], args)
  end

  defp script(script, digest, keys, args) do
    try do
      do_script(script, digest, keys, args)
    rescue
      e in Redix.Error ->
        cond do
          String.match?(e.message, ~r/cas_error/) ->
            {:error, :cas_error}
          true ->
            raise e
        end
    end
  end

  defp do_script(script, digest, keys, args) do
    try do
      Redix.command(:redix, ["EVALSHA", digest, Enum.count(keys)] ++ keys ++ args)
    rescue
      e in Redix.Error ->
        cond do
          String.match?(e.message, ~r/^NOSCRIPT/) ->
            Redix.command(:redix, ["EVAL", script, Enum.count(keys)] ++ keys ++ args)
          true ->
            raise e
        end
    end
  end
end
