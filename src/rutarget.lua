local provider = {
  _VERSION     = '0.0.1',
  _DESCRIPTION = 'Click stream redis importer module for RuTarget'
}

local cjson = require('cjson')
local redis = require('redis')

local defaults = {
  host_port   = 'unix:/tmp/redis.sock',
  expire_time = 60*60*24*365 -- one year
}

local function merge_defaults(parameters)
  if parameters == nil then
    parameters = {}
  end
  for k, v in pairs(defaults) do
    if parameters[k] == nil then
      parameters[k] = defaults[k]
    end
  end
  return parameters
end

local client_prototype = {}

client_prototype.store_click = function(client, data)
  local key = 'c=click:t=' .. data['id']
  data['id'] = nil -- remove the key to save sove memory in redis
  assert(client.redis:lpush(key, cjson.encode(data)))
  assert(client.redis:ltrim(key, 0, 99))
  assert(client.redis:expire(key, client.expire_time))
  return true
end

local function create_redis_client(parameters)
  local redis_client = redis.connect(parameters.host_port)
  return redis_client
end

local function create_client(proto, redis_client, parameters)
  local client = setmetatable({}, getmetatable(proto))

  for i, v in pairs(proto) do
    client[i] = v
  end

  client.redis = redis_client
  client.expire_time = parameters.expire_time
  return client
end

function provider.new(...)
  local args, parameters = {...}, nil

  if #args == 1 then
    if type(args[1]) == 'table' then
      parameters = args[1]
    end
  end

  parameters = merge_defaults(parameters)
  local redis_client  = create_redis_client(parameters)
  local client = create_client(client_prototype, redis_client, parameters)
  return client
end

return provider
