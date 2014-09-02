package.path = '../lib/?.lua;../lib/?/?.lua;' .. package.path

local storage = {
  _VERSION     = '0.0.1',
  _DESCRIPTION = 'Click stream storage operations (mongodb version)'
}

local mongol = require('mongol')

local defaults = {
  host='localhost',
  port=27017,
  history=100
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

client_prototype.save_event = function(client, event)
  local visitor = event.id

  if not visitor then return false end

  event.id = nil
  local update = {
    ['$set']={
      lu=os.time()
    },
    ['$push']={
      events={
        ['$each']={
          event
        },
        ['$sort']={ ts=-1 },
        ['$slice']=client.history
      }
    }
  }
  client.db:update('events', { _id=visitor }, update, true, false)

  return true
end

client_prototype.get_events = function(client, visitor)
  for i, v in client.db:find('events', { _id = visitor }):pairs() do
    return v.events
  end
  return nil
end

client_prototype.scan_events = function(client, fn, query)
  local continue
  query = query or {}

  for i, v in client.db:find('events', query):pairs() do
    continue = fn(v._id, v.events)
    if not continue then break end
  end
end

local function create_mongo_connection(parameters)
  local mongo_conn = mongol(parameters.host, parameters.port)
  return mongo_conn
end

local function create_client(proto, mongo_conn, parameters)
  local client = setmetatable({}, getmetatable(proto))

  for i, v in pairs(proto) do
    client[i] = v
  end

  client.conn = mongo_conn
  client.db = mongo_conn:new_db_handle('cstream')
  client.history = parameters.history

  return client
end

function storage.new(...)
  local args, parameters = {...}, nil

  if #args == 1 then
    if type(args[1]) == 'table' then
      parameters = args[1]
    end
  end

  parameters = merge_defaults(parameters)
  local mongo_conn = create_mongo_connection(parameters)
  local client = create_client(client_prototype, mongo_conn, parameters)
  return client
end

return storage
