package.path = '../lib/?.lua;../lib/?/?.lua;' .. package.path

local storage = {
  _VERSION     = '0.0.1',
  _DESCRIPTION = 'Click stream storage operations (mongodb version)'
}

local mongol = require('mongol')
local crypto = require('crypto')

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

local function findOne(client, collection, query)
  for k, v in client.db:find(collection, query):pairs() do
    return v
  end
end

local client_prototype = {}

client_prototype.save_event = function(client, event)
  local visitor = event.id

  if not visitor then return false end

  event.id = nil

  local ua = event.ua
  if ua then
    client.sha1:reset()
    local ua_hash = client.sha1:final(ua) -- returns hex value
    event.ua = ua_hash
    client.db:insert('uadict', {{ _id=ua_hash, ua=ua }}, true)
  end

  local url = event.url
  if url then
    -- leave only domain name of the server in 'url'
    event.url = string.match(url, '^https*://([%w%.%-]+)/*')
  end

  local update

  if math.random(1,1000) == 1 then
    update = {
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
  else
    update = {
      ['$set']={
        lu=os.time()
      },
      ['$push']={
        events=event
      }
    }
  end

  client.db:update('events', { _id=visitor }, update, true, false)

  return true
end

client_prototype.get_events = function(client, visitor)
  local uas = {}
  local result = findOne(client, 'events', { _id=visitor }).events

  for i, c in ipairs(result) do
    local hex_hash = c.ua

    if hex_hash then
      local ua_text

      if uas[hex_hash] == nil then
        ua_text = findOne(client, 'uadict', { _id=hex_hash }).ua
        uas[hex_hash] = ua_text
      else
        ua_text = uas[hex_hash]
      end

      result[i].ua = ua_text
    end
  end

  return result
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
  if parameters.user and parameters.password then
    print('authenticating user=' .. parameters.user ..
            ' password=' .. parameters.password)
    assert(client.db:auth(parameters.user, parameters.password))
  end
  client.history = parameters.history
  client.sha1 = crypto.digest.new('sha1')

  return client
end

function storage.new(...)
  local args, parameters = {...}, nil

  if #args == 1 then
    if type(args[1]) == 'table' then
      parameters = args[1]
    end
  end

  math.randomseed(os.time())
  parameters = merge_defaults(parameters)
  local mongo_conn = create_mongo_connection(parameters)
  local client = create_client(client_prototype, mongo_conn, parameters)
  return client
end

return storage
