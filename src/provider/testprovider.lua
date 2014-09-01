local testprovider = {
  _VERSION     = '0.0.1',
  _DESCRIPTION = 'Click stream redis importer module for RuTarget'
}

local cjson = require('cjson.safe')

testprovider.parse_message = function(s)
  local t = cjson.decode(s)
  t['id'] = t['sberlabspx']
  t['url'] = 'http://www.sberlabs-oh-yeah.com/some/url?with=1&parameters'
  t['ts'] = t['msec']
  t['msec'] = nil
  t['sberlabspx'] = nil
  return t
end

return testprovider
