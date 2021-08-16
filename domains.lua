dofile("table_show.lua")
dofile("urlcode.lua")
local urlparse = require("socket.url")
local http = require("socket.http")

local max_seconds = tonumber(os.getenv('max_seconds'))
local max_urls = tonumber(os.getenv('max_urls'))
local max_bytes = tonumber(os.getenv('max_bytes'))
local item_name = os.getenv('item_name')

io.stdout:write("Max seconds: " .. tostring(max_seconds) .. ".\n")
io.stdout:write("Max URLs: " .. tostring(max_urls) .. ".\n")
io.stdout:write("Max bytes: " .. tostring(max_bytes) .. ".\n")
io.stdout:flush()

local url_count = 0
local total_size = 0
local tries = 0
local downloaded = {}
local addedtolist = {}
local redirect_urls = {}
local abortgrab = false

local discovered = {}
local discovered_all = {}
local outlinks = {}
local discovered_count = 0

local domains = {}
domains["www." .. item_name] = true
domains[item_name] = true

if urlparse == nil or http == nil then
  io.stdout:write("socket not corrently installed.\n")
  io.stdout:flush()
  abortgrab = true
end

local start_time = os.time(os.date("!*t"))

io.stdout:write("Starting at " .. tostring(start_time) .. " seconds.\n")
io.stdout:flush()

local seconds_expired = false
local urls_expired = false
local bytes_expired = false

read_file = function(file)
  if file then
    local f = assert(io.open(file))
    local data = f:read("*all")
    f:close()
    return data
  else
    return ""
  end
end

submit_discovered = function()
  print("Submitting " .. tostring(discovered_count) .. " items.\n")
  for key, table in pairs({
    ["afghan-sites-nr7t9js2ea9qsaz"]=discovered,
    ["urls-ok7ej29an6v1jmo"]=outlinks
  }) do
    local items = nil
    for item, _ in pairs(table) do
      if not items then
        items = item
      else
        items = items .. "\0" .. item
      end
    end
    if items then
      local tries = 0
      while tries < 10 do
        local body, code, headers, status = http.request(
          "http://blackbird.arpa.li:23038/" .. key .. "/",
          items
        )
        if code == 200 or code == 409 then
          break
        end
        print("Could not queue items.\n")
        os.execute("sleep " .. math.floor(math.pow(2, tries)))
        tries = tries + 1
      end
      if tries == 10 then
        abort_item()
      end
    end
  end
  discovered = {}
  outlinks = {}
  discovered_count = 0
end

discover_item = function(type_, value, target)
  local item = nil
  if not target then
    target = "afghan-sites"
  end
  if target == "afghan-sites" then
    item = value
    target = discovered
  elseif target == "urls" then
    item = ""
    for c in string.gmatch(value, "(.)") do
      local b = string.byte(c)
      if b < 32 or b > 126 then
        c = string.format("%%%02X", b)
      end
      item = item .. c
    end
    target = outlinks
  else
    print("Bad items target.\n")
    abort_item()
  end
  if item == item_name or discovered_all[item] then
    return true
  end
  print('discovered item', item)
  target[item] = true
  discovered_all[item] = true
  discovered_count = discovered_count + 1
  if discovered_count == 100 then
    return submit_discovered()
  end
  return true
end

bad_code = function(status_code)
  return status_code == 0
    or status_code == 401
    or status_code == 403
    or status_code == 407
    or status_code == 408
    or status_code == 411
    or status_code == 413
    or status_code == 429
    or status_code == 451
    or status_code >= 500
end

allowed = function(url, parenturl)
  if string.match(url, "'+")
      or string.match(url, "[<>\\%*%$;%^%[%],%(%){}]") then
    return false
  end

  local tested = {}
  for s in string.gmatch(url, "([^/]+)") do
    if tested[s] == nil then
      tested[s] = 0
    end
    if tested[s] == 6 then
      return false
    end
    tested[s] = tested[s] + 1
  end

  local website = string.match(url, "^https?://([^/]+)")

  if domains[website] then
    return true
  end

  if string.match(website, "%.af$") then
    discover_item(nil, website, "afghan-sites")
  end

  discover_item(nil, url, "urls")

  return false
end

wget.callbacks.download_child_p = function(urlpos, parent, depth, start_url_parsed, iri, verdict, reason)
  if redirect_urls[parent["url"]] then
    return true
  end

  if allowed(urlpos["url"]["url"], parent["url"])
    and not downloaded[urlpos["url"]["url"]]
    and not addedtolist[urlpos["url"]["url"]] then
    addedtolist[urlpos["url"]["url"]] = true
    return verdict
  end
  return false
end

wget.callbacks.get_urls = function(file, url, is_css, iri)
  local urls = {}
  local html = nil
  
  downloaded[url] = true

  local function check(urla)
    local origurl = url
    local url = string.match(urla, "^([^#]+)")
    local url_ = string.match(url, "^(.-)%.?$")
    url_ = string.gsub(url_, "&amp;", "&")
    url_ = string.match(url_, "^(.-)%s*$")
    url_ = string.match(url_, "^(.-)%??$")
    url_ = string.match(url_, "^(.-)&?$")
    url_ = string.match(url_, "^(.-)/?$")
    if not downloaded[url_] and not addedtolist[url_] then
      table.insert(urls, { url=url_ })
      addedtolist[url_] = true
      addedtolist[url] = true
    end
  end

  local function checknewurl(newurl)
    if string.match(newurl, "\\[uU]002[fF]") then
      return checknewurl(string.gsub(newurl, "\\[uU]002[fF]", "/"))
    end
    if string.match(newurl, "^https?:////") then
      check(string.gsub(newurl, ":////", "://"))
    elseif string.match(newurl, "^https?://") then
      check(newurl)
    elseif string.match(newurl, "^https?:\\/\\?/") then
      check(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^\\/") then
      checknewurl(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^//") then
      check(urlparse.absolute(url, newurl))
    elseif string.match(newurl, "^/") then
      check(urlparse.absolute(url, newurl))
    elseif string.match(newurl, "^%.%./") then
      if string.match(url, "^https?://[^/]+/[^/]+/") then
        check(urlparse.absolute(url, newurl))
      else
        checknewurl(string.match(newurl, "^%.%.(/.+)$"))
      end
    elseif string.match(newurl, "^%./") then
      check(urlparse.absolute(url, newurl))
    end
  end

  local function checknewshorturl(newurl)
    if string.match(newurl, "^%?") then
      check(urlparse.absolute(url, newurl))
    elseif not (string.match(newurl, "^https?:\\?/\\?//?/?")
      or string.match(newurl, "^[/\\]")
      or string.match(newurl, "^%./")
      or string.match(newurl, "^[jJ]ava[sS]cript:")
      or string.match(newurl, "^[mM]ail[tT]o:")
      or string.match(newurl, "^vine:")
      or string.match(newurl, "^android%-app:")
      or string.match(newurl, "^ios%-app:")
      or string.match(newurl, "^%${")) then
      check(urlparse.absolute(url, "/" .. newurl))
    end
  end

  if string.match(url, "^https?://[^/]+/sitemap.xml")
    and status_code == 200 then
    html = read_file(file)
    for newurl in string.gmatch(string.gsub(html, "&quot;", '"'), '([^"]+)') do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(string.gsub(html, "&#039;", "'"), "([^']+)") do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(html, ">%s*([^<%s]+)") do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(html, "[^%-]href='([^']+)'") do
      checknewshorturl(newurl)
    end
    for newurl in string.gmatch(html, '[^%-]href="([^"]+)"') do
      checknewshorturl(newurl)
    end
    for newurl in string.gmatch(html, ":%s*url%(([^%)]+)%)") do
      checknewurl(newurl)
    end
  end

  return urls
end

wget.callbacks.write_to_warc = function(url, http_stat)
  return not bad_code(http_stat["statcode"])
end

wget.callbacks.httploop_result = function(url, err, http_stat)
  status_code = http_stat["statcode"]

  url_count = url_count + 1
  io.stdout:write(url_count .. "=" .. status_code .. " " .. url["url"] .. "  \n")
  io.stdout:flush()

  if downloaded[url["url"]] then
    io.stdout:write("URL already archived.\n")
    io.stdout:flush()
    return wget.actions.EXIT
  end

  total_size = total_size + http_stat["len"]

  if url_count > max_urls then
    urls_expired = true
    io.stdout:write("Archived maximum number of URLs.\n")
    io.stdout:flush()
    abortgrab = true
  end

  if os.time(os.date("!*t")) - start_time > max_seconds then
    seconds_expired = true
    io.stdout:write("Archived for maximum number of seconds.\n")
    io.stdout:flush()
    abortgrab = true
  end

  if total_size > max_bytes then
    bytes_expired = true
    io.stdout:write("Archived the maximum number of bytes.\n")
    io.stdout:flush()
    abortgrab = true
  end

  if status_code >= 300 and status_code <= 399 then
    local newloc = urlparse.absolute(url["url"], http_stat["newloc"])
    redirect_urls[url["url"]] = true
    if downloaded[newloc] == true or addedtolist[newloc] == true
      or not allowed(newloc, url["url"]) then
      tries = 0
      return wget.actions.EXIT
    end
  end
  
  if status_code >= 200 and status_code <= 399 then
    downloaded[url["url"]] = true
  end

  if abortgrab == true then
    io.stdout:write("ABORTING...\n")
    io.stdout:flush()
    return wget.actions.ABORT
  end

  if bad_code(status_code) then
    io.stdout:write("Server returned " .. http_stat.statcode .. " (" .. err .. "). Sleeping.\n")
    io.stdout:flush()
    local maxtries = 3
    if tries >= maxtries then
      io.stdout:write("I give up...\n")
      io.stdout:flush()
      tries = 0
      return wget.actions.EXIT
    else
      os.execute("sleep " .. math.floor(math.pow(2, tries)))
      tries = tries + 1
      return wget.actions.CONTINUE
    end
  end

  tries = 0

  local sleep_time = 0

  if sleep_time > 0.001 then
    os.execute("sleep " .. sleep_time)
  end

  return wget.actions.NOTHING
end

wget.callbacks.before_exit = function(exit_status, exit_status_string)
  if seconds_expired or urls_expired or bytes_expired then
    return wget.exits.WGET_EXIT_SUCCESS
  end
  if abortgrab == true then
    return wget.exits.IO_FAIL
  end
  return exit_status
end

