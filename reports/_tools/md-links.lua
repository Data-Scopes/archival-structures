local function fix(t)
  if t:match("^%a[%w+.-]*:") or t:match("^#") or t:match("^/") or t == "" then return t end
  local path, frag = t:match("^([^#]*)(.*)$")
  if path:match("%.md$") and not path:find("/") then return path:gsub("%.md$", ".html") .. frag end
  return "../" .. t
end
function Link(el) el.target = fix(el.target); return el end
function Image(el) el.src = fix(el.src); return el end
