from rich import box
from rich.table import Table
from rich.text import Text

from eosvc.s3 import iter_local_files

# status -> (rich style, glyph, human label)
STATUS_STYLE = {
  "same size": ("dim", "=", "both (same size)"),
  "modified": ("yellow", "~", "both (modified)"),
  "local only": ("green", "+", "local only"),
  "remote only": ("red", "-", "remote only"),
}

# byte-magnitude heat scale: the unit suffix is colored so size is legible at a glance
UNIT_STYLE = {"B": "dim", "KB": "green", "MB": "yellow", "GB": "red", "TB": "bold red"}


def human_size(n):
  """Format a byte count as a Rich Text with a heat-colored unit, or dim '—' if None."""
  if n is None:
    return Text("—", style="dim")
  size = float(n)
  for unit in ("B", "KB", "MB", "GB", "TB"):
    if size < 1024 or unit == "TB":
      num = f"{int(size)}" if unit == "B" else f"{size:.1f}"
      t = Text(num + " ")
      t.append(unit, style=UNIT_STYLE[unit])
      return t
    size /= 1024


def fmt_date(dt):
  """Format an S3 LastModified datetime as 'YYYY-MM-DD HH:MM UTC', or '—' if None."""
  if dt is None:
    return "—"
  return dt.strftime("%Y-%m-%d %H:%M UTC")


def local_files_map(repo_dir, rel_dir):
  """Return {rel_posix: size} for all local files under repo_dir/rel_dir.

  rel_posix is relative to repo_dir (e.g. 'data/inputs.csv'), matching the
  namespace of remote keys with the '{repo}/' base stripped. Returns an empty
  map if the directory does not exist locally.
  """
  base = repo_dir / rel_dir
  if not base.exists():
    return {}
  repo_dir_abs = repo_dir.resolve()
  out = {}
  for file_path in iter_local_files(base.resolve()):
    rel = file_path.relative_to(repo_dir_abs).as_posix()
    out[rel] = file_path.stat().st_size
  return out


def remote_files_map(objects, base_prefix):
  """Return {rel: {'size', 'last_modified'}} from s3_list_objects results.

  Args:
      objects: List of dicts from s3_list_objects (key/size/last_modified).
      base_prefix: The '{repo}/' prefix to strip from each key.
  """
  base = base_prefix.rstrip("/") + "/"
  out = {}
  for o in objects:
    key = o["key"]
    if key.endswith("/") or not key.startswith(base):
      continue
    rel = key[len(base) :].lstrip("/")
    if rel:
      out[rel] = {"size": o["size"], "last_modified": o["last_modified"]}
  return out


def diff_entries(local_map, remote_map):
  """Classify the union of local and remote files by size-based status.

  Returns a list of dicts sorted by rel:
      {rel, status, local_size, remote_size, last_modified}
  """
  entries = []
  for rel in sorted(set(local_map) | set(remote_map)):
    local_size = local_map.get(rel)
    remote = remote_map.get(rel)
    remote_size = remote["size"] if remote else None
    last_modified = remote["last_modified"] if remote else None

    if remote is None:
      status = "local only"
    elif local_size is None:
      status = "remote only"
    elif local_size == remote_size:
      status = "same size"
    else:
      status = "modified"

    entries.append({
      "rel": rel,
      "status": status,
      "local_size": local_size,
      "remote_size": remote_size,
      "last_modified": last_modified,
    })
  return entries


def print_legend(console):
  """Print a one-line legend mapping glyphs/colors to statuses."""
  parts = []
  for status in ("same size", "modified", "local only", "remote only"):
    style, glyph, label = STATUS_STYLE[status]
    parts.append(f"[{style}]{glyph} {label}[/{style}]")
  console.print("Legend: " + "   ".join(parts))


def _build_tree(entries, strip):
  """Build a nested-dict tree of display paths, plus a {display_rel: entry} map.

  Leaf nodes are empty dicts. `strip` (e.g. the category dir) is removed from the
  front of each rel for display so the tree is rooted at the category contents.
  """
  tree = {}
  entry_by_path = {}
  for e in entries:
    rel = e["rel"]
    display = rel
    if strip and rel.startswith(strip + "/"):
      display = rel[len(strip) + 1 :]
    entry_by_path[display] = e
    node = tree
    for part in [p for p in display.split("/") if p]:
      node = node.setdefault(part, {})
  return tree, entry_by_path


def _aggregate(node, parts, entry_by_path):
  """Roll up the status counts and byte totals of all leaf descendants of a dir node."""
  agg = {
    "same size": 0,
    "modified": 0,
    "local only": 0,
    "remote only": 0,
    "local_bytes": 0,
    "remote_bytes": 0,
  }
  for name, child in node.items():
    full = "/".join(parts + [name])
    if child:
      sub = _aggregate(child, parts + [name], entry_by_path)
      for k in agg:
        agg[k] += sub[k]
    else:
      e = entry_by_path[full]
      agg[e["status"]] += 1
      agg["local_bytes"] += e["local_size"] or 0
      agg["remote_bytes"] += e["remote_size"] or 0
  return agg


def _counts_text(agg):
  """Compact colored rollup like '1+ 1- 2~ 3=' for a directory row (zeros omitted)."""
  t = Text()
  for status in ("local only", "remote only", "modified", "same size"):
    c = agg[status]
    if not c:
      continue
    style, glyph, _ = STATUS_STYLE[status]
    if len(t):
      t.append(" ")
    t.append(f"{c}{glyph}", style=style)
  return t


def render_diff_tree(console, title, entries, strip=None, max_depth=None):
  """Render a merged tree+table diff of local vs remote for one category/path.

  Column 0 carries the directory tree (glyphs + name); the remaining columns show
  per-file Status / Local size / Remote size / Uploaded date, color-coded by status.

  Returns a counts dict (per-status counts plus 'files'/'differ'/'same size') so the
  caller can aggregate a grand summary across categories.
  """
  counts = {"same size": 0, "modified": 0, "local only": 0, "remote only": 0}
  for e in entries:
    counts[e["status"]] += 1
  differ = counts["modified"] + counts["local only"] + counts["remote only"]
  local_bytes = sum(e["local_size"] or 0 for e in entries)
  remote_bytes = sum(e["remote_size"] or 0 for e in entries)

  # Header: sync-state dot + title + per-category totals.
  header = Text()
  header.append("● ", style="green" if differ == 0 else "yellow")
  header.append(title, style="bold")
  header.append(f"  ·  {len(entries)} files · local ")
  header.append_text(human_size(local_bytes))
  header.append(" · remote ")
  header.append_text(human_size(remote_bytes))
  console.print()
  console.print(header)

  if not entries:
    console.print("  [dim](empty)[/dim]")
    counts.update(files=0, differ=0)
    return counts

  table = Table(box=box.SIMPLE, expand=False, pad_edge=False)
  table.add_column("Path", no_wrap=True)
  table.add_column("Status", no_wrap=True)
  table.add_column("Local", justify="right", no_wrap=True)
  table.add_column("Remote", justify="right", no_wrap=True)
  table.add_column("Uploaded", no_wrap=True)

  tree, entry_by_path = _build_tree(entries, strip)

  def walk(node, parts, glyph_prefix):
    depth = len(parts) + 1  # depth of the children rendered in this call
    items = sorted(node.items(), key=lambda x: x[0])
    for i, (name, child) in enumerate(items):
      last = i == len(items) - 1
      branch = "└── " if last else "├── "
      full = "/".join(parts + [name])
      is_dir = bool(child)

      if is_dir:
        agg = _aggregate(child, parts + [name], entry_by_path)
        collapsed = max_depth is not None and depth >= max_depth
        name_text = Text(name + ("/ …" if collapsed else "/"), style="bold")
        path_cell = Text(glyph_prefix + branch) + name_text
        table.add_row(
          path_cell,
          _counts_text(agg),
          human_size(agg["local_bytes"]),
          human_size(agg["remote_bytes"]),
          "",
        )
        if not collapsed:
          walk(child, parts + [name], glyph_prefix + ("    " if last else "│   "))
      else:
        e = entry_by_path[full]
        style, glyph, label = STATUS_STYLE[e["status"]]
        path_cell = Text(glyph_prefix + branch) + Text(name, style=style)
        table.add_row(
          path_cell,
          Text(f"{glyph} {label}", style=style),
          human_size(e["local_size"]),
          human_size(e["remote_size"]),
          fmt_date(e["last_modified"]),
        )

  walk(tree, [], "")
  console.print(table)

  console.print(
    f"  [dim]{counts['same size']} same size · {counts['modified']} modified · "
    f"{counts['local only']} local-only · {counts['remote only']} remote-only[/dim]"
  )
  counts.update(files=len(entries), differ=differ)
  return counts
