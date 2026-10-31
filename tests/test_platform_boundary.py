'AST guard: OS-specific process/socket facts stay in ccx.system.oslayer.'
from __future__ import annotations
import ast
from pathlib import Path
import pytest
ROOT = Path(__file__).resolve().parents[1]
BANNED = frozenset({"O_PATH", "SO_PEERCRED", "SOL_LOCAL", "LOCAL_PEERCRED",
                    "LOCAL_PEERPID", "prctl", "PR_SET_PDEATHSIG", "proc_pidinfo"})
def _name(node):
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        head = _name(node.value)
        return f"{head}.{node.attr}" if head else None
    return None
def _string(node):
    return node.value if isinstance(node, ast.Constant) and isinstance(node.value, str) else None
def _check_source(source: str, filename: str) -> tuple[list[str], int]:
    tree = ast.parse(source, filename=filename)
    nodes = list(ast.walk(tree))
    parents = {child: parent for parent in nodes for child in ast.iter_child_nodes(parent)}
    docs = {id(node.body[0].value) for node in nodes
            if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef))
            and node.body and isinstance(node.body[0], ast.Expr)
            and isinstance(_string(node.body[0].value), str)}
    modules = {"os": {"os"}, "sys": {"sys"}, "platform": {"platform"}}
    direct = {"kill": set(), "getenv": set(), "environ": set(), "system": set()}
    for node in nodes:
        if isinstance(node, ast.Import):
            for item in node.names:
                if item.name in modules:
                    modules[item.name].add(item.asname or item.name)
        if isinstance(node, ast.ImportFrom):
            for item in node.names:
                name = item.asname or item.name
                if ((node.module == "os" and item.name in {"kill", "getenv", "environ"})
                        or (node.module == "platform" and item.name == "system")):
                    direct[item.name].add(name)
    kills = {f"{m}.kill" for m in modules["os"]} | direct["kill"]
    envs = {f"{m}.environ" for m in modules["os"]} | direct["environ"]
    getters = {f"{m}.getenv" for m in modules["os"]} | direct["getenv"]
    getters |= {f"{env}.{method}" for env in envs for method in ("get", "pop", "setdefault")}
    platforms = {f"{m}.system" for m in modules["platform"]} | direct["system"]
    issues, allowed = [], 0
    def exempt(node):
        nonlocal allowed
        parent = node
        while parent in parents:
            parent = parents[parent]
            if isinstance(parent, (ast.FunctionDef, ast.AsyncFunctionDef)):
                break
        if (filename == "ccx/claude/spawn.py" and getattr(parent, "name", None) == "_user_bus_available"
                and _name(node.func) == "os.environ.get" and len(node.args) == 1
                and not node.keywords and allowed == 0):
            allowed += 1
            return True
        return False
    for node in nodes:
        reason = None
        symbol = _name(node)
        if isinstance(node, (ast.Name, ast.Attribute)):
            if (node.id if isinstance(node, ast.Name) else node.attr) in BANNED:
                reason = "OS-specific primitive"
            if symbol in {f"{m}.platform" for m in modules["sys"]}:
                reason = "platform dispatch"
        elif isinstance(node, ast.ImportFrom):
            if node.module in {"os", "socket"} and any(x.name in BANNED for x in node.names):
                reason = "imported OS-specific primitive"
            if node.module == "sys" and any(x.name == "platform" for x in node.names):
                reason = "imported platform dispatch"
        elif isinstance(node, ast.Constant) and id(node) not in docs:
            if isinstance(node.value, str) and node.value.startswith(("/proc/", "/dev/fd/")):
                reason = "Linux-specific pathname"
        elif isinstance(node, ast.Subscript):
            if _string(node.slice) == "XDG_RUNTIME_DIR" and _name(node.value) in envs:
                reason = "XDG_RUNTIME_DIR lookup"
        elif isinstance(node, ast.Call):
            fn = _name(node.func)
            if fn in platforms:
                reason = "platform.system dispatch"
            if fn in kills and (len(node.args) > 1 and isinstance(node.args[1], ast.Constant)
                                and type(node.args[1].value) is int and node.args[1].value == 0
                                or any(k.arg in {"sig", "signal"} and isinstance(k.value, ast.Constant)
                                       and k.value.value == 0 for k in node.keywords)):
                reason = "bare kill(0) PID probe"
            if fn in {"getattr", "hasattr"} and len(node.args) >= 2 and _string(node.args[1]) in BANNED:
                reason = "dynamic OS-specific option"
            keys = node.args[:1] + [k.value for k in node.keywords if k.arg == "key"]
            if fn in getters and any(_string(x) == "XDG_RUNTIME_DIR" for x in keys):
                if not exempt(node):
                    reason = "XDG_RUNTIME_DIR lookup"
        if reason:
            issues.append(f"{filename}:{node.lineno}: {reason}")
    return issues, allowed
def test_production_platform_boundary():
    findings, bus = [], 0
    for path in sorted((ROOT / "ccx").rglob("*.py")):
        if path.name != "oslayer.py":
            issues, allowed = _check_source(path.read_text(), path.relative_to(ROOT).as_posix())
            findings.extend(issues)
            bus += allowed
    assert not findings, "\n".join(findings)
    assert bus == 1, "only spawn._user_bus_available may read the systemd bus environment"


@pytest.mark.parametrize("source,path,blocked", [
    ("os.O_PATH", "ccx/x.py", True), ("from os import O_PATH", "ccx/x.py", True),
    ("socket.SO_PEERCRED", "ccx/x.py", True), ("getattr(socket, 'SO_PEERCRED', None)", "ccx/x.py", True),
    ("libc.prctl(1,9)", "ccx/x.py", True), ("proc_pidinfo(1)", "ccx/x.py", True),
    ("source='/proc/self/fd/3'", "ccx/x.py", True), ("os.kill(pid,0)", "ccx/x.py", True),
    ("from os import kill as sig\nsig(pid,0)", "ccx/x.py", True),
    ("from os import environ as env\na=env['XDG_RUNTIME_DIR']", "ccx/x.py", True),
    ("import os as shell\nshell.environ.get('XDG_RUNTIME_DIR')", "ccx/x.py", True),
    ("sys.platform=='linux'", "ccx/x.py", True), ("from platform import system as platform_name\nplatform_name()", "ccx/x.py", True),
    ("def _user_bus_available():\n    x=os.environ.get('XDG_RUNTIME_DIR')", "ccx/claude/spawn.py", False), ("def other():\n    x=os.environ.get('XDG_RUNTIME_DIR')", "ccx/claude/spawn.py", True),
    ("\"\"\"mentions /proc/x, O_PATH, sys.platform\"\"\"\n# os.kill(pid,0)\nos.kill(pid,signal.SIGTERM)", "ccx/x.py", False),
])
def test_guard_self_check(source, path, blocked):
    issues, _allowed = _check_source(source, path)
    assert bool(issues) is blocked, (path, source, issues)
