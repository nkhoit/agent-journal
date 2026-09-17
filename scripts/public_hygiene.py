#!/usr/bin/env python3
"""Reject common private identifiers and credential material in public files."""
from __future__ import annotations

import re
from pathlib import Path

SKIP = {".git", "bin", "dist", "target"}
TEXT_SUFFIXES = {
    ".rs", ".md", ".yaml", ".yml", ".json", ".sql", ".toml", ".lock", ".txt", ".sh", ".py"
}
TEXT_NAMES = {"Makefile"}
PATTERNS = {
    "home path": re.compile(r"/(?:Users|home)/[A-Za-z0-9._-]+"),
    "windows home path": re.compile(r"[A-Za-z]:\\Users\\", re.IGNORECASE),
    "private IPv4": re.compile(r"(?<![\d.])(?:10(?:\.[0-9]{1,3}){3}|192\.168(?:\.[0-9]{1,3}){2}|172\.(?:1[6-9]|2[0-9]|3[0-1])(?:\.[0-9]{1,3}){2})(?![\d.])"),
    "secret assignment": re.compile(r"(?i)\b(?:password|secret|api[_-]?key|access[_-]?token)\s*[:=]\s*['\"]?[A-Za-z0-9+/=_-]{16,}"),
    "bearer token": re.compile(r"(?i)\bBearer\s+[A-Za-z0-9._~-]{16,}"),
}


def main() -> int:
    violations: list[str] = []
    for path in Path(".").rglob("*"):
        if (
            not path.is_file()
            or (path.name not in TEXT_NAMES and path.suffix.lower() not in TEXT_SUFFIXES)
            or any(part in SKIP for part in path.parts)
        ):
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        for label, pattern in PATTERNS.items():
            for match in pattern.finditer(text):
                line = text.count("\n", 0, match.start()) + 1
                violations.append(f"{path}:{line}: {label}")
    if violations:
        print("public hygiene failed:")
        print("\n".join(violations))
        return 1
    print("public hygiene passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
