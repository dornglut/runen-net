from pathlib import Path
import re

pattern = re.compile(r"\.validate_(?:authority|peer)\s*\(\s*[^,\)]*\s*,", re.S)
violations = []
for path in Path("crates/runen-net").rglob("*.rs"):
    text = path.read_text()
    if pattern.search(text):
        violations.append(str(path))
if violations:
    raise SystemExit("old two-argument validation call remains in: " + ", ".join(sorted(violations)))
