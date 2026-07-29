#!/usr/bin/env python3
"""Fix db.DB_PATH -> db.connection.DB_PATH and similar references in tests."""

import re
import glob

for filepath in glob.glob('tests/test_*.py'):
    with open(filepath) as f:
        src = f.read()
    orig = src
    
    # Replace db.DB_PATH with db.connection.DB_PATH
    # But NOT in monkeypatch.setattr(db.connection, "DB_PATH", ...) lines
    # Look for db.DB_PATH that's NOT already preceded by "connection."
    lines = src.split('\n')
    new_lines = []
    for line in lines:
        # Skip lines that are monkeypatch setattr calls
        if 'monkeypatch.setattr(db.connection, "DB_PATH"' in line:
            new_lines.append(line)
        else:
            # Replace db.DB_PATH -> db.connection.DB_PATH
            line = re.sub(r'\bdb\.DB_PATH\b', 'db.connection.DB_PATH', line)
            new_lines.append(line)
    
    src = '\n'.join(new_lines)
    
    if src != orig:
        with open(filepath, 'w') as f:
            f.write(src)
        print(f'Fixed: {filepath}')

print('Done')
