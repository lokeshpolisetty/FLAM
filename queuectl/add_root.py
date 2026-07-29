#!/usr/bin/env python3
"""Add ROOT import to test files that use str(ROOT) but don't define ROOT."""

import re
import glob

needs_root = [
    "tests/test_advanced_suite.py",
    "tests/test_concurrency.py",
    "tests/test_integration_strategy.py",
    "tests/test_unit_comprehensive.py",
    "tests/test_unit_missing.py",
    "tests/test_nonfunctional_missing.py",
    "tests/test_observability.py",
    "tests/test_performance.py",
    "tests/test_resource.py",
    "tests/test_security.py",
    "tests/test_component_deep.py",
    "tests/test_component_missing.py",
    "tests/test_missing_comprehensive.py",
    "tests/test_queuectl.py",
    "tests/test_e2e_missing.py",
    "tests/test_e2e_shell.py",
    "tests/test_bug_regression.py",
]

for filepath in needs_root:
    with open(filepath) as f:
        src = f.read()

    # Skip if ROOT already defined
    if re.search(r'^ROOT\s*=', src, re.MULTILINE):
        print(f'Skipping (already has ROOT): {filepath}')
        continue

    # Skip if we don't actually use str(ROOT ...) 
    if 'str(ROOT' not in src:
        print(f'Skipping (no str(ROOT) usage): {filepath}')
        continue

    # Add Path import if not present
    if 'from pathlib import Path' not in src and 'import Path' not in src:
        src = src.replace('import sys\n', 'import sys\nfrom pathlib import Path\n')

    # Add ROOT definition after the first import block
    # Find the last import line and add ROOT after it
    # Strategy: find first non-import line after imports and insert ROOT before it
    lines = src.split('\n')
    insert_at = 0
    in_imports = False
    for i, line in enumerate(lines):
        stripped = line.strip()
        if stripped.startswith('import ') or stripped.startswith('from '):
            in_imports = True
            insert_at = i + 1
        elif in_imports and stripped and not stripped.startswith('#'):
            insert_at = i
            break

    lines.insert(insert_at, '\nROOT = Path(__file__).resolve().parent.parent')
    src = '\n'.join(lines)

    with open(filepath, 'w') as f:
        f.write(src)
    print(f'Fixed: {filepath}')

print('Done')
