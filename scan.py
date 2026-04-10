import os
packages = set()
for root, dirs, files in os.walk('.'):
    dirs[:] = [d for d in dirs if d not in ['__pycache__', '.venv', 'Lib', 'Scripts', 'Include', 'temp']]
    for f in files:
        if f.endswith('.py'):
            for line in open(os.path.join(root, f), encoding='utf-8', errors='ignore'):
                line = line.strip()
                if line.startswith('import '):
                    pkg = line.split()[1].split('.')[0]
                    packages.add(pkg)
                elif line.startswith('from '):
                    pkg = line.split()[1].split('.')[0]
                    packages.add(pkg)
for p in sorted(packages):
    print(p)