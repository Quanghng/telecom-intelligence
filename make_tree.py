from pathlib import Path

def generate_tree(dir_path: Path, prefix: str = '', ignore: set | None = None):    # Your exact list of folders to ignore
    if ignore is None:
        ignore = {'.venv', '__pycache__', '.pytest_cache', 'neo4j_data', '.git'}
    
    # Get all items, filter out ignored directories
    items = sorted([p for p in dir_path.iterdir() if p.name not in ignore])
    pointers = ['├── '] * (len(items) - 1) + ['└── ']
    
    for pointer, path in zip(pointers, items):
        print(prefix + pointer + path.name)
        if path.is_dir():
            extension = '│   ' if pointer == '├── ' else '    '
            generate_tree(path, prefix + extension, ignore)

print("Project Tree:")
generate_tree(Path('.'))