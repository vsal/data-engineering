from pathlib import Path

current_dir = Path.cwd()
current_fle = Path(__file__).name

for filepath in current_dir.iterdir():
    if filepath.name == current_fle:
        continue

    print(f" - {filepath.name}")

    if filepath.is_file():
        content = filepath.read_text(encoding='utf-8')
        print(f"    content: {content}")