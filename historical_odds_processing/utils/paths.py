from pathlib import Path


def get_path(*args):
    path = Path(*[str(value) for value in args])
    path.mkdir(parents=True, exist_ok=True)
    return str(path)
