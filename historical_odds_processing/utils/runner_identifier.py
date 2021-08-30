from typing import List


def get_runner_identifier(*args) -> str:
    return '__'.join((str(arg) for arg in args))


def break_runner_identifier_string(runnerIdentifier: str) -> List[str]:
    return runnerIdentifier.split('__')
