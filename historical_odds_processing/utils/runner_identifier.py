def get_runner_identifier(*args):
    return '__'.join((str(arg) for arg in args))


def break_runner_identifier_string(runnerIdentifier: str):
    return runnerIdentifier.split('__')
