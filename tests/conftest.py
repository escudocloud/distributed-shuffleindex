def pytest_addoption(parser):
    parser.addoption('--small', action='store_true', help='limit accesses')

def pytest_generate_tests(metafunc):
    if 'N' in metafunc.fixturenames:
        metafunc.parametrize('N', [10 ** 2 if metafunc.config.option.small
                                   else 10 ** 4])
