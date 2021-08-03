from pytest import mark


@mark.envs
def test_sandbox(app_config):
    env = app_config.env
    sandbox = app_config.sandbox

    if env == 'sandbox' and sandbox:
        assert True
    else:
        assert False


@mark.envs
def test_live(app_config):
    env = app_config.env
    live = app_config.live

    if env == 'live' and live:
        assert True
    else:
        assert False

