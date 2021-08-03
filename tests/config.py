class Config:
    def __init__(self, env):
        #SUPPORTED_ENVS = ['sandbox', 'live']

        self.env = env
        self.sandbox = False
        self.live = False

        if env == 'sandbox':
            self.sandbox = True
        elif env == 'live':
            self.live = True
