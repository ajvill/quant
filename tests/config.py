class Config:
    def __init__(self, env):
        #SUPPORTED_ENVS = ['sandbox', 'live']

        self.env = env
        self.sandbox = False
        self.live = False
        self.db = False

        if env == 'sandbox':
            self.sandbox = True
        elif env == 'live':
            self.live = True
        elif env == 'db':
            self.db = True
