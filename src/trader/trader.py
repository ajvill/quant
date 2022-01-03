
class Trader:

    def __init__(self):
        self.trader = 'Calla and Lilly'

    def update_trader(self):  # Abstract method, defined by convention only
        raise NotImplementedError("Subclass must implement abstract method")