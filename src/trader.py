from typing import List, Dict, Tuple, Any


class Trader:
    trader = 'Calla and Lilly'

    def __init__(self):
        pass

    def update_trader(self):  # Abstract method, defined by convention only
        raise NotImplementedError("Subclass must implement abstract method")