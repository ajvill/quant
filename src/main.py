import tkinter as tk
import logging
from connectors.binance_futures import BinanceFuturesClient
from connectors.coinbase import CoinbaseClient

logger = logging.getLogger()
logger.setLevel(logging.INFO)

stream_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s %(levelname)s %(module)s %(funcName)s:%(lineno)d :: %(message)s')
stream_handler.setFormatter(formatter)
stream_handler.setLevel(logging.INFO)

file_handler = logging.FileHandler('../info.log')
file_handler.setFormatter(formatter)
file_handler.setLevel(logging.DEBUG)

logger.addHandler(stream_handler)
logger.addHandler(file_handler)

if __name__ == '__main__':
    coinbase = CoinbaseClient(sandbox=True)

    #order = {
    #    'size': 0.01,
    #    'price': 0.100,
    #    'side': 'buy',
    #    'product_id': 'BTC-USD'
    #}
    #for i in range(0,10):
        #print(coinbase.place_new_order(order))
    #print(coinbase.cancel_an_order('d8f4306e-d5d5-4562-87a5-77a13e6e38e7'))
    #print(coinbase.cancel_all())
    #print('Here is a list of orders \n{}'.format(coinbase.list_orders()))
    #print(coinbase.get_an_order('d3d40574-e4bd-4408-a8d7-8cfd1abb18d7'))
    params = {
        'product_id': 'BTC-USD'
    }
    #print(coinbase.list_fills(params))

    # main window of application
    root = tk.Tk()
    root.mainloop()
    """
    # configure background
    root.config(bg='gray12')

    i = 0
    j = 0
    calibri_font = ("Calibri", 16, "normal")
    for contract in coinbase_contracts:
        #label_widget = tk.Label(root, text=contract, borderwidth=1, relief=tk.SOLID, width=18)

        #label_widget.pack(side=tk.TOP)
        #label_widget.pack(side=tk.BOTTOM)
        #label_widget.pack(side=tk.LEFT)
        #label_widget.pack(side=tk.RIGHT)
        # each label_widget background and foreground and font settings
        label_widget = tk.Label(root, text=contract, bg='gray12', fg='SteelBlue1', width=18, font=calibri_font)


        label_widget.grid(row=i, column=j, sticky='ew')
        if i == 4:
            j += 1
            i = 0
        else:
            i += 1
    """