# from decimal import Decimal, getcontext, ROUND_HALF_UP
from datetime import datetime
from dataclasses import dataclass

from QuikPy import QuikPy

# getcontext().prec = 6
# getcontext().rounding = ROUND_HALF_UP

@dataclass
class Transaction:
    "Class to handle transcations"
    trans_id: int
    action: str
    classcode: str
    seccode: str
    client_code: str = None
    account: str = None
    operation: str = None
    price: float = None
    quantity: int = None
    type: str = None
    order_key: str = None

    def as_dict(self):
        "Convert transaction to dict"
        tr_dict = {key.upper(): str(value) for key, value in self.__dict__.items() if value is not None}
        if tr_dict.get('QUANTITY'):
            tr_dict['QUANTITY'] = str(int(float(tr_dict['QUANTITY'])))
        return tr_dict


class QuikPyExtended(QuikPy):
    "QuikPy child class with high-level functions"
    trans_id = 1

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.active = True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.CloseConnectionAndThread()
        self.disable()
        # return True

    def incr_tr_id(self):
        self.trans_id += 1

    def is_active(self):
        return self.active

    def disable(self):
        self.active = False

    def enable(self):
        self.active = True
        
    def SendTransaction(self, transaction: Transaction, trans_id:int = 0):
        return super().SendTransaction(transaction.as_dict(), trans_id)
        
    @staticmethod
    def time_is_up(end_time: datetime.time):
        """chech if market exchange is closed"""
        current_time = datetime.now().time()
        return end_time < current_time

    def get_weighted_price(self, class_code: str, sec_code: str, trans_id: int = 0) -> float:
        """returns volume weighted price"""
        weighted_price = float(self.GetParamEx(class_code, sec_code, "WAPRICE", trans_id)['data']['param_value'])
        return weighted_price

    def get_available_volume(self, class_code: str, sec_code: str, trans_id: int = 0, percentage: float = 0.03) -> float:
        """returns amount of securities that available to buy/sell without influence on market"""

        volume = float(self.GetParamEx(class_code, sec_code, "VOLTODAY", trans_id)['data']['param_value'])
        available_volume = int(volume * percentage)
        return available_volume

    def get_last_price(self, class_code: str, sec_code: str, trans_id: int = 0) -> float:
        """returns last price of the deal"""

        last_price = float(self.GetParamEx(class_code, sec_code, "LAST", trans_id)['data']['param_value'])
        return last_price

    def get_best_offer(self, class_code: str, sec_code: str, trans_id: int = 0) -> tuple[int, float]:
        """returns the best offer price and size"""
        offer_info = self.GetQuoteLevel2(class_code, sec_code, trans_id)['data']['offer'][0]
        offer_quantity = int(float(offer_info['quantity']))
        offer_price = float(offer_info['price'])
        return offer_quantity, offer_price

    def get_worst_offer(self, class_code: str, sec_code: str, trans_id: int = 0) -> tuple[int, float]:
        """returns the worst offer price and size"""
        offer_info = self.GetQuoteLevel2(class_code, sec_code, trans_id)['data']['offer'][-1]
        offer_quantity = int(float(offer_info['quantity']))
        offer_price = float(offer_info['price'])
        return offer_quantity, offer_price

    def get_best_bid(self, class_code: str, sec_code: str, trans_id: int = 0) -> tuple[int, float]:
        """returns the best bid price and size"""
        bid_info = self.GetQuoteLevel2(class_code, sec_code, trans_id)['data']['bid'][-1]
        bid_quantity = int(float(bid_info['quantity']))
        bid_price = float(bid_info['price'])
        return bid_quantity, bid_price

    def get_best_bid_price(self, class_code: str, sec_code: str, trans_id: int = 0):
        """returns the best bid price"""
        bid_price = float(self.GetParamEx(class_code, sec_code, "BID", trans_id)['data']['param_value'])
        return bid_price
    
    def get_price_step(self, class_code: str, sec_code: str, trans_id: int = 0):
        """returns minimal price step"""
        price_step = float(self.GetParamEx(class_code, sec_code, "SEC_PRICE_STEP", trans_id)['data']['param_value'])
        return price_step
        

