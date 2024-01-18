"Proportion algorithm"
import datetime
import time
import sys
import signal

from QuikPyExtended import QuikPyExtended, Transaction
from database import Database

deal_size = {
    'LKOH': 50,
    'FIVE': 100,
    'NVTK': 150,
    'GAZP': 100,
    'NLMK': 50,
    'MGNT': 50,
    'YNDX': 50,
    
}

trans_data = {}

CLASS_CODE = 'TQBR'  # Код площадки
SEC_CODE = 'MGNT'  # Код тикера
CLIENT_CODE = '30_10010'
ACCOUNT = 'L01-00000F00'
VOLATILITY = 3

CANCEL_AFTER_SECONDS = 5
WAIT_SECONDS = 5
CLOSING_TIME = datetime.time(18, 55)


def on_close(sig, frame):
    """Handler of ctrl + C"""
    database.insert_results(ticker=SEC_CODE, vwap_market=vwap)
    print("Программа закрыта! Результаты прогона записаны в базу данных.")
    sys.exit(0)


def on_trans_reply(data: dict):
    """Callback to transaction"""
    trans_data[data['data']['trans_id']] = data['data']['order_num']


def on_trade(data: dict):
    """Callback to accomplished deal"""
    if data['data'].get('sec_code') == SEC_CODE:
        database.insert_deal(data['data'])
        vwap_ = qp_provider.get_weighted_price(CLASS_CODE, SEC_CODE,
                                               qp_provider.trans_id)
        my_vwap = float(database.calc_vwap(SEC_CODE))
        print(f"My VWAP: {my_vwap:.3f}, Market VWAP: {vwap_:.3f},"
              f"Difference: {my_vwap / vwap_ - 1:.4f}%")


signal.signal(signal.SIGINT, on_close)

if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    # sys.stdout = None  # закрываем поток вывода
    # indicative_day_volume = 1_000_000
    ORDER_VOLUME = 1_000_000
    TRADED_QUANTITY_OTHER = 0  # проторговано вне алго
    traded_quantity_algo = 0  # проторговано в алго, накопленное
    quantity_limit = 0
    last_price = 0

    with QuikPyExtended() as qp_provider, Database() as database:
        qp_provider.OnTrade = on_trade
        qp_provider.OnTransReply = on_trans_reply
        PRICE_STEP = qp_provider.get_price_step(CLASS_CODE, SEC_CODE,
                                                qp_provider.trans_id)
        LIMIT = [dct for dct in qp_provider.GetDepoLimits(SEC_CODE, qp_provider.trans_id)['data']
                 if 'openbal' in dct][0]['openbal']
        if LIMIT == 0:
            print('Лимиты не установлены!')
            sys.exit(0)


        # if qp_provider.is_active():
        while ORDER_VOLUME > 0 and (not qp_provider.time_is_up(CLOSING_TIME)):
            print("----------------------------------------------------------------------")
            # до какого времени исполняется алгоритм (задается, ориентировочно конец дня)

            # if qp_provider.is_active():
            quantity_deal = deal_size[SEC_CODE]
            quantity_limit += qp_provider.get_available_volume(CLASS_CODE, SEC_CODE, qp_provider.trans_id, 0.03)
            quantity_limit -= traded_quantity_algo
            quantity_limit -= TRADED_QUANTITY_OTHER
            print(f"{quantity_limit=}, {traded_quantity_algo=}")
            traded_quantity_algo += quantity_limit

            # не превысить объем ордера
            if ORDER_VOLUME < quantity_limit:
                quantity_limit = ORDER_VOLUME

            # сколько % исполнено от всего дневного оборота
            # раз в t времени сколько прошло на рынке
            while quantity_limit > 0 and (not qp_provider.time_is_up(CLOSING_TIME)):
                vwap = qp_provider.get_weighted_price(CLASS_CODE, SEC_CODE, qp_provider.trans_id)  # Цена входа/выхода
                last_price = qp_provider.get_last_price(CLASS_CODE, SEC_CODE, qp_provider.trans_id)

                difference = round(abs((last_price - vwap) / vwap * 100), 2)
                if difference > VOLATILITY / 2:
                    print(f"Skip: {difference=}, {VOLATILITY=}")
                    continue  # волатильность < разница (последняя цена и срднвзвес цена) - alert

                if quantity_limit < quantity_deal:
                    quantity_deal = quantity_limit
                    print(f"{quantity_deal=}")
                quantity_limit -= quantity_deal

                best_quantity, best_price = qp_provider.get_best_offer(CLASS_CODE, SEC_CODE, qp_provider.trans_id)
                qp_provider.incr_tr_id()
                sell_transaction = Transaction(trans_id=qp_provider.trans_id, action='NEW_ORDER', classcode=CLASS_CODE,
                                          seccode=SEC_CODE, client_code=CLIENT_CODE, account=ACCOUNT,
                                          operation='S', price=best_price, quantity=quantity_deal, type='L')
                qp_provider.SendTransaction(sell_transaction, qp_provider.trans_id)
                time.sleep(1.5)

                start = time.time()
                order_id = trans_data.get(qp_provider.trans_id)
                            
                order = qp_provider.GetOrderByNumber(order_id, qp_provider.trans_id)['data']
                while order['balance'] > 0 and (not qp_provider.time_is_up(CLOSING_TIME)):
                    if time.time() - start >= CANCEL_AFTER_SECONDS:
                        order_id = trans_data.get(qp_provider.trans_id)
                        if order_id is None:
                            break
                        
                        order = qp_provider.GetOrderByNumber(order_id, qp_provider.trans_id)['data']
                        if order['balance'] == 0:
                            continue
                        
                        qp_provider.incr_tr_id()
                        cancel_transaction = Transaction(trans_id=qp_provider.trans_id, action='KILL_ORDER',
                                                         classcode=CLASS_CODE, seccode=SEC_CODE, order_key=order_id)
                        qp_provider.SendTransaction(cancel_transaction, qp_provider.trans_id)
                        time.sleep(2.5)
                        
                        qp_provider.incr_tr_id()
                        sell_transaction.quantity = int(qp_provider.GetOrderByNumber(order_id, qp_provider.trans_id)['data']['balance'])
                        sell_transaction.price -= PRICE_STEP
                        sell_transaction.trans_id = qp_provider.trans_id
                        qp_provider.SendTransaction(sell_transaction, qp_provider.trans_id)
                        
                        start = time.time()
                        time.sleep(0.5)
                    order = qp_provider.GetOrderByNumber(order_id, qp_provider.trans_id)['data']
                time.sleep(WAIT_SECONDS)
            print(f"Процент исполнения ордера равен {traded_quantity_algo / ORDER_VOLUME:.2%}")
            time.sleep(WAIT_SECONDS)
            
