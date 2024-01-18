import datetime
import time

from QuikPyExtended import QuikPyExtended, Transaction
from report import Report


def on_trans_reply(data):
    global report, trans_data
    """Обработчик события ответа на транзакцию пользователя"""
    print(f"Выставлена сделка на {int(data['data']['quantity'])} штук "
          f"по {float(data['data']['price'])}.  "
          f"Осталось {quantity_limit} бумаг за этот период.")

    data['data']['amount'] = int(data['data']['quantity']) * float(data['data']['price'])
    report.current_report_sheet.append([data['data'][val] for val in
                                        ['class_code', 'sec_code', 'quantity', 'price', 'amount']])
    trans_data[data['data']['trans_id']] = data['data']['order_num'], time.time()


if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    # indicative_day_volume = 1_000_000
    order_volume = 1_000_000
    indicative_day_volume = 10_000
    previous_quantity_limit = 0
    with QuikPyExtended() as qp_provider, Report() as report:
        qp_provider.OnTransReply = on_trans_reply  # Ответ на транзакцию пользователя (вызов функции)
        trans_data = {}
        class_code = 'TQBR'  # Код площадки
        sec_code = 'LKOH'  # Код тикера
        client_code = '30_10010'
        account = 'L01-00000F00'
        quantity_limit = 0
        volatility = 3

        stop_time = datetime.time(18, 40)
        # price_step = float(qp_provider.GetParamEx(class_code, sec_code, "SEC_PRICE_STEP")['data']['param_value'])
        # пока ордер не исполнен
        # if qp_provider.is_active():
        while order_volume != 0 and (not qp_provider.time_is_up(stop_time)):
            print("----------------------------------------------------------------------")

            seconds = 15
            # if qp_provider.is_active():
            quantity_limit = qp_provider.get_available_volume(class_code, sec_code, qp_provider.trans_id, 0.03)
            print(f"{quantity_limit=}")
            quantity_limit -= previous_quantity_limit
            previous_quantity_limit += quantity_limit

            # не превысить объем ордера
            if order_volume < quantity_limit:
                quantity_limit = order_volume

            # quantity_deal = int(np.ceil(quantity_limit * 0.03))
            # ограничим кол-во бумаг за период вне зависимости от старта алгоритма
            quantity_deal = 50
            quantity_deal_ = None

            while quantity_limit > 0 and (not qp_provider.time_is_up(stop_time)):
                quantity_deal_ = quantity_deal
                if quantity_limit < quantity_deal_:
                    quantity_deal_ = quantity_limit
                print(f"{quantity_deal_}")
                while quantity_deal_ > 0:
                    vwap = qp_provider.get_weighted_price(class_code, sec_code,
                                                          qp_provider.trans_id)
                    last_price = qp_provider.get_last_price(class_code, sec_code, qp_provider.trans_id)

                    difference = abs((last_price - vwap) / vwap * 100)
                    if difference > volatility / 2:
                        print(difference, volatility)
                        continue

                    quantity_limit -= 1
                    quantity_deal_ -= 1
                    # best_quantity, best_price = qp_provider.get_best_bid(class_code, sec_code, qp_provider.trans_id)
                    best_price = qp_provider.get_best_bid_price(class_code, sec_code, qp_provider.trans_id)
                    transaction = Transaction(trans_id=qp_provider.trans_id, client_code=client_code,
                                              account=account, action='NEW_ORDER', classcode=class_code,
                                              seccode=sec_code, operation='S', price=best_price,
                                              quantity=1, type='L')

                    qp_provider.SendTransaction(transaction.as_dict(), qp_provider.trans_id)
                    qp_provider.MessageInfo(
                        f"Процент исполнения ордера равен {previous_quantity_limit / order_volume:.2%}")
                    qp_provider.incr_tr_id()

                    time.sleep(0.2)
                time.sleep(seconds)
            report.save()
            time.sleep(seconds)
