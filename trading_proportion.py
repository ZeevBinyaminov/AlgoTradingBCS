import datetime
import time

from QuikPyExtended import QuikPyExtended, Transaction
from report import Report

deal_size = {
    'LKOH': 50,
    'FIVE': 100,
    'NVTK': 150,
    'GAZP': 100,
    'NLMK': 50,
    'MGNT': 50,
}


CLASS_CODE = 'TQBR'  # Код площадки
SEC_CODE = 'LKOH'  # Код тикера
CLIENT_CODE = '30_10010'
ACCOUNT = 'L01-00000F00'
VOLATILITY = 3

CANCEL_AFTER_SECONDS = 5
WAIT_SECONDS = 5
CLOSING_TIME = datetime.time(18, 50)

# redis db instead of dict (cache + crash)
# переделать под замыкание (возвращаем функцию),
# вместо глобальных переменных внутренняя функция будет принимать параметры
# что быстрее и удобенее - запись в excel или в sql (nosql)


# def on_trans_reply(data):
#     global report, trans_data, quantity_limit, best_price
#     """Обработчик события ответа на транзакцию пользователя"""
#     if data['data']['quantity'] == 0:
#         # print(f"Снята заявка на продажу {int(data['data']['balance'])} штук. "
#         #       f"Осталось продать {quantity_limit} бумаг за этот период.")
#         # неявное изменение количества, добавить остаток обратно в необходимый объем
#         data['data']['amount'] = int(data['data']['balance']) * best_price
#         minus = [data['data'][val] for val in
#                      ['class_code', 'sec_code', 'balance', 'price', 'amount']]
#         minus[2] = -minus[2]
#         minus[3] = best_price
#         minus[4] = -minus[4]
#         report.current_report_sheet.append(minus)
#     else:
#         # print(f"Выставлена заявка на продажу {int(data['data']['quantity'])} штук. "
#         #       f"по {float(data['data']['price'])}.  "
#         #       f"Осталось продать {quantity_limit} бумаг за этот период.")

#         data['data']['amount'] = int(data['data']['quantity']) * float(data['data']['price'])
#         report.current_report_sheet.append([data['data'][val] for val in
#                                             ['class_code', 'sec_code', 'quantity', 'price', 'amount']])
#     trans_data[data['data']['trans_id']] = [data['data']['order_num']]


def on_trade(data):
    print(data)
    print(data['data']['qty'])
    global report, trans_data, quantity_limit, best_price
    """Обработчик события исполнения заявки"""
    data['data']['amount'] = int(data['data']['quantity']) * float(data['data']['price'])
    report.current_report_sheet.append([data['data'][val] for val in
                                            ['class_code', 'sec_code', 'quantity', 'price', 'amount']])
    trans_data[data['data']['trans_id']] = [data['data']['order_num']]



if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    # sys.stdout = None  # закрываем поток вывода
    # indicative_day_volume = 1_000_000
    order_volume = 1_000_000
    traded_quantity_other = 0  # проторговано вне алго
    traded_quantity_algo = 0  # проторговано в алго, накопленное
    quantity_limit = 0
    last_price = None

    with QuikPyExtended() as qp_provider, Report() as report:
        # qp_provider.OnTransReply = on_trans_reply  # Ответ на транзакцию пользователя (вызов функции)
        qp_provider.OnTrade = on_trade
        trans_data = {}

        # price_step = float(qp_provider.GetParamEx(CLASS_CODE, SEC_CODE, "SEC_PRICE_STEP")['data']['param_value'])
        # пока ордер не исполнен
        # if qp_provider.is_active():
        while order_volume != 0 and (not qp_provider.time_is_up(CLOSING_TIME)):
            print("----------------------------------------------------------------------")
            # до какого времени исполняется алгоритм (задается, ориентировочно конец дня)

            # if qp_provider.is_active():
            quantity_limit += qp_provider.get_available_volume(CLASS_CODE, SEC_CODE, qp_provider.trans_id, 0.03)
            quantity_limit -= traded_quantity_algo
            # quantity_limit -= traded_quantity_other
            traded_quantity_algo += quantity_limit

            # не превысить объем ордера
            if order_volume < quantity_limit:
                quantity_limit = order_volume

            quantity_deal = deal_size[SEC_CODE]
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
                quantity_limit -= quantity_deal

                if quantity_deal > 0:
                    best_quantity, best_price = qp_provider.get_best_offer(CLASS_CODE, SEC_CODE, qp_provider.trans_id)
                    transaction = Transaction(trans_id=qp_provider.trans_id, action='NEW_ORDER', classcode=CLASS_CODE,
                                              seccode=SEC_CODE, client_code=CLIENT_CODE, account=ACCOUNT,
                                              operation='S', price=best_price, quantity=quantity_deal, type='L')
                    qp_provider.SendTransaction(transaction.as_dict(), qp_provider.trans_id)
                    time.sleep(1.5)
                else:
                    print('quantity_deal <= 0')
                    break

                # while deal (balance != 0) is not accomplished: every 30 WAIT_SECONDS price -= price_step
                start = time.time()
                order = trans_data.get(qp_provider.trans_id)
                # заявка не дошла
                if order is None:
                    # учесть остаток
                    break

                order_id = order[0]
                # заявка не исполнилась полностью
                while qp_provider.GetOrderByNumber(order_id, qp_provider.trans_id).get(
                        'data', {'balance': 0}).get('balance', 0) != 0 and (not qp_provider.time_is_up(CLOSING_TIME)):
                    if time.time() - start >= CANCEL_AFTER_SECONDS:
                        transaction = Transaction(trans_id=qp_provider.trans_id + 1, action='KILL_ORDER',
                                                  classcode=CLASS_CODE, seccode=SEC_CODE, order_key=order_id)
                        quantity_limit += qp_provider.GetOrderByNumber(order_id,
                                                                       qp_provider.trans_id)['data']['balance']
                        qp_provider.incr_tr_id()
                        qp_provider.SendTransaction(transaction.as_dict(), qp_provider.trans_id)
                        time.sleep(0.5)
                        break
                else:
                    qp_provider.MessageInfo(f"Процент исполнения ордера равен {traded_quantity_algo / order_volume:.2%}")
                    time.sleep(0.5)

                qp_provider.incr_tr_id()
            report.save()
            time.sleep(WAIT_SECONDS)
