"""
Preparing of data for excel report.

Скрипт подготовливает данные для макроса, который находится в файле эксель (сам отчет).
    Добавляет данные (поступления на склад материалов) из УПП в новые данные из ERP.
    Обновляет старые названия номенклатур (если код номенклатуры скопировали в ERP).
    Добавляет новые справочники уровней номенклатуры, кратких названий (сортаментов).
"""

import logging

from etl.prepare import prepare_data


def main() -> None:
    """Главная функция скрипта"""
    prepare_data()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    # logging.disable(level=logging.CRITICAL)
    main()
