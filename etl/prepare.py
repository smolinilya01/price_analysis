"""Prepare data"""

import logging
import re
import datetime

from pandas import (
    DataFrame, Series, read_csv, read_excel, concat
)
PATH_METAL = r"\\oemz-fs01.oemz.ru\Works$\Analytics\Илья\!outloads\Справочник_металла (ANSITXT).txt"
PATH_METIZ = r"\\oemz-fs01.oemz.ru\Works$\Analytics\Илья\!outloads\Справочник_метизов_лэп (ANSITXT).txt"
PATH_NEW_INPUTS_METAL = r"\\oemz-fs01.oemz.ru\Works$\Analytics\Илья\!outloads\Закупки_за_период_металл (ANSITXT).txt"
PATH_NEW_INPUTS_METIZ = r"\\oemz-fs01.oemz.ru\Works$\Analytics\Илья\!outloads\Закупки_за_период_метизы (ANSITXT).txt"
PATH_NEW_INPUTS_ELEKTROD = r"\\oemz-fs01.oemz.ru\Works$\Analytics\Илья\!outloads\Закупки_за_период_сварочная_электроды (ANSITXT).txt"
PATH_NEW_INPUTS_CGC = r"\\oemz-fs01.oemz.ru\Works$\Analytics\Илья\!outloads\Закупки_за_период_мат_цгц (ANSITXT).txt"


def prepare_data() -> None:
    """Prepare data for report
    Обновляет справочники, соединяет старые поступления с новыми, обновляет некоторые назван номенклатур."""
    # справочники
    prepare_dicts()

    # посупление и обновление старых названий номенклатур
    old_inputs = read_excel(r'support_data/data/old_inputs.xlsx')

    new_inputs_metal = prepare_inputs(PATH_NEW_INPUTS_METAL)
    new_inputs_metiz = prepare_inputs(PATH_NEW_INPUTS_METIZ)
    new_inputs_electrod = prepare_inputs(PATH_NEW_INPUTS_ELEKTROD)
    new_inputs_zinc = prepare_inputs(PATH_NEW_INPUTS_CGC)
    new_inputs = concat(
        [new_inputs_metal, new_inputs_metiz,
         new_inputs_electrod, new_inputs_zinc],
        sort=True
    )

    need_columns = [
        'Ссылка', 'Номер', 'Номер4', 'Дата', 'Контрагент', 'НомерВходящегоДокумента',
        'Сумма', 'Склад', 'Номенклатура', 'Количество', 'Всего', 'Без_НДС', 'Цена',
        'Ед_Изм', 'СкладОтправитель', 'Комментарий', 'Код_УПП', 'Код_БП',
        'Наименование_БП', 'Код_БП_УПП', 'ИНН_Контрагента'
    ]
    new_inputs = concat(
        [old_inputs, new_inputs],
        sort=True
    )
    new_inputs = new_inputs[need_columns]

    today = datetime.datetime.now().strftime('%d.%m.%Y')
    new_inputs.to_excel(
        r"\\oemz-fs01.oemz.ru\Works$\Analytics\Выгрузки из УПП\Цены и индекс\Новые_поступления_{0}.xlsx".format(today),
        index=False
    )
    logging.info('Поступления подготовились')


def prepare_dicts() -> None:
    """Prepare dicts for excel report
    Подготовка справочников, котоые ьудут подгружены макросом."""
    metal = dict_nomenclature(path=PATH_METAL, kind=1)
    metiz = dict_nomenclature(path=PATH_METIZ, kind=2)
    new_dict_elekctrod = prepare_dicts_elektrod()
    new_dict_zinc = prepare_dicts_zinc()

    new_dict = concat(
        [metal,
         metiz,
         new_dict_elekctrod,
         new_dict_zinc],
        sort=True
    )
    new_dict = new_dict[[
        'name', 'level_1',
        'level_2', 'level_3',
        'Сортамент'
    ]]

    old_dict_levels = read_excel(r'support_data/data/dict_levels.xlsx')
    new_dict_levels: DataFrame = concat(
        [old_dict_levels, new_dict.iloc[:, :4]],
        sort=True
    )
    new_dict_levels = new_dict_levels[['name', 'level_1', 'level_2', 'level_3']]
    new_dict_levels.to_excel(r'support_data/dumps/new_dict_levels.xlsx', index=False)

    old_dict_short_name = read_excel(r'support_data/data/dict_short_names.xlsx')
    new_dict_short_name: DataFrame = concat(
        [old_dict_short_name,
         new_dict.loc[:, ['name', 'Сортамент']].
             rename(columns={'name': 'full_name', 'Сортамент': 'short_name'}),
         new_dict_elekctrod.loc[:, ['name', 'Сортамент']].
             rename(columns={'name': 'full_name', 'Сортамент': 'short_name'}),
         new_dict_zinc.loc[:, ['name', 'Сортамент']].
             rename(columns={'name': 'full_name', 'Сортамент': 'short_name'}),
         ],
        sort=True
    )
    new_dict_short_name.to_excel(r'support_data/dumps/new_dict_short_names.xlsx', index=False)


def dict_nomenclature(path: str, kind: int) -> DataFrame:
    """Load nomenclature dict.
    Загрузка спрвочника по металлу и метизам

    :param path: file path
    :param kind: 1 - metal, 2 - metiz
    """
    data = read_csv(
        path,
        sep='\t',
        encoding='ansi'
    ).fillna('')

    if kind == 1:
        rename_columns = {
            'Номенклатура': 'name',
            'Номенклатура.Вид номенклатуры': 'level_2',
            'Номенклатура.Марка стали': 'Марка_стали'
        }
        data = data.rename(columns=rename_columns)
        data['level_1'] = 'Металлопрокат'
        data['level_3'] = 'Прочие'
        data['level_3'] = data['level_3'].\
            where(~data['Марка_стали'].str.contains(r'ст3|Ст3', regex=True), 'Ст3').\
            where(~data['Марка_стали'].str.contains(r'09Г2С|09г2с|С345|С345|C345|c345', regex=True), r'С345\09Г2С')
        data['Сортамент'] = data.iloc[:, :3].apply(lambda x: create_sortam(x), axis=1)
        logging.info('Металл загрузился')

    elif kind == 2:
        rename_columns = {
            'Номенклатура': 'name',
            'Номенклатура.Вид номенклатуры': 'level_2',
            'Номенклатура.Толщина покрытия (только для ТД)': 'Покрытие'
        }
        data = data.rename(columns=rename_columns)
        data['level_1'] = 'Метизы'
        data['level_3'] = 'Прочие'
        data['level_3'] = data['level_3']. \
            where(data['Покрытие'] == '', 'ТД'). \
            where(~data['name'].str.contains(r'ГЦ|Гц', regex=True), 'ГЦ')

        # в новых наименованиях метизов очень сложно выделить именно сортамент
        # в разных типах метизов сортамент разный в отличие от металла
        # из-за этого, я пошел по пути строгого определения нужного сортамента для индекса
        data['Сортамент'] = 'Прочие'
        data['Сортамент'] = data['Сортамент']. \
            where(~data['name'].str.contains(r'Болт М14-6gх50', regex=True), 'Болт М14х50'). \
            where(~data['name'].str.contains(r'Болт М16-6gх55', regex=True), 'Болт М16х55'). \
            where(~data['name'].str.contains(r'Болт М20-6gх200', regex=True), 'Болт М20х200'). \
            where(~data['name'].str.contains(r'Болт М20-6gх60', regex=True), 'Болт М20х60'). \
            where(~data['name'].str.contains(r'Болт М20-6gх65', regex=True), 'Болт М20х65'). \
            where(~data['name'].str.contains(r'Болт М24-6gх70', regex=True), 'Болт М24х70'). \
            where(~data['name'].str.contains(r'Болт М24-6gх75', regex=True), 'Болт М24х75'). \
            where(~data['name'].str.contains(r'Болт М30-6gх110', regex=True), 'Болт М30х110'). \
            where(~data['name'].str.contains(r'Болт М36-6gх120', regex=True), 'Болт М36х120'). \
            where(~data['name'].str.contains(r'Болт М36-6gх130', regex=True), 'Болт М36х130'). \
            where(~data['name'].str.contains(r'Гайка М20', regex=True), 'Гайка М20'). \
            where(~data['name'].str.contains(r'Гайка М24', regex=True), 'Гайка М24'). \
            where(~data['name'].str.contains(r'Гайка М30', regex=True), 'Гайка М30'). \
            where(~data['name'].str.contains(r'Гайка М36', regex=True), 'Гайка М36'). \
            where(~data['name'].str.contains(r'Гайка М42', regex=True), 'Гайка М42'). \
            where(~data['name'].str.contains(r'Гайка М48', regex=True), 'Гайка М48'). \
            where(~data['name'].str.contains(r'Гайка М56-6h.11', regex=True), 'Гайка М56.11'). \
            where(~data['name'].str.contains(r'Шпилька Ш1', regex=True), 'Шпилька Ш1'). \
            where(~data['name'].str.contains(r'Шпилька Ш2', regex=True), 'Шпилька Ш2')
        logging.info('Метизы загрузились')

    else:
        ValueError('Parameter kind receive only 2 values: 1 or 2')

    data = data[[
        'name', 'level_1',
        'level_2', 'level_3',
        'Сортамент'
    ]]
    return data


def create_sortam(x: Series) -> str:
    """Создание сортамента из наименования, вида и госта"""
    nom, vid, gost = str(x[0]).strip(), str(x[1]).strip(), str(x[2]).strip()
    if '' in [nom, vid, gost]:
        return ''

    size = re.search(f'{vid}(\s*.+)\s*{gost}', nom).group(1)

    return vid + size.rstrip()


def prepare_inputs(path: str) -> DataFrame:
    """Prepare input materials data
    Подготавливает данные о поступлениях материалов

    :param path: file path
    """
    data = read_csv(
        path,
        sep='\t',
        encoding='ansi'
    )
    data = data.rename(columns={
        'Документ': 'Ссылка',
        'Номенклатура': 'Номенклатура',
        'Ед. изм.': 'Ед_Изм',
        'Контрагент': 'Контрагент',
        'Дата': 'Дата',
        'Количество закупок': 'Количество',
        'Сумма закупок': 'Всего'
    })
    data['Количество'] = modify_col(
        data['Количество'],
        instr=1, space=1, comma=1, numeric=1
    )
    data['Всего'] = modify_col(
        data['Всего'],
        instr=1, space=1, comma=1, numeric=1
    )
    data = data[(data['Всего'] > 0) & (data['Ссылка'] != 'Итого')]

    if path == PATH_NEW_INPUTS_CGC:  # если цинк, то только 2 номенклатуры
        data = data[data['Номенклатура'].isin(['Цинк ЦВ', 'Цинк ЦВО'])]

    return data


def modify_col(col: Series, instr=0, space=0, comma=0, numeric=0, minus=0) -> Series:
    """Изменяет колонку в зависимости от вида:

    :param col: колнка, которую нужно поменять
    :param instr: если 1, то в стринговое значение
    :param space: если 1, то удаляет пробелы
    :param comma: если 1, то заменяет запятые на точки в цифрах
    :param numeric: если 1, то в число с точкой
    :param minus: если 1, то минусовое число в ноль
    """
    if instr == 1:
        col = col.map(str)
    if space == 1:
        col = col.map(del_space)
    if comma == 1:
        col = col.map(replace_comma)
    if numeric == 1:
        col = col.map(float)
    if minus == 1:
        col = col.map(lambda x: 0 if x < 0 else x)
    return col


def del_space(x: str) -> str:
    """Удаление пробелов."""
    return re.sub(r'\s', '', x)


def replace_comma(x: str) -> str:
    """Меняет запятую на точку"""
    return x.replace(',', '.')


def prepare_dicts_elektrod() -> DataFrame:
    """Prepare part of new dict_levels.xlsx with elecktrod data"""
    data = read_csv(
        PATH_NEW_INPUTS_ELEKTROD,
        sep='\t',
        encoding='ansi'
    )

    data['level_1'] = 'Прочие материалы'
    data['level_2'] = 'Проволка сварочная, электроды'
    data['level_3'] = 'Прочие'
    data['Сортамент'] = 'Прочие'

    data = data\
        [data['Документ'] != 'Итого'].\
        rename(columns={'Номенклатура': 'name'})

    data = data[[
        'name', 'level_1',
        'level_2', 'level_3',
        'Сортамент'
    ]].drop_duplicates()

    return data


def prepare_dicts_zinc() -> DataFrame:
    """Prepare part of new dict_levels.xlsx with zinc data"""
    data = read_csv(
        PATH_NEW_INPUTS_CGC,
        sep='\t',
        encoding='ansi'
    )

    data['level_1'] = 'Прочие материалы'
    data['level_2'] = 'Цинк'
    data['level_3'] = 'Прочие'
    data['Сортамент'] = 'Прочие'

    data = data\
        [(data['Документ'] != 'Итого') & (data['Номенклатура'].isin(['Цинк ЦВ', 'Цинк ЦВО']))].\
        rename(columns={'Номенклатура': 'name'})

    data = data[[
        'name', 'level_1',
        'level_2', 'level_3',
        'Сортамент'
    ]].drop_duplicates()

    return data
