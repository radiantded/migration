import datetime
import logging
import os
import sys
import time
import uuid
import threading
from logging.handlers import RotatingFileHandler

import psycopg2
from psycopg2 import extras
from dbfread import DBF
from PIL import Image

PACKAGE = '101'
DB_FILE = '00000001'
TIME_FORMAT = "%Y-%m-%d %H:%M:%S"
IMPORT_DIRECTORY = "C:\\Dev\\"
DESTINATION = 'C:\\Dev\\export\\'
NFS = '/mnt/nfs/data/datamart/'
NSERS = []
MAPPING = {}
GOODS_DB_NAME = 'MD_GOOD{}.DBF'
ERROR_STRING = '\033[31m Ошибка {} в файле {} в строке {}: {} \033[0m'

# Данные для заполнения БД
CERTIFICATES = []
NODES = []
SEARCH_ATTRS = []
STRUCTURE_ID = '100'
STATE = '0'
UNKNOWN_DATE = '0001-01-01 00:00:00'
VERSION = '1'
STORAGE_OBJECTS_QUERY = """
    INSERT INTO "Objects"
    ("Number", "Kind", "ParentNumber", "StructureID",
    "UpdateDate", "CreatedDate", "State",
    "OperStoragePeriod", "TempStoragePeriod",
    "ClassType", "LastStoragePeriod", "Version",
    "Received")
    VALUES (%s, %s, %s, %s,
    %s, %s, %s, %s, %s, %s,
    %s, %s, %s);
"""
STORAGE_OBJECTS = []
NODE_QUERY = """
    INSERT INTO "SearchAttributes"
    ("ID", "Name", "ParentNumber", "ParentAttrId",
    "Kind", "CreatedBy", "CreatedDate", "GuidValue")
    VALUES
    (%s, %s, %s, %s, %s, %s, %s, %s);
"""
NODE_VALUES = []
NODE_NO_PARENT_QUERY = """
    INSERT INTO "SearchAttributes"
    ("ID", "Name", "ParentNumber",
    "Kind", "CreatedBy", "CreatedDate", "GuidValue")
    VALUES
    (%s, %s, %s, %s, %s, %s, %s);
"""
NODE_NO_PARENT_VALUES = []
ATTRS_QUERY = """
    INSERT INTO "SearchAttributes"
    ("ID", "CreatedDate", "ParentNumber",
    "ParentAttrId", "Name", "Kind",
    "TextValue", "IntValue", "DateValue",
    "GuidValue", "CreatedBy")
    VALUES
    (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""
ATTRS_VALUES = []
CREATED_BY = 'EA_Migration_{}'.format(PACKAGE)
NONETYPE = type(None)
TMK_COUNT = 0
WK_COUNT = 0
MDRD_COUNT = 0
APL_COUNT = 0
APLCERT_COUNT = 0

# Настройки логгирования
CONSOLE_HANDLER = logging.StreamHandler()
LOG_HANDLER = RotatingFileHandler(
    '{}sync/logs/{}_{}.log'.format(
        IMPORT_DIRECTORY, PACKAGE,
        datetime.datetime.now().strftime("%Y-%m-%d_%H-%M")
    ),
    mode='a', maxBytes=50*1024*1024,
    backupCount=100, encoding='utf-8', delay=0)
logging.basicConfig(
    handlers=(LOG_HANDLER, CONSOLE_HANDLER),
    format='[%(asctime)s | %(levelname)s]: %(message)s',
    datefmt=TIME_FORMAT,
    level=logging.INFO
)
DATA_TYPES = {
    NONETYPE: [0, 'TextValue'],
    str: [0, 'TextValue'], int: [1, 'IntValue'],
    float: [2, ' FloatValue'],
    datetime.date: [3, 'DateValue'],
    datetime.datetime: [3, 'DateValue'],
    bool: [4, 'BoolValue'],
    uuid.UUID: [6, 'GuidValue']
}


def time_test(func):
    """Функция декоратор, измеряет время выполнения функций."""
    def f(*args):
        t1 = time.time()
        res = func(*args)
        t2 = time.time()
        result = round(int(t2 - t1) / 60, 1)
        logging.info(
            'Время выполнения функции {}: {} минут'.format(
                func.__name__, result))
        return res
    return f


def connect_to_database(local=True):
    connection = psycopg2.connect(
        dbname=os.environ.get(
            'local_dbname' if local else 'dbname'
        ),
        user=os.environ.get(
            'local_user' if local else 'user'
        ),
        password=os.environ.get(
            'local_password' if local else 'password'
        ),
        host=os.environ.get(
            'local_host' if local else 'host'
        ),
        port=os.environ.get(
            'local_port' if local else 'port'
        )
    )
    logging.info('Соединение установлено')
    cursor = connection.cursor()
    return connection, cursor


def create_storage_obj(kind, parent_number=None, retro_date=None, uid=None):
    date = datetime.datetime.now()
    storage_object_number = "{}".format(str(uuid.uuid1()) if not uid else uid)
    STORAGE_OBJECTS.append(
        [storage_object_number, kind,
         parent_number if parent_number else '-ROOT-', STRUCTURE_ID,
         date, date, STATE, UNKNOWN_DATE, UNKNOWN_DATE,
         PACKAGE, UNKNOWN_DATE, VERSION, retro_date]
        )
    logging.info(
        'Объект хранения {} успешно создан'.format(storage_object_number))
    return storage_object_number


def create_node(parent_number, table_name, parent_attr_id=None, uid=None):
    kind = 5
    date = datetime.datetime.now()
    node_id = "{}".format(str(uuid.uuid1()) if not uid else uid)
    if parent_attr_id:
        NODE_VALUES.append(
            [node_id, table_name, parent_number,
             parent_attr_id, kind, CREATED_BY,
             date, node_id]
        )
    else:
        NODE_NO_PARENT_VALUES.append(
            [node_id, table_name, parent_number,
             kind, CREATED_BY, date, node_id]
        )
    logging.info(
        'Узел таблицы {} - {} объекта {} успешно создан'.format(
            table_name, node_id, parent_number)
        )
    return node_id


def create_attrs(root_storage_obj_id, node_string_id=None,
                 record=None, table_name=None, parent_node_id=None,
                 main_table_id=None, mode=None):
    date = datetime.datetime.now()
    tables = {
        'RUTrademark': {
            'rutmk_uid': uuid.UUID(
                node_string_id) if node_string_id else None,
            'appl_doc_link': None,
            'appl_ui_link': None,
            'appl_type': None,
            'appl_receiving_date': None,
            'appl_number': 'NAP',
            'appl_date': 'DAP',
            'reg_ui_link': None,
            'reg_doc_link': None,
            'reg_number': 'NTM',
            'reg_date': 'DPUB',
            'reg_country': 'CU',
            'reg_publ_number': None,
            'reg_publ_date': None,
            'status_code': 'SDACT',
            'status_date': 'SDIZM',
            'expiry_date': 'DEX',
            'seniority_priority_number': None,
            'seniority_priority_date': None,
            'priority_date': 'DAPK',
            'exhib_priority_date': 'DAPV',
            'divisional_appl_number': 'NPARENT',
            'divisional_appl_date': None,
            'other_date': None,
            'corr_address': 'MAIL2',
            'corr_address_country': None,
            'applicants': None,
            'applicants_count': None,
            'holders': 'OWN2',
            'holders_count': None,
            'representatives': 'NPP',
            'representatives_count': None,
            'representative_number': 'KPP',
            'representatives_term': None,
            'users': None,
            'users_count': None,
            'mark_category': None,
            'representation_names': 'IMAGE_NAME',
            'search_result': None,
            'goods': 'GS',
            'prev_reg_number': None,
            'prev_reg_date': None,
            'prev_reg_country': None,
            'feature_description': None,
            'disclaimers': None,
            'association_marks': None,
            'payment': None,
            'records': None,
            'corr_type': None,
            'corr_method': None,
            'sheets_count': None,
            'image_sheets_count': None,
            'payment_doc_count': None,
            'is_external_search': None,
            'outgoing_correspondence': None,
            'responsible_expert': 'EXPRTNAME',
            'retro_number': 'NSER',
            'update_time': date,
            'delete_time': None
        },
        
    wrong_values = ['TM_DAT__', 'NSER', 'NAP', 'NAPTW',
                    'TWICE', 'DAP', 'CU', 'IS', 'WCD']
    try:
        for attr, attr_value in tables[table_name].items():
            value = record[
                attr_value] if attr_value in record.keys() else attr_value
            uid = uuid.uuid1()
            attr_id = "{}".format(str(uid))
            data_type = type(value)
            if data_type == str:
                value.encode('ascii', errors='ignore').decode()
                if "'" in value:
                    wrong_value = value.split("'")
                    value = "`".join(wrong_value)
                elif value == '' or value in wrong_values:
                    value = None
                ATTRS_VALUES.append(
                    [attr_id, date, root_storage_obj_id,
                     node_string_id, attr,
                     DATA_TYPES[data_type][0], value,
                     None, None, None, CREATED_BY]
                )
            elif data_type == int:
                ATTRS_VALUES.append(
                    [attr_id, date, root_storage_obj_id,
                     node_string_id, attr,
                     DATA_TYPES[data_type][0], None,
                     value, None, None, CREATED_BY]
                )
            elif data_type == NONETYPE:
                ATTRS_VALUES.append(
                    [attr_id, date, root_storage_obj_id,
                     node_string_id, attr,
                     DATA_TYPES[data_type][0], None,
                     None, None, None, CREATED_BY]
                )
            elif data_type == datetime.date or data_type == datetime.datetime:
                ATTRS_VALUES.append(
                    [attr_id, date, root_storage_obj_id,
                     node_string_id, attr,
                     DATA_TYPES[data_type][0], None,
                     None, value, None, CREATED_BY]
                )
            elif data_type == uuid.UUID:
                value = str(value)
                ATTRS_VALUES.append(
                    [attr_id, date, root_storage_obj_id,
                     node_string_id, attr,
                     DATA_TYPES[data_type][0], None,
                     None, None, value, CREATED_BY]
                )
    except Exception as ex:
        exc_type, _, exc_tb = sys.exc_info()
        file_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        logging.error(
            ERROR_STRING.format(exc_type, file_name, exc_tb.tb_lineno, ex))
    logging.info(
        'Атрибуты таблицы {} - {} объекта {} успешно созданы'.format(
            table_name, node_string_id, root_storage_obj_id))


@time_test
def get_objects():
    conn, cur = connect_to_database(local=False)
    query = """
        SELECT * FROM "Objects"
        WHERE "ClassType" BETWEEN '100' AND '800'
    """
    cur.execute(query)
    objects = cur.fetchall()
    conn.close()
    return objects


OBJECTS = get_objects()


@time_test
def get_attrs():
    conn, cur = connect_to_database(local=False)
    query = """
        SELECT * FROM "SearchAttributes"
        WHERE "CreatedBy" LIKE 'EA_Migration_%'
        AND "Name" IN ('retro_number', 'appl_number')
    """
    cur.execute(query)
    attrs = cur.fetchall()
    conn.close()
    return attrs


ATTRS = get_attrs()


def delete_storage_objects(storage_objects_id):
    conn, cur = connect_to_database(local=False)
    query = """
        DELETE FROM "Objects"
        WHERE "Number" = '{}'
    """.format(storage_objects_id)
    cur.execute(query)
    conn.commit()
    conn.close()


def delete_image(file_path, storage_object_id):
    folders = file_path[0].split('/')
    path = '/'
    for folder in folders:
        path += folder + '/'
        if folder == uuid.UUID(storage_object_id).hex:
            break
    os.system('rm -r {}'.format(path))
    logging.info(
        '\033[33m Директория с файлом {} удалена \033[0m'.format(file_path))
    return


def create_contact(root_storage_obj_id, parent_table_id,
                   mode, record, table_name):
    Contact_id = create_node(
        root_storage_obj_id, 'Contact', parent_table_id
    )
    create_attrs(
        root_storage_obj_id, Contact_id, record, 'Contact', mode=mode
    )
    ContactAddress_id = create_node(
        root_storage_obj_id, 'ContactAddress', Contact_id
    )
    create_attrs(
        root_storage_obj_id, ContactAddress_id, record,
        'ContactAddress', Contact_id, mode=mode
    )
    ContactName_id = create_node(
        root_storage_obj_id, 'ContactName', Contact_id
    )
    create_attrs(
        root_storage_obj_id, ContactName_id, record,
        'ContactName', Contact_id, mode=mode
    )
    ContactType_id = create_node(
        root_storage_obj_id, table_name, parent_table_id
    )
    create_attrs(
        root_storage_obj_id, ContactType_id, record,
        table_name, parent_table_id, Contact_id, mode=mode
    )
    logging.info(
        'Контакт типа {} таблицы {} - {} объекта {} успешно создан'.format(
            mode, table_name, parent_table_id, root_storage_obj_id)
    )


def import_image(image_path, date, root_storage_obj_id,
                 storage_obj_id, nser, image_name, image_type,
                 convert=False):
    try:
        extensions = {'TIFF': 'TIF',
                      'JPEG': 'JPG'}
        if convert:
            image = Image.open(image_path).convert('RGB')
            image_path = '.'.join(
                [image_path.split(".")[0], extensions[image_type]]
            )
            image.save(image_path, 'JPEG', quality=80)
            image.close()
        image = Image.open(image_path)
        height, width = image.size
        image.close()
        file_id = uuid.UUID(storage_obj_id)
        new_filename = '{}_1_{}.{}'.format(
            file_id.hex, image_name, extensions[image_type]
        )
        folders = ['{}'.format(date.strftime("%Y")),
                   '{}'.format(date.strftime("%m")),
                   '{}'.format(date.strftime("%d")),
                   '{}'.format(str(root_storage_obj_id).replace('-', '')),
                   '{}'.format(str(storage_obj_id).replace('-', '')),
                   'TRADEMARK_IMAGE']
        path = '{}img_data\\'.format(DESTINATION) + "\\".join(folders)
        final_path = NFS + '/'.join(folders) + '/'
        os.makedirs(path, exist_ok=True)
        os.system('cp {} {}'.format(image_path, path))
        os.system(
            f'ren {path}\\{image_name}.{extensions[image_type]} {new_filename}'
        )
        logging.info('Файл {}{} создан'.format(final_path, new_filename))
        keys = ['file_path', 'file_name', 'file_type',
                'content', 'height', 'width']
        values = [final_path + new_filename, new_filename,
                  image_type, file_id, height, width]
        file_data = dict(zip(keys, values))
    except Exception as ex:
        exc_type, _, exc_tb = sys.exc_info()
        file_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        logging.error(
            ERROR_STRING.format(exc_type, file_name, exc_tb.tb_lineno, ex)
        )
    return file_data


@time_test
def execute_query_list(query, values_list):
    conn, cur = connect_to_database(local=False)
    conn.set_session(autocommit=True)
    logging.info('Загрузка {} строк'.format(len(values_list)))
    extras.execute_batch(cur, query, values_list, 2000)
    conn.close()


def appellation_number_check(ntm_number):
    for row in ATTRS:
        if ntm_number == row[4]:
            parent_attr_id = row[12]
            return parent_attr_id
    return


def create_main_tables(record, kind, table_prefix,
                       old_root_obj_id=None, table_id=None,
                       inner_obj_id=None,
                       rewrite=False):
    nser = record.get('NSER')
    retro_date = record.get('DAP')
    image_name = record.get('IMAGE_NAME')
    image_path = record.get('IMAGE_PATH')
    image_type = record.get('IMAGE_TYPE')
    ntm = record.get('NTM')
    if record.get('GOODS'):
        record['GOODS'] = ' '.join(record.get('GOODS').split())
    date = datetime.datetime.now() if not retro_date else retro_date
    root_obj_id = create_storage_obj(
        kind, retro_date=retro_date,
        uid=old_root_obj_id if rewrite
        else None
    )
    modes = {
        'RUApl': ['RUAppellation'],
        'RUAplCert': ['RUAppellationCertificate'],
        'RUTmk': ['RUTrademark', 'RUTrademarkRepresentationFile'],
        'WKTmk': ['WKTrademark', 'WKTrademarkRepresentationFile'],
        'MadridTmk': ['MadridTrademark', 'MadridTrademarkRepresentationFile'],
    }
    table_name = modes[table_prefix][0]
    if table_prefix == 'RUAplCert':
        record_exists = appellation_number_check(ntm[:-2] + '00')
        if record_exists:
            root_table_id = record_exists
        else:
            root_table_id = create_node(
                root_obj_id, table_name,
                uid=table_id if rewrite else None
            )
    else:
        root_table_id = create_node(
            root_obj_id, table_name,
            uid=table_id if rewrite else None
        )
    create_attrs(
        root_obj_id, root_table_id,
        record, table_name
    )
    if table_prefix in ('RUTmk', 'WKTmk', 'MadridTmk'):
        table_name = table_prefix + 'Priority'
        TmkPriority_id = create_node(
            root_obj_id, table_name,
            root_table_id
        )
        create_attrs(
            root_obj_id, TmkPriority_id,
            record, table_name,
            root_table_id
        )
        table_name = table_prefix + 'GoodsServices'
        TmkGoodsServices_id = create_node(
            root_obj_id,
            table_name,
            root_table_id
        )
        create_attrs(
            root_obj_id, TmkGoodsServices_id,
            record, table_name, root_table_id
        )
    if table_prefix in ('RUTmk', 'WKTmk', 'MadridTmk', 'RUApl'):
        table_name = table_prefix + 'Disclaimer'
        TmkDisclaimer_id = create_node(
            root_obj_id, table_name,
            root_table_id
        )
        create_attrs(
            root_obj_id, TmkDisclaimer_id,
            record, table_name,
            root_table_id
        )
    CorrespondenceAddress_id = create_node(
        root_obj_id, 'CorrespondenceAddress',
        root_table_id
    )
    create_attrs(
        root_obj_id, CorrespondenceAddress_id,
        record, 'CorrespondenceAddress'
    )
    table_name = table_prefix + 'CorrespondenceAddress'
    TmkCorrespondenceAddress_id = create_node(
        root_obj_id,
        table_name
    )
    create_attrs(
        root_obj_id, TmkCorrespondenceAddress_id,
        record, table_name,
        root_table_id,
        main_table_id=CorrespondenceAddress_id
    )
    if record.get('OWN') != '':  # Holder
        if table_prefix != 'RUApl':
            mode = 'holder'  # holder и applicant дублируются
            create_contact(root_obj_id, root_table_id, mode,
                           record, table_prefix + 'Holder')
        mode = 'applicant'
        create_contact(root_obj_id, root_table_id, mode,
                       record, table_prefix + 'Applicant')
    if record.get('NPP') != '':  # Representative
        mode = 'representative'
        create_contact(root_obj_id, root_table_id, mode,
                       record, table_prefix + 'Representative')
    if image_path:
        convert = False  # делаем TIF и JPEG
        if rewrite:
            old_uid = inner_obj_id if inner_obj_id else None
        storage_object_id = create_storage_obj(
            kind, root_obj_id, retro_date, old_uid if rewrite else None)
        for _ in range(2):
            WKTrademarkRepresentationFile_id = create_node(
                storage_object_id, modes[table_prefix][1], root_table_id
            )
            file_data = import_image(
                image_path, date, root_obj_id, storage_object_id,
                nser, image_name, image_type, convert
            )
            create_attrs(
                storage_object_id, WKTrademarkRepresentationFile_id,
                file_data, modes[table_prefix][1], root_table_id
            )
            convert = True
            image_type = 'JPEG'


def thread(record):
    try:
        nser = record.get('NSER')
        rewrite = False
        for row in ATTRS:
            if nser == row[5]:
                old_root_obj_id = row[1]
                table_id = row[12]
                inner_obj_id = None
                for row in OBJECTS:
                    if old_root_obj_id == row[6]:
                        inner_obj_id = row[0]
                        delete_storage_objects(inner_obj_id)
                        break
                delete_storage_objects(old_root_obj_id)
                rewrite = True
                break
        ntm = record.get('NTM')
        if ntm[:3] == '999':  # WKTrademark
            global WK_COUNT
            WK_COUNT += 1
            kind = 100010
            record['NTM'] = ntm[3:]
            table_prefix = 'WKTmk'
        elif record['IS'] == 'I':  # Madrid
            global MDRD_COUNT
            MDRD_COUNT += 1
            kind = 100001
            table_prefix = 'MadridTmk'
        elif record.get('WCD') == 'N':
            kind = 100002
            if ntm[-2:] == '00':  # RUAppellation
                global APL_COUNT
                APL_COUNT += 1
                table_prefix = 'RUApl'
            else:  # RuAppellationCertificate
                global APLCERT_COUNT
                APLCERT_COUNT += 1
                table_prefix = 'RUAplCert'
                logging.warning('ПНМПТ: {}'.format(nser))
        else:  # RUTrademark
            global TMK_COUNT
            TMK_COUNT += 1
            kind = 100001
            table_prefix = 'RUTmk'
        create_main_tables(
            record, kind, table_prefix,
            old_root_obj_id if rewrite else None,
            table_id if rewrite else None,
            inner_obj_id if rewrite else None,
            rewrite
        )
        logging.info('Обработка серийного номера {} завершена'.format(nser))
    except Exception as ex:
        exc_type, _, exc_tb = sys.exc_info()
        file_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        logging.error(
            ERROR_STRING.format(exc_type, file_name, exc_tb.tb_lineno, ex)
        )
    return


@time_test
def import_data(big_dict):
    try:
        threads = []
        for nser in big_dict:
            t = threading.Thread(
                target=thread,
                name='Обработка серийного номера {}'.format(nser),
                args=[big_dict[nser]]
            )
            threads.append(t)
            t.start()
            logging.info('{} стартовала'.format(t.name))
        logging.info('Ждём завершения всех потоков')
        for t in threads:
            t.join()
    except Exception as ex:
        exc_type, _, exc_tb = sys.exc_info()
        file_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        logging.error(
            ERROR_STRING.format(exc_type, file_name, exc_tb.tb_lineno, ex)
        )


@time_test
def collect_data(directory):
    start = 0
    end = 1000000
    logging.info('Подготовка записей {}-{}'.format(start, end))
    try:
        goods_name_template = 'MD_GOOD{}.DBF'
        mains_path = os.path.join(directory, "MD_MAINS.DBF")
        collected_data_dict = {
            r['NSER']: dict(r) for r in sorted(
                DBF(mains_path, ignore_missing_memofile=True),
                key=lambda x: x['NSER']) if start <= r['NSER'] < end
        }
        file_types = {'TIF': 'TIFF',
                      'JPG': 'JPEG'}
        for num in range(9):
            dbf_name = goods_name_template.format(
                "S" if num == 0 else str(num)
            )
            dbf_path = os.path.join(
                directory, dbf_name)
            if os.path.isfile(dbf_path):
                goods = [
                    r for r in DBF(
                        dbf_path) if start <= r['NSER'] < end
                ]
                for r in goods:
                    collected_data_dict[r['NSER']]['GOODS'] = r['GOODS']
        for root, _, files in os.walk(directory + "\\IMG\\"):
            for file in files:
                if '.TIF' in file:
                    image_path = os.path.join(root, file)
                    name, ext = file.split('.')
                    if start <= int(name) < end:
                        collected_data_dict[int(name)]['IMAGE_PATH'] = image_path
                        collected_data_dict[int(name)]['IMAGE_NAME'] = name
                        collected_data_dict[int(name)]['IMAGE_TYPE'] = file_types[ext]
    except Exception as ex:
        exc_type, _, exc_tb = sys.exc_info()
        file_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        logging.error(
            ERROR_STRING.format(exc_type, file_name, exc_tb.tb_lineno, ex)
        )
    return collected_data_dict


@time_test
def migrate():
    collected_data_dict = collect_data("{}{}".format(IMPORT_DIRECTORY, DB_FILE))
    import_data(collected_data_dict)
    logging.info('Заливаем объекты хранения')
    execute_query_list(STORAGE_OBJECTS_QUERY, STORAGE_OBJECTS)
    logging.info('Заливаем узлы без значений')
    execute_query_list(NODE_NO_PARENT_QUERY, NODE_NO_PARENT_VALUES)
    logging.info('Заливаем узлы со значениями')
    execute_query_list(NODE_QUERY, NODE_VALUES)
    logging.info('Заливаем атрибуты')
    execute_query_list(ATTRS_QUERY, ATTRS_VALUES)
    logging.info('Миграция завершена')


if __name__ == '__main__':
    migrate()
