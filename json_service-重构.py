from util.logging_util import init_logger
from util.file_util import get_dir_files_list, get_new_by_compare_lists
from config import project_config as config
from util.mysql_util import MySQLUtil, get_processed_files
from model.retail_ordrs_model import OrdersModel, OrdersDetailModel

logger = init_logger()


def bulid_util():
    db_util = MySQLUtil()
    target_util = MySQLUtil(
        host=config.target_host,
        port=config.target_port,
        user=config.target_user,
        password=config.target_password,
        database=config.target_database)
    return db_util, target_util


def get_process_files(db_util):
    files = get_dir_files_list(config.json_data_root_path, recursive=False)
    logger.info(f"判断json的文件夹，发现有如下文件：{files}")
    processed_files = get_processed_files(db_util)
    logger.info(f"查询MySQL，找到有如下文件已经被处理过了：{processed_files}")
    need_to_process_files = get_new_by_compare_lists(files, processed_files)
    logger.info(f"经过对比mysql元数据库，找出如下文件供我们处理：{need_to_process_files}")
    return need_to_process_files


def build_model_list(filename):
    """
    获取指定文件中的json数据
    每一行json数据生成一个OrderModel和一个OrderDetailModel
    :param filename: json文件路径
    :return:
    """
    file_processed_lines_count = 0
    # 存储所有的订单模型对象
    order_model_list = []
    # 存储所有的订单详情模型对象
    order_detail_model_list = []
    # 4.2 根据读取的json字符串生成OrderModel OrderDetailModel
    for line in open(filename, 'r', encoding='utf-8'):
        file_processed_lines_count += 1

        order_model = OrdersModel(data=line)
        order_detail_model = OrdersDetailModel(data=line)

        order_model_list.append(order_model)
        order_detail_model_list.append(order_detail_model)

    return order_model_list, order_detail_model_list, file_processed_lines_count


def filte_except_data(order_model_list):
    reserved_models = []
    for model in order_model_list:
        if model.receivable <= 10000:
            reserved_models.append(model)

    return reserved_models


def get_order_csv_file():
    # 4.4 把得到的模型中的数据写入到csv
    order_csv_write_f = open(
        file=config.retail_output_csv_root_path + config.retail_orders_output_csv_file_name,
        mode='a',
        encoding='utf-8'
    )
    # 订单详情写入到csv
    order_detail_csv_write_f = open(
        file=config.retail_output_csv_root_path +
             config.retail_orders_output_csv_file_name,
        mode='a', encoding='utf-8')
    return order_csv_write_f, order_detail_csv_write_f


def write_model_data_to_csv(reserved_models, order_detail_model_list,
                            order_csv_write_f, order_detail_csv_write_f
                            ):
    for model in reserved_models:
        line = model.to_csv()
        order_csv_write_f.write(line)
        order_csv_write_f.write('\n')

    for model in order_detail_model_list:
        line = model.to_csv()
        order_detail_csv_write_f.write(line)


def close_order_csv_file(order_csv_write_f, order_detail_csv_write_f):
    order_csv_write_f.close()
    order_detail_csv_write_f.close()


def create_order_tables(target_util):
    if not target_util.check_table_exists(config.target_database, config.target_orders_table_name):
        target_util.create_table(
            config.target_database,
            config.target_orders_table_name,
            config.target_orders_table_create_cols
        )
    if not target_util.check_table_exists(config.target_database, config.target_orders_detail_table_name):
        target_util.create_table(
            config.target_database,
            config.target_orders_detail_table_name,
            config.target_orders_detail_table_create_cols
        )


def write_model_data_to_mysql(models):
    # order表写入到目的地
    for i, model in enumerate(models):
        sql = model.generate_insert_sql()

        target_util.select_db(config.target_database)
        target_util.execute_without_commit(sql)
        # 每1000次提交一次
        if (i + 1) % 1000 == 0:
            target_util.conn.commit()
    # 提交零头
    target_util.conn.commit()


def write_to_csv(reserved_models, order_detail_model_list):
    order_csv_write_f, order_detail_csv_write_f = get_order_csv_file()

    write_model_data_to_csv(
        reserved_models, order_detail_model_list,
        order_csv_write_f, order_detail_csv_write_f
    )
    close_order_csv_file(
        order_csv_write_f, order_detail_csv_write_f
    )


def write_to_mysql(reserved_model, order_detail_model_list):
    create_order_tables(target_util)
    write_model_data_to_mysql(reserved_model)
    write_model_data_to_mysql(order_detail_model_list)


def write_metadata_to_metadatabase(db_util, processed_files_record_dict):
    for file_name, processed_lines in processed_files_record_dict.items():
        insert_sql = f"INSERT INTO {config.metadata_file_monitor_table_name}(file_name, process_lines) " \
                     f"VALUES('{file_name}', {processed_lines})"
        db_util.execute_with_commit(insert_sql)
    logger.info("读取JSON数据向MySQL插入以及写出CSV备份，程序执行完成......")


def close_db_util(db_util, target_util):
    target_util.close_conn()
    db_util.close_conn()


if __name__ == '__main__':
    logger.info("读取JSON数据处理，程序开始执行了......")
    # 创建数据库
    db_util, target_util = bulid_util()
    # 筛选需要处理的文件
    need_to_process_files = get_process_files(db_util)
    # 读取需要处理的文件内容
    processed_files_record_dict = {}
    for filename in need_to_process_files:

        order_model_list, order_detail_model_list, file_processed_lines_count = \
            build_model_list(filename)
        # 过滤文件信息
        order_model_list = filte_except_data(order_model_list)
        # 写入csv
        write_to_csv(order_model_list, order_detail_model_list)
        logger.info(f"完成了CSV备份文件的写出，写出到了：{config.retail_output_csv_root_path}")
        # 写入sql文件
        write_to_mysql(order_model_list, order_detail_model_list)
        logger.info(f"完成了MYSQL写入")
        processed_files_record_dict[filename] = file_processed_lines_count

    global_count = sum(processed_files_record_dict.values())
    logger.info(f"完成了CSV备份文件的写出，写出到了：{config.retail_output_csv_root_path}")
    logger.info(f"完成了向MySQL数据库中插入数据的操作。"
                f"共处理了：{global_count}条数据")

    write_metadata_to_metadatabase(db_util, processed_files_record_dict)

    close_db_util(db_util, target_util)
