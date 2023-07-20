from util.logging_util import init_logger
from util import file_util as fu
from config import project_config as config
from util.mysql_util import MySQLUtil, get_processed_files
from model.retail_ordrs_model import OrdersModel, OrdersDetailModel

# 1 init_logger
# 2 build_db_util 生成数据库工具对象
# 	metadata_db_util
# 	target_db_util
# 3 获取待处理文件名
# 	1 读取json目录下的文件生成一个列表
# 	2 读取元数据库中file_monitor中已经处理过的文件，生成一个列表
# 	3 这两个列表相减，得到需要处理的文件
logger = init_logger()
logger.info("读取JSON数据处理，程序开始执行了......")

files = fu.get_dir_files_list(config.json_data_root_path, recursive=False)
logger.info(f"判断json的文件夹，发现有如下文件：{files}")

db_util = MySQLUtil()
processed_files = get_processed_files(db_util)
logger.info(f"查询MySQL，找到有如下文件已经被处理过了：{processed_files}")

need_to_process_files = fu.get_new_by_compare_lists(files, processed_files)
logger.info(f"经过对比mysql元数据库，找出如下文件供我们处理：{need_to_process_files}")

# 步骤二 写csv文件
# 	1 传入json文件名名，读取数据生成模型对象，对象中存储了json数据
# 	2 进行过滤
# 	3 写入到csv文件
# 	循环取出每个模型(每个模型承载一行数据)，调用to_csv方法，得到字符串，写入文件

# 被处理的文件信息记录
processed_files_record_dict = {}
target_util = MySQLUtil(
    host=config.target_host,
    port=config.target_port,
    user=config.target_user,
    password=config.target_password,
    database=config.target_database)

for file_name in need_to_process_files:
    target_util = MySQLUtil(
        host=config.target_host,
        port=config.target_port,
        user=config.target_user,
        password=config.target_password,
        database=config.target_database)
    file_processed_lines_count = 0
    # 存储所有的订单模型对象
    order_model_list = []
    # 存储所有的订单详情模型对象
    order_detail_model_list = []
    # 4.2 根据读取的json字符串生成OrderModel OrderDetailModel
    for line in open(file_name, 'r', encoding='utf-8'):
        file_processed_lines_count += 1

        order_model = OrdersModel(data=line)
        order_detail_model = OrdersDetailModel(data=line)

        order_model_list.append(order_model)
        order_detail_model_list.append(order_detail_model)

        # 4.3 对数据进行过滤 过滤掉测试数据
    reserved_model = []
    for model in order_model_list:
        if model.receivable <= 1000:
            reserved_model.append(model)

    # 4.4 把得到的模型中的数据写入到csv
    order_csv_write_f = open(
        file=config.retail_output_csv_root_path + config.retail_orders_output_csv_file_name,
        mode='a',
        encoding='utf-8'
    )

    for model in reserved_model:
        line = model.to_csv()
        order_csv_write_f.write(line)
        order_csv_write_f.write('\n')
    order_csv_write_f.close()

    # 订单详情写入到csv
    order_detail_csv_write_f = open(
        file=config.retail_output_csv_root_path +
             config.retail_orders_output_csv_file_name,
        mode='a', encoding='utf-8')

    for model in order_detail_model_list:
        line = model.to_csv()
        order_detail_csv_write_f.write(line)

    order_detail_csv_write_f.close()

    processed_files_record_dict[file_name] = file_processed_lines_count
    logger.info(f"完成了CSV备份文件的写出，写出到了：{config.retail_output_csv_root_path}")

    # 4.5 把得到的模型中的数据写入到sql
    # 判断order order_detail两张表是否存在
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

    # order表写入到目的地
    for i, model in enumerate(reserved_model):
        sql = model.generate_insert_sql()

        target_util.select_db(config.target_database)
        target_util.execute_without_commit(sql)
        # 每1000次提交一次
        if (i + 1) % 1000 == 0:
            target_util.conn.commit()
    # 提交零头
    target_util.conn.commit()

    # order_detail
    for i, model in enumerate(order_detail_model_list):
        sql = model.generate_insert_sql()
        target_util.select_db(config.target_database)
        target_util.execute_without_commit(sql)
        if (i + 1) % 1000 == 0:
            target_util.conn.commit()

    target_util.conn.commit()
    target_util.conn.close()

global_count = sum(processed_files_record_dict.values())
logger.info(f"完成了CSV备份文件的写出，写出到了：{config.retail_output_csv_root_path}")
logger.info(f"完成了向MySQL数据库中插入数据的操作。"
            f"共处理了：{global_count}条数据")

metadata_util = MySQLUtil()
# 取出这个存放别处理过的文件名的字典中 的 key 和value，也就是文件名和 一个文件中有几条数据
for file_name, processed_lines in processed_files_record_dict.items():
    insert_sql = f"INSERT INTO {config.metadata_file_monitor_table_name}(file_name, process_lines) " \
                 f"VALUES('{file_name}', {processed_lines})"
    metadata_util.execute_with_commit(insert_sql)

metadata_util.close_conn()
logger.info("读取JSON数据想MYSQL插入以及写出CSV备份，程序执行完成......")
