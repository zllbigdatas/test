import config.project_config as config
from model.backend_logs_model import BackendLogsModel
from util.file_util import get_dir_files_list, get_new_by_compare_lists
from util.logging_util import init_logger
from util.mysql_util import MySQLUtil, get_processed_files


def build_util():
    # 构建元数据库的util对象
    metadata_util = MySQLUtil()

    # 构建目的地数据库的util对象
    target_util = MySQLUtil(
        host=config.target_host,
        port=config.target_port,
        user=config.target_user,
        password=config.target_password,
        database=config.target_database)

    return metadata_util, target_util


def get_need_to_process_file(metadata_util):
    # 1、读取文件中待处理的文件有哪些
    files_name = get_dir_files_list(path=config.backend_logs_path)
    logger.info(f"指定路径：{config.backend_logs_path}下，有以下文件：{files_name}")

    # 2、读取元数据库中已处理的文件有哪些
    processed_file = get_processed_files(
        metadata_util,
        config.metadata_database,
        config.metadata_backend_logs_table_name,
        config.metadata_backend_logs_table_create_cols
    )
    logger.info(f"元数据库中被处理文件有：{processed_file}")

    # 3、获取哪些文件待处理
    need_process_file = get_new_by_compare_lists(files_name, processed_file)
    logger.info(f"待处理的文件有：{need_process_file}")
    # 4、判断有无待处理文件，没有则直接退出
    if len(need_process_file) == 0:
        logger.info(f"没有待处理的文件，程序退出......")
        exit(0)

    return need_process_file


def build_logs_model(need_process_file):
    # 定义一个列表，用于存储logs模型（对象）
    models_list = []
    # 定义一个字典，用于存储这批处理的文件名称（绝对路径）及该文件处理的行数
    process_dict = {}
    # 遍历待处理文件 构建模型
    for file in need_process_file:
        count = 0
        for line in open(file, 'r', encoding='utf-8'):
            count += 1
            line = line.strip()
            backend_logs_model = BackendLogsModel(data=line)
            models_list.append(backend_logs_model)
        # 记录被处理的文件名称及行数
        process_dict[file] = count
    return models_list, process_dict


def write_data_to_csv(models_list):
    # 创建写入到CSV文件中的文件对象
    logs_file = open(
        config.backend_logs_output_csv_root_path + config.backend_logs_output_csv_file_name,
        mode='a',
        encoding='utf-8')
    for i, model in enumerate(models_list):
        csv_line = model.to_csv()

        logs_file.write(csv_line)
        logs_file.write('\n')
        # 每1000条刷新缓存一次
        if (i + 1) % 1000 == 0:
            logs_file.flush()
            logger.info(f"已写入到：{logs_file.name}文件{i + 1}条数据......")
    # 提交零头
    logger.info(f"写入到：{logs_file.name}文件共{len(models_list)}条数据......")
    logs_file.close()


def check_target_logs_table_exists(target_util):
    if not target_util.check_table_exists(config.target_database, config.target_backend_logs_table_name):
        # 不存在，则创建
        target_util.create_table(
            config.target_database,
            config.target_backend_logs_table_name,
            config.target_backend_logs_table_create_cols
        )
    else:
        # 存在则跳过建表语句
        logger.debug(f"目的地数据库{config.target_database}中已存在表{config.target_barcode_table_name},跳过建表语句。")


def write_data_to_target(target_util, models_list):
    target_util.select_db(config.target_database)
    target_util.check_table_exists(config.target_backend_logs_table_name)
    for i, model in enumerate(target_util):
        line_sql = model.generate_insert_sql(config.target_backend_logs_table_name)
        target_util.execute_without_commit(line_sql)
        if (i + 1) % 1000 == 0:
            target_util.conn.commit()
            logger.info(f"已向目的地数据库：{config.target_database}中提交了{i + 1}条数据......")
    # 提交零头
    target_util.conn.commit()
    logger.info(f"已向目的地数据库：{config.target_database}中提交{len(models_list)}条数据......")


def write_data_to_metadata(metadata_util, process_dict):
    for file_name, num in process_dict.items():
        sql = f"INSERT INTO {config.metadata_backend_logs_table_name}" \
              f"(file_name, process_lines) VALUES (" \
              f"'{file_name}'," \
              f"{num}" \
              f");"

        # 执行sql
        metadata_util.select_db(config.metadata_database)
        metadata_util.execute_with_commit(sql)


def close_util(metadata_util, target_util):
    metadata_util.close_conn()
    target_util.close_conn()


if __name__ == '__main__':
    # TODO：1、创建logger对象
    logger = init_logger()
    logger.info("后台日志采集开始......")

    # TODO：2、创建连接对象
    metadata_util, target_util = build_util()

    # TODO：3、获取待处理的文件，没有则直接退出程序
    need_to_process_file = get_need_to_process_file(metadata_util)

    # TODO：4、构建logs模型，记录已处理的每个文件名称及行数
    models_list, process_dict = build_logs_model(need_to_process_file)

    # TODO：5、写入到csv中
    write_data_to_csv(models_list)

    # TODO：6、写入到目的地数据库中（retail）
    write_data_to_target(target_util, models_list)

    # TODO：7、向元数据库中写入此次处理的文件名称及对应条数
    write_data_to_metadata(metadata_util, process_dict)

    # TODO：8、关闭数据库连接对象util
    close_util(metadata_util, target_util)

    logger.info("采集后台日志数据，写入目标MySQL和CSV程序执行完成......")
