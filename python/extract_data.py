# !/usr/bin/python
# -*- coding: utf-8 -*-

"""
    本模块的目的是将"流量Panel"的采集数据从Gitlab上下载至本地机器完成有效信息提取。
    由于原始"流量Panel"数据中存在schema中'url'=='heartbeat'形式的记录信息，该信息对于流量而言目前没有分析价值，需要过滤掉。
    在本地提取基于"Panel_id"的信息提取，以每条浏览行为记录为单位，获得'Time'，'URL','Referer', 'Ip'
"""
import os
from datetime import datetime


def read_single_raw_log(input_path):
    """
    :param input_path: 原始日志
    :return: 目标格式日志
    从本地读取原始的panel.log日志，过滤掉"heartbeat"、无意义字段和空行等records
    在原始日志中，前四个字段写成一行，后四个字段写成一个字段，然后会空一行，表示一条record
    """
    read_result = []
    # count_heartbeat = 0
    # count_X_for = 0
    # count_space = 0
    with open(input_path, "r") as file:
        for line in file:
            line_split = line.split("\n")
            target_line = line_split[0]
            if target_line.find("heartbeat") == -1 and target_line.find("X-Forwarded-For") == -1 and target_line != "":
                read_result.append(line.strip())
            # if line_split[0].find("heartbeat") != -1:
            #     count_heartbeat += 1
            # if line_split[0].find("X-Forwarded-For") != -1:
            #     count_X_for += 1
            # if line_split[0] == "":
            #     count_space += 1

    # print("count_heartbeat: " + str(count_heartbeat))  # 含有"heartbeat"字段信息过滤掉
    # print("count_X_for: " + str(count_X_for))  # 含有"X-Forwarded-For"字段信息过滤掉
    # print("count_space: " + str(count_space))  # 没有任何信息的空行过滤掉
    print("read_result: " + str(len(read_result)))

    for i in range(1):
        print("eg. " + read_result[i])
    return read_result

def read_multi_raw_log(base_path):
    """
    :param base_path: 原始日志文件夹路径
    :return: 多日志文件原始数据
    """
    read_total_result = []
    paths_list = []
    files_name = os.listdir(base_path)
    # 得到完整文件路径
    for file in files_name:
        if file.find("panel.log") != -1 and file.find("zip") == -1:  # 只找与records写入有关文件
            paths_list.append(base_path + file)
    
    # 测试目标日志文件
    # for line in paths_list:
    #     print(line)
    
    # 读取全部原始日志数据
    for single_path in paths_list:
        with open(single_path, "r") as file:
            for line in file:
                line_split = line.split("\n")
                target_line = line_split[0]
                if target_line.find("heartbeat") == -1 and target_line.find("X-Forwarded-For") == -1 and \
                        target_line != "":
                    read_total_result.append(line.strip())
    return read_total_result

def extract_records(raw_data):
    """
    :param raw_data: 从原始panel_log中过滤不符合条件的日志结果
    :return: 取得特定信息的字段数据，按照"panel_id time url referer ip"以"\t"作为字符串分隔符。返回该字符串组成的列表
    以panel_id作为日志过滤条件得到该用户在2021/03/19这一天网页浏览行为日志（4字段信息)
    """
    extract_records_list = []
    
    for line in raw_data:
        # print("raw_data:" + line)
        raw_data_split = line.split("\t")
        data_fields_split = raw_data_split[2].replace("\"", "").strip("{").strip("}").split(",")
        # print("data_fields_split:", data_fields_split)

        # 使用字典存储data_split[2]中的Json对象，包括panel_id、referer等
        data_fields_dic = {}
        for raw_key_value_str in data_fields_split:
            # print(raw_key_value_str)
            raw_key_value_str_split = raw_key_value_str.split(": ")
            if len(raw_key_value_str_split) == 2:  # 只有key-value形式才能加入dic
                key = str(raw_key_value_str_split[0].replace(" ", ""))
                value = str(raw_key_value_str_split[1])
                # print("key-value: " + key + ">" + value)
                data_fields_dic[key] = value
        
        # noinspection PyBroadException
        panel_id_in_data = data_fields_dic.get("panel-id")  # panel_id
        referer_in_data = data_fields_dic.get("Referer")  # referer
        if referer_in_data is None:
            referer_in_data = ""
        
        # print("dic-search:" + panel_id_in_data + "|" + referer_in_data)
        ip_in_data = raw_data_split[0][47:]  # ip
        time_in_data = raw_data_split[0][14:33] # time
        url_in_data = raw_data_split[1].strip()  # url
        
        # print("time_stamp:" + raw_data_split[0][14:33])
        # print("url:" + raw_data_split[1].strip())
        # print("ip:" + raw_data_split[0][47:])
        # print("panel_id, time, url, referer, ip:{0}, {1}, {2}, {3}, {4}".format(
        #     panel_id_in_data, time_in_data, url_in_data, referer_in_data, ip_in_data))
        
        separator = "\t"
        extract_string = panel_id_in_data + separator + time_in_data + separator + url_in_data + separator + \
                         referer_in_data + separator + ip_in_data
        extract_records_list.append(extract_string)
        # print(str(index) + ": " + extract_string); index += 1
    print("  valid data count: " + str(len(extract_records_list)))
    
    save_history_information(extract_records_list, "./extract_records.log")
    print("  save extract_records.log")
    return extract_records_list

def get_data_by_id(extract_data, panel_id):
    """
    :param extract_data: 不区分panel_id的提取日志数据
    :param panel_id: 目标panel_id
    :return: 目标panel_id的全部有效日志记录
    提取日志记录'panel_id', 'time', 'url', 'referer','ip'构成的数据列表
    """
    filter_data_by_id_list = []
    for line in extract_data:
        panel_id_in_data = str(line.split("\t", -1)[0])
        # print("panel_id_in_data:" + panel_id_in_data + "|" + str(len(panel_id_in_data)))
        if panel_id_in_data == panel_id:
            filter_data_by_id_list.append(line)
            # print("filtered:" + line)
    return filter_data_by_id_list

def get_data_by_day(input_data, day_time):
    """
    :param input_data: 输入整理后的有效用户浏览行为日志数据(五字段数据)
    :param day_time: 需要过滤的时间(输入格式:"%Y-%m-%d %H:%M:%S")
    :return: 返回某天时间内该id的浏览行为日志数据
    从已经处理好的有效信息中，按照某个时间段只过滤实验当天收集数据的时段浏览行为数据
    """
    filter_list = []
    for records in input_data:
        time_stamp = records.split("\t")[1]
        record_time = datetime.strptime(time_stamp, "%Y-%m-%d %H:%M:%S")
        target_time = datetime.strptime(day_time, "%Y-%m-%d %H:%M:%S")
        record_day = record_time.day
        target_day = target_time.day
        
        if record_day == target_day:
            filter_list.append(records)
    return filter_list

def get_data_by_time(input_data, start_time, end_time):
    """
    :param input_data: 输入整理后的有效用户浏览行为日志数据(五字段数据)
    :param start_time: 需要过滤的开始时间(输入格式:"%Y-%m-%d %H:%M:%S")
    :param end_time: 需要过滤的结束时间(输入格式:"%Y-%m-%d %H:%M:%S")
    :return: 满足时间限制的目标日志数据
    """
    filter_list = []
    for records in input_data:
        time_stamp = records.split("\t")[1]
        format_str = "%Y-%m-%d %H:%M:%S"
        record_time = datetime.strptime(time_stamp, format_str)
        start_compare = datetime.strptime(start_time, format_str)
        end_compare = datetime.strptime(end_time, format_str)
        
        if start_compare <= record_time <= end_compare:
            filter_list.append(records)
        
    return filter_list

def save_history_information(filtered_data, path):
    # 将目标数据以文本形式保留在本地，后需上传至HDFS中进行下一步分析（聚类、特征向量化，层次聚类等）
    with open(path, "w") as file:
        for line in filtered_data:
            file.write(line + "\n")

def test():
    """
    功能测试函数
    """
    base_path = "/Users/kaiwenyu/Downloads/panel_log/"
    input_path = "/Users/kaiwenyu/Downloads/panel_log/panel.log"
    output_path = "/Users/kaiwenyu/Downloads/panel_log/target_data.log"
    target_id = "1612152628547-725E6EFB"
    
    # target_id = "1611912640642-0E874AE7"
    # data = read_single_raw_log(input_path)
    # extracted_data = extract_records(data)
    # filter_data_by_id = get_data_by_id(extracted_data, target_id)
    # for line in filter_data_by_id[100:110]:
    #     print("filter_data_by_id: " + line)
    
    # filter_data_by_day = get_data_by_day(filter_data_by_id, "2021-03-17")
    
    # file_name = os.listdir(base_path)
    # for path in file_name:
    #     print(base_path + path)
    
    data = read_multi_raw_log(base_path)
    extract_data = extract_records(data)
    for line in extract_data[10:30]:
        print("target_data_extract:" + line)
    filter_by_id = get_data_by_id(extract_data, target_id)
    for line in filter_by_id[10:30]:
        print("target_data_id:" + line)
    filter_by_time = get_data_by_time(filter_by_id, "2021-03-19 14:35:00", "2021-03-19 18:25:00")  # 2021-03-17 11:20:22
    for line in filter_by_time[10:30]:
        print("target_data:" + line)
    save_history_information(filter_by_time, output_path)

if __name__ == '__main__':
    # print("=" * 50)
    # base_path = "/data/logs/surge/"
    # panel_id = "1611912640642-0E874AE7"
    # output_path = "/data/logs/surge/panel_log_process/panel_log_by_id_during_time.log"
    #
    # separator = "*"
    # print(separator + " [1] 读取日志数据")
    # raw_data = read_multi_raw_log(base_path)
    #
    # print(separator + " [2] 提取日志信息")
    # extract_data = extract_records(raw_data)
    #
    # print(separator + " [3] 过滤ID日志数据")
    # filter_by_id = get_data_by_id(extract_data, panel_id)
    #
    # print(separator + " [4] 过滤该ID在对应时间内的日志数据")
    # filter_by_time = get_data_by_time(filter_by_id, "2021-03-19 15:00:00", "2021-03-19 18:40:00")
    #
    # print(separator + " [5] 写出目标日志数据")
    # save_history_information(filter_by_time, output_path)
    # print(separator * 50)
    
    dict = {"2021-03-19 17:35:00": "A", "2021-03-19 14:40:00": "B", "2021-03-19 13:35:00": "D", "2021-03-19 11:50:00": "R"}
    sorted_list = sorted(dict.keys())
    print(sorted_list)
    
    # test()
    # read_multi_raw_log(base_path)