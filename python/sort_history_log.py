# !/usr/bin/python
# -*- coding:utf-8 -*-
import os

def sort_log_by_time(path):
	"""
	:param path: 数据路径(数据格式 panel_id + time + url + referer + ip)
	:return: 按照时间先后得到的
	"""
	result_dict = {}
	with open(file_path, "r") as file:
		for line in file:
			fields = line.split("\t", 3)  # 表示只分割为三段
			key = fields[1] + "\t" + fields[2]  # time + url
			value = fields[0] + "\t" + fields[3]  # panel_id + [referer] + ip
			print("[{0}] + [{1}]".format(key, value.strip()))
			result_dict[key] = value
	
	keys_list = result_dict.keys()
	sorted_keys = sorted(keys_list)
	
	result = []
	separator = "\t"
	for key in sorted_keys:
		rebuild_string = result_dict[key].split("\t", 1)[0] + separator + key + separator +result_dict[key].split("\t", 1)[1].strip()
		result.append(rebuild_string)
		
	return result

def save_history_information(data, path):
	with open(path, "w") as file:
		for line in data:
			file.write(line + "\n")

if __name__ == '__main__':
	file_path = "/Users/kaiwenyu/Downloads/panel_log_by_id_during_time.log"
	out_path = "/Users/kaiwenyu/Downloads/sorted_log.log"

	result_log = sort_log_by_time(file_path)
	save_history_information(result_log, out_path)