# coding:utf-8
"""
File: model_push_util.py
Author: ling.fang (ling.fang@cootek.cn）
Date: 2019/8/15 5:53:17
"""
import multiprocessing as mp
import os
import sys
import time
from datetime import datetime, timedelta

import meepo
import redis


class push_single_meepo:
  def __init__(self, addr, vaddr):
    self.log_prefix = 'push_single_meepo'
    self.addr = addr
    self.vaddr = vaddr
    self.meepo_path = "ad"
    self.new_version = ModelPushUtilParallel.new_version
    self.meepo_writer = None
    self.init()

  def init(self):
    self.meepo_writer = self.get_writer()

  def get_writer(self):
    addrs = []
    addrs.extend(self.addr)
    vaddr = self.vaddr
    writer = meepo.Writer(addrs, vaddr)
    writer.set_send_buffer(1024 * 1024)
    writer.set_send_timeout(30000)
    if writer.initialize():
      return writer
    print ('get meepo writer fail!', self.log_prefix, 'error')
    return None

  def get_task(self, data_type):
    meepo_name = "%s" % (data_type)
    print meepo_name, len(meepo_name)
    meepo_task = meepo.Task(meepo.MEEPO_ENTRY_TYPE_DOUBLE,
                            self.meepo_path, meepo_name)
    meepo_task.version(self.new_version)
    meepo_task.description("naga ctr model")
    meepo_task.slots(500000)
    meepo_task.buf_size(1024 * 1024)
    return meepo_task

  def upload(self, task):
    try:
      print("start uploading %s:%s%s" % (
        task.name, str(self.new_version), self.addr))

      self.meepo_writer.begin(task)
      while not task.if_can_commit():
        try:
          self.meepo_writer.write(task)
          print("%s:%s:%s, commit percent:%f" % (
            task.name, self.new_version, self.addr, task.precent()))
        except Exception as e:
          time.sleep(1)
          print(
              "%s:%s:%s, %s" % (task.name, self.new_version, self.addr, str(e)))
      self.meepo_writer.commit(task)
      self.meepo_writer.set_current(task.group(), task.name(), task.version())
    except Exception as e:
      print("update meepo occurred exception: %s:%s:%s, %s" % (
        task.name, self.new_version, self.addr, str(e)))
      exit(1)

    print("done uploading %s:%s%s" % (
      task.name, str(self.new_version), self.addr))

  def push_model(self):
    print("start pushing model %s:%s%s" %
          (ModelPushUtilParallel.model_meepo_dictname, str(self.new_version),
           self.addr))
    meepo_task = self.get_task(ModelPushUtilParallel.model_meepo_dictname)
    for key, value in ModelPushUtilParallel.model_value:
      meepo_task.write_number(key, float(value))
    self.upload(meepo_task)
    self.delete_old_version(ModelPushUtilParallel.model_meepo_dictname)
    print("done pushing model %s:%s%s" %
          (ModelPushUtilParallel.model_meepo_dictname, str(self.new_version),
           self.addr))

  def push_fea_list(self):
    print("start pushing fea_list %s:%s%s" %
          (ModelPushUtilParallel.model_meepo_dictname, str(self.new_version),
           self.addr))
    meepo_task = self.get_task(ModelPushUtilParallel.fealist_meepo_dictname)
    for key, value in ModelPushUtilParallel.fea_list_value:
      meepo_task.write_string(str(key), value)
    self.upload(meepo_task)
    print("done pushing fea_list %s:%s%s" %
          (ModelPushUtilParallel.model_meepo_dictname, str(self.new_version),
           self.addr))

  def delete_old_version(self, data_type):
    try:
      meepo_name = "%s" % data_type
      sanpshot_version_path = "./model_version/%s.ver" % (meepo_name)
      lines = []
      if os.path.exists(sanpshot_version_path):
        stable_dt = (datetime.now() + timedelta(hours=-2)).strftime(
          "%Y%m%d%H%S")
        print stable_dt
        with open(sanpshot_version_path, 'r') as f_r:
          lines = f_r.readlines()
      cur_ver = lines[-1]
      with open(sanpshot_version_path, 'w') as f_w:
        for line in lines:
          ver = line.strip('\n')
          dt_str = int(ver)
          if dt_str < int(
            stable_dt + '00') and dt_str != self.new_version and dt_str != int(
            cur_ver):
            self.meepo_writer.remove_snapshot("ad", meepo_name, int(ver))
            self.meepo_writer.remove_snapshot("ad", 'ctr_fea_list', int(ver))
            continue
          f_w.write("%s\n" % (ver))
        f_w.write("%s\n" % (self.new_version))
    except Exception as e:
      print("delete meepo[%s] occured exception: %s" %
            (str(self.new_version), str(e)))
      return

  def run(self):
    if self.meepo_writer is None:
      print("None writer for %s%s" % (str(self.new_version), self.addr))
      return self.addr, False

    print("start task %s%s" % (str(self.new_version), self.addr))
    self.push_model()
    self.push_fea_list()
    print("done task %s%s" % (str(self.new_version), self.addr))
    return self.addr, True


class ModelPushUtilParallel:
  model_file = None
  fea_list = None
  model_meepo_dictname = None
  fealist_meepo_dictname = None
  model_value = None
  fea_list_value = None
  new_version = None

  def __init__(self, new_version, model_push_config):
    self.config_file = model_push_config
    ModelPushUtilParallel.new_version = int(new_version)
    self.r_pipeline = None
    self.key_prefix = None
    self.redis_host = None
    self.redis_key = None
    self.local_addr = None
    self.remote_addr = None

    self.init()

  def parse_config(self):
    with open(self.config_file) as f_in:
      for raw_line in f_in:
        line = raw_line.strip("\n\r")
        if line == "" or line[0] == '#':
          continue

        key, value = line.split("=")
        key_strip = key.strip("\t ")
        value_strip = value.strip("\t ")

        if key_strip == "model_file":
          ModelPushUtilParallel.model_file = value_strip
        elif key_strip == "fea_list":
          ModelPushUtilParallel.fea_list = value_strip
        elif key_strip == "redis_host":
          self.redis_host = value_strip
        elif key_strip == "redis_port":
          self.redis_port = value_strip
        elif key_strip == "redis_key":
          self.redis_key = value_strip
        elif key_strip == "local_addr":
          self.local_addr = value_strip
        elif key_strip == "remote_addr":
          self.remote_addr = value_strip
        elif key_strip == "model_meepo_dictname":
          ModelPushUtilParallel.model_meepo_dictname = value_strip
          print self.model_meepo_dictname
        elif key_strip == "fealist_meepo_dictname":
          ModelPushUtilParallel.fealist_meepo_dictname = value_strip
        else:
          continue

    if self.local_addr is not None:
      str_sp = self.local_addr.split(":")
      if len(str_sp) == 2:
        local_host = str_sp[0]
        local_port = str_sp[1]
        self.local_addr = {"host": local_host,
                           "port": int(local_port)}
      else:
        print("local address format error!")
        exit(1)
    else:
      print("must set the local address!!")
      exit(1)

    remote_hosts = []
    if self.remote_addr is not None:
      machine_list = self.remote_addr.strip("\n\r ").split(",")
      for ml in machine_list:
        ml_sp = ml.strip().split(":")
        if len(ml_sp) == 2:
          remote_hosts.append({"host": ml_sp[0], "port": int(ml_sp[1])})
        else:
          print("remote address format error!")
          exit(1)
    else:
      print("must set the remote address!!")
      exit(1)
    self.remote_addr = remote_hosts

  def init(self):
    self.parse_config()
    self.init_redis_client()
    ModelPushUtilParallel.model_value = ModelPushUtilParallel.parse_model_file()
    ModelPushUtilParallel.fea_list_value = ModelPushUtilParallel.parse_fea_list()

  def init_redis_client(self):
    pool = redis.ConnectionPool(host=self.redis_host,
                                port=self.redis_port, db=0)
    r = redis.StrictRedis(connection_pool=pool)
    self.r_pipeline = r.pipeline(transaction=False)

  @staticmethod
  def parse_model_file():
    ret = {}
    for raw_line in open(ModelPushUtilParallel.model_file):
      flds = raw_line.strip("\n\r ").split(" ")
      if len(flds) > 1:
        ret[flds[0]] = flds[1]
      else:
        continue

    return ret

  @staticmethod
  def parse_fea_list():
    ret = {}
    for raw_line in open(ModelPushUtilParallel.fea_list):
      line_sp = raw_line.strip("\n\r ").split("\t")
      fea_name = line_sp[0]
      slot = line_sp[1]
      ret[fea_name] = slot

    return ret

  def update_redis(self):
    try:
      print('set redis_key[%s] redis_value[%s]' %
            (self.redis_key, self.new_version))
      self.r_pipeline.set(self.redis_key, self.new_version)
      self.r_pipeline.execute()
    except Exception as e:
      print(e)
      exit(1)

  def run(self):
    print("start push to %s:%s" % (self.remote_addr, self.new_version))
    push_task_list = []
    for addr in self.remote_addr:
      push_task_list.append((addr, self.local_addr))
    pool = mp.Pool(len(self.remote_addr))
    result = pool.map(ModelPushUtilParallel.push_single_server, push_task_list).get()
    pool.close()

    self.update_redis()
    print("done push to %s:%s, %s" % (self.remote_addr, self.new_version, result))

  @staticmethod
  def push_single_server(push_task):
    addr = push_task[0]
    vaddr = push_task[1]
    single_push = push_single_meepo(addr=addr, vaddr=vaddr)
    return single_push.run()


if __name__ == '__main__':
  if len(sys.argv) < 3:
    print("Usage: python model_push_util.py ver conf")
    exit(1)

  model_push_util_parallel = ModelPushUtilParallel(sys.argv[1], sys.argv[2])
  model_push_util_parallel.run()
