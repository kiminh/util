naga DSP CTR Model Offline Train
================================

## Overview

```
│    README.md
└─── daily_upadte: ftrl模型日更新
|
└─── ed_join_click: 日更新模型所需的ed和click日志拼接，通过mr实现
|
└─── ed_join_click_hour: 小时级别的ed和click日志拼接，通过mr实现
|
└─── hourly_update: ftrl模型小时更新
│   |
│   │ log: 保存模型所需要的日志日录，目前保存三种数据
│   │   │ ed: 从kafka读取展现的数据持久化到磁盘
│   │   │ click: 从kafka读取的点击日志持久化到磁盘
│   │   │ shitu: 上上个小时展现和点击拼接后的视图日志（由于点击延迟，相对当前时间上上个小时的视图日志拼接的点击丢失较小）
│   │   │ shitu_tmp: 上一个小时的展现拼接的视图日志
│   │ model_train: ftrl小时模型训练
│   │   │ conf: 模型推送meepo相关配置文件
│   │   │ done_path: 模型训练时间控制
│   │   │ ftrl: ftrl算法实现
│   │   │ model: 保存的模型文件
│   │   │ model_bk: 历史模型备份
│   │   │ model_version：历史模型版本保存，以便定期清理meepo过期的模型版本
│   │   │ script: 相关脚本
│   │   │ utils: 相关监控的脚本
│   │   │ model_train.sh: 模型训练的脚本（由于hdfs数据落盘延迟，所以还是使用从kafka拉接的日志做模型训练）
│   │   │ model_train_hdfs.sh: 从hdfs上拉取小时拼接的shitu做模型训练（暂未使用）
│   │ offline_stat: 离线偏差的统计
│   │ shitu_generate: kafka流的ed和click日志小时粒度的拼接
│
└───stat_ctr: 统计ctr模型
```
## 小时更新日志拼接逻辑
![image](https://gitlab.corp.cootek.com/davinci/personal/blob/ling.fang/ling.fang/ctr_space/image/%E5%9B%BE%E7%89%87_1.jpg)

[hourly_update/shitu/](https://gitlab.corp.cootek.com/davinci/personal/tree/ling.fang/ling.fang/ctr_space/hourly_update/log/shitu)保存上上个小时的shitu日志，使用上上个小时的ed、上上个小时的click、上个小时的click拼接而成，实际上通过[join_click.py](https://gitlab.corp.cootek.com/davinci/personal/blob/ling.fang/ling.fang/ctr_space/hourly_update/shitu_generate/script/join_click.py)和[join_click_tmp.py](https://gitlab.corp.cootek.com/davinci/personal/blob/ling.fang/ling.fang/ctr_space/hourly_update/shitu_generate/script/join_click_tmp.py)先后进行拼接。

[hourly_update/shitu_tmp/](https://gitlab.corp.cootek.com/davinci/personal/tree/ling.fang/ling.fang/ctr_space/hourly_update/log/shitu_tmp)保存了上个小时的shitu日志，如果click延迟一小时，那本目录就是有一些click可能会延后到下个小时才能到达，因此该目录只是临时保存，后面会使用[join_click_tmp.py](https://gitlab.corp.cootek.com/davinci/personal/blob/ling.fang/ling.fang/ctr_space/hourly_update/shitu_generate/script/join_click_tmp.py)进行回补。


