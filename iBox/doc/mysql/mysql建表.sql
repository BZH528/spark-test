use xiaojinhe;

create table `realtime_feature`(
  `menu` varchar(128) not null comment '菜单项',
  `feature_name` varchar(128) not null comment '指标名称',
  `feature_value` varchar(128) not null comment '指标值',
  `date` varchar(64) not null comment '统计日期' ,
  `time` varchar(64) not null comment '统计时间',
primary key (`menu`,`feature_name`,`date`,`time`)
)ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='小金盒实时指标表';