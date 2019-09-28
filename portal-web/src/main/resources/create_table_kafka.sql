#创建topic表
CREATE TABLE IF NOT EXISTS `dataplatform_topic`(
`id` INT UNSIGNED AUTO_INCREMENT COMMENT 'INDEX ID',
`topic_name` VARCHAR(100) UNIQUE NOT NULL COMMENT 'topic名称',
`product` VARCHAR(100) NOT NULL COMMENT '产品线',
`peek_traffic` INT UNSIGNED COMMENT '峰值流量大小(byte/s)',
`peek_qps` INT UNSIGNED COMMENT '峰值条数大小(条/s)',
`datasize_number` INT UNSIGNED COMMENT 'topic数据量(条/day)',
`datasize_space` INT UNSIGNED COMMENT 'topic数据量(byte/day)',
`logsize` INT UNSIGNED COMMENT '单条日志大小(byte)',
`contact_person` VARCHAR(100) NOT NULL COMMENT '联系人(邮箱前缀逗号隔开)',
`create_time` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建topic时间',
`update_time` TIMESTAMP NOT NULL ON UPDATE CURRENT_TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '最近一次更新topic时间',
`comment` TEXT COMMENT '备注',
PRIMARY KEY ( `id` )
)ENGINE=InnoDB DEFAULT CHARSET=utf8;

#创建consumer表
CREATE TABLE IF NOT EXISTS `dataplatform_consumer`(
`id` INT UNSIGNED AUTO_INCREMENT COMMENT 'INDEX ID',
`topic_name` VARCHAR(100) NOT NULL COMMENT 'topic名称',
`consumer_product` VARCHAR(100) NOT NULL COMMENT '产品线',
`consumer_group` VARCHAR(100) NOT NULL COMMENT '消费者组',
`contact_person` VARCHAR(200) NOT NULL DEFAULT 'yurun,jianhong1' COMMENT '联系人(邮箱前缀逗号隔开，报警接收人)',
`create_time` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建topic时间',
`update_time` TIMESTAMP NOT NULL ON UPDATE CURRENT_TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '最近一次更新topic时间',
`comment` TEXT COMMENT '备注',
PRIMARY KEY ( `id` ),
UNIQUE(`topic_name`,`consumer_group`)
)ENGINE=InnoDB DEFAULT CHARSET=utf8;


# 创建产品线表
CREATE TABLE IF NOT EXISTS `dataplatform_product`(
`id` INT UNSIGNED AUTO_INCREMENT COMMENT 'INDEX ID',
`product_name` VARCHAR(100) NOT NULL COMMENT '产品线名称',
PRIMARY KEY ( `id` )
)ENGINE=InnoDB DEFAULT CHARSET=utf8;