#### 需求1：统计页面的浏览量
sql select count(*) from table
统计数量  把每一行记录作为一个固定的key， value赋值为1.
事实上就是统计所有log的数量。
#### 需求2： 统计各个省份的浏览量
sql select count（*）from table group by province
利用ip信息得到省份。 ip===》城市信息
ip解析：收费

