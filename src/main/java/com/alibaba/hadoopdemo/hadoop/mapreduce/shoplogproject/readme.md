#### 需求1：统计页面的浏览量
sql select count(*) from table
统计数量  把每一行记录作为一个固定的key， value赋值为1.
事实上就是统计所有log的数量。
#### 需求2： 统计各个省份的浏览量
sql select count（*）from table group by province
利用ip信息得到省份。 ip===》城市信息
ip解析：收费
#### 需求3  统计页面的访问量
原始日志第二个字段是一个url， url日志里面带topicId的东西解析出来（pageID）
取出log之后利用正则表达式将对应的每一条日志里面访问url里面的topicid取出来，然后根据这个值来进行聚类
求和。group by