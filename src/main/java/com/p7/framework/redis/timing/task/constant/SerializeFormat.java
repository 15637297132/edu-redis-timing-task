package com.p7.framework.redis.timing.task.constant;

public enum SerializeFormat {
	//字符串序列化形式，基本类型（包装类型）、字符串和可JSON化的数据类型才能选用
	STRING,
	//二进制对象序列化形式，所有可序列化java对象类型
	BLOB,
	;
}
