/**
  建表语句
 */
CREATE TABLE fruit IF NOT EXISTS
(
    id    int    NOT NULL ENCODING AUTO_ENCODING COMPRESSION NO_COMPRESSION,
    name  STRING NOT NULL ENCODING AUTO_ENCODING COMPRESSION NO_COMPRESSION,
    type  STRING NOT NULL ENCODING AUTO_ENCODING COMPRESSION NO_COMPRESSION,
    price float  NOT NULL ENCODING AUTO_ENCODING COMPRESSION NO_COMPRESSION,
    PRIMARY KEY (id)
) PARTITION BY HASH (id) PARTITIONS 8
 STORED AS KUDU
 TBLPROPERTIES ('kudu.master_addresses'='cdh-test-server-01:7051,cdh-test-server-02:7051,cdh-test-server-03:7051', 'kudu.num_tablet_replicas'='3')

/**
  初始化语句
 */

INSERT INTO fruit values (1,'苹果','仁果类',9.9 );
INSERT INTO fruit values (2,'西瓜','瓜果',8.9 );
INSERT INTO fruit values (3,'葡萄','浆果类',18.9 );