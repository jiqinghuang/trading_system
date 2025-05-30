/*
-- 创建表并导入数据
CREATE TABLE IF NOT EXISTS member_name_387 (
    客户编码 TEXT PRIMARY KEY,
    席位编码 TEXT
);

CREATE TABLE IF NOT EXISTS member_name_185 (
    客户编码 TEXT PRIMARY KEY,
    席位编码 TEXT
);

CREATE TABLE IF NOT EXISTS monitor_data (
    席位号 TEXT,
    客户编码 TEXT,
    交易日 TEXT,
    是否程序化报备 TEXT
);

-- 导入数据(实际应用中需使用SQLite的导入命令或程序化导入)
-- .import 387家.xlsx member_name_387
-- .import 185家.xlsx member_name_185
-- .import 异常监控记录.xlsx monitor_data
*/

-- 检查客户编码是否为10位数字
SELECT * FROM member_387 
WHERE 客户编码 NOT GLOB '[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]';

SELECT * FROM member_185 
WHERE 客户编码 NOT GLOB '[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]';

SELECT * FROM monitor_data 
WHERE 客户编码 NOT GLOB '[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]';

-- 检查185家中不在387家的客户编码
SELECT m185.* FROM member_185 m185
LEFT JOIN member_387 m387 ON m185.客户编码 = m387.客户编码
WHERE m387.客户编码 IS NULL;

SELECT * FROM member_185
WHERE 客户编码 NOT IN (SELECT 客户编码 FROM member_387);

-- 添加是否程序化报备列
ALTER TABLE monitor_data ADD COLUMN 是否程序化报备 TEXT;

-- 更新是否程序化报备列
UPDATE monitor_data
SET 是否程序化报备 = '在185中'
WHERE 客户编码 IN (SELECT 客户编码 FROM member_185);

UPDATE monitor_data
SET 是否程序化报备 = '在387不在185中'
WHERE 客户编码 IN (SELECT 客户编码 FROM member_387)
AND 客户编码 NOT IN (SELECT 客户编码 FROM member_185);

UPDATE monitor_data
SET 是否程序化报备 = '非程序化标记客户'
WHERE 是否程序化报备 IS NULL;

-- 格式化交易日为YYYY-MM-DD
UPDATE monitor_data
SET 交易日 = substr(交易日, 1, 10);
