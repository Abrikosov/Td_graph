REPLACE PROCEDURE SCHEMA4.P1 (IN param1 INT,
				IN param2 INT)
MAIN:
BEGIN

/*
	Какой нибудь комментарий
*/

-- Допустим трансформация данных будет происходить с участием волатильнх таблиц
CREATE MULTISET VOLATILE TABLE t_v_step1 
(
	A INTEGER
,	B INTEGER
,	C FLOAT
)
PRIMARY INDEX ( A,B ) ON COMMIT PRESERVE ROWS
;

INSERT INTO 
	t_v_step1
SELECT	* 
FROM	SCHEMA3.VIEW1 
;

 
CREATE MULTISET VOLATILE TABLE t_v_step2 
(
	A INTEGER
,	B INTEGER
,	C FLOAT
)
PRIMARY INDEX ( A,B ) ON COMMIT PRESERVE ROWS
;

INSERT INTO 
	t_v_step2
SELECT	* 
FROM	t_v_step1 
;

-- По ошибке удаляем данные из одной таблицы, а вставку осуществляем в другую
DELETE FROM SCHEMA2.TABLE3_OLD;

INSERT INTO 
	SCHEMA2.TABLE3
SELECT	* 
FROM	t_v_step2
;

CALL SCHEMA4.P2();

END
;

