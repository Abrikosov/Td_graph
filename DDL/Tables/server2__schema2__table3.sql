/*
	Какой нибудь комментарий
*/

CREATE MULTISET TABLE SCHEMA2.TABLE3 , NO FALLBACK ,
	NO BEFORE JOURNAL,
	NO AFTER JOURNAL,
	CHECKSUM = DEFAULT,
	DEFAULT MERGEBLOCKEATIO
(
	COLUMN1 INTEGER	-- Зачем нужно это поле
,	COLUMN2 INTEGER	-- Зачем нужно это поле
,	COLUMN3 INTEGER	-- Зачем нужно это поле
) PRIMARY INDEX (COLUMN1, COLUMN2);
