REPLACE VIEW SCHEMA3.VIEW1 as 

/*
	Какой нибудь комментарий
*/

SELECT	*
FROM	SCHEMA2.TABLE2 -- Какая-нибудь полезная информация

UNION ALL

-- Вполне возможно, что нужно одну таблицу использовали несколько раз
SELECT	*
FROM	SCHEMA2.TABLE2 -- Какая-нибудь полезная информация

UNION ALL

/*
	что-нибудь написано
*/

SELECT	*
FROM	SCHEMA10.TABLE10 -- Для данной таблицы нет DDL файла

