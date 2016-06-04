import re
import teradata

# Sorry, but comments only in Russian.

# Цель создания приложения: получение графа зависимостей между объектами БД,
# т.е. представление А зависит от таблиц Б и В, а процедура П зависит от таблицы Т и представления А (интересуют в первую очередб процедуры).
# Анализ ведется на основе DDL кода (в ряде случаев дает более полную информацию, напр. зависимости от удаленных объектов). 
# На данный момент, приложение ориентированно только на диалект teradata.

# TODO: 1. Исключить конструкции вида extract (month from DAY_ID)
# TODO: 2. Исключить td_unpivot
# TODO: 3. Добавить обработку MERGE
# TODO: 4. Добавить обработку функций
# TODO: 5. Исключить UPDATE FOR SESSION
# TODO: 5. Обработка конструкций вида ... FROM table1 a, table2 b ...

OBJECT_TYPE_ID_DICT = {"T": 1, "V": 2, "P": 3, "NA": 4}
SQL_OBJECTS_LIST = r"SELECT tablename FROM dbc.tablesv WHERE databasename = '{0}' and TableKind in ({1}) order by tablename"

def graph_object_name(server_name, schema_name, object_name):

    return "{0}.{1}.{2}".format(server_name, schema_name, object_name.strip())


class Td_Graph(object):

    def __init__(self, UdaExec_file_name):

        # Скомпилируем необходимы для работы регулярные выражения.
        # Очистка кода от ненужных элементов.
        self.re_sql_clean_compile = re.compile(r"""
                                                    \/\* [\s\S]*? \*\/  # Многострочный комментарий
                                                    | ' [\s\S]*? '      # Строковые значения 
                                                    | --[ \t\S]*        # Комментарий в одну строку
                                                """, re.VERBOSE)

        # Выделение объекта-источника (ключевое слово from) 
        self.re_sql_source_compile = re.compile(r"\b(?:FROM|JOIN)\s+((?:[a-zA-Z]\w+\.\s*)?(?:[a-zA-Z#_]\w+))\b", re.IGNORECASE)

        # Исключения для вышестоящего правлиа
        self.re_sql_from_ignore_compile = re.compile(r"\b(?:DELETE\s+FROM)\s+((?:[a-zA-Z]\w+\.\s*)?(?:[a-zA-Z#_]\w+))\b", re.IGNORECASE)

        # Временные таблицы создаваемые в процессе работы процедуры
        self.re_sql_volatile_compile = re.compile(r"\b(?:VOLAT\w*\s+TABLE)\s+((?:[a-zA-Z]\w+\.\s*)?(?:[a-zA-Z#_]\w+))\b", re.IGNORECASE)

        # Выделение изменяемого объекта
        self.re_sql_target_compile = re.compile(r"\b(?:INSERT\s+INTO|DELETE\s+FROM|UPDATE|DROP\s+TABLE|CALL)\s+((?:[a-zA-Z]\w+\.\s*)?(?:[a-zA-Z#_]\w+))\b", re.IGNORECASE)

        # Для экспорта в БД необходимо выделять имя сервера, имя схемы и имя объекта из его названия
        self.re_obj_name_compile = re.compile(r"""
                                        (?P<server_name>[\w]+)\.(?P<schema_name>[\w]+)\.(?P<object_name>[#\w]+)
                                            """, re.VERBOSE)

        # Граф объектов БД будем представлять в виде двух списков: списка вершин и списка связей.
        # nodes = {"table1": {"id":1, "type": "T"}, "view1": {"id":2, "type": "V"}, "table2": {"id":3, "type": "T"}}
        # edges = [("table1", "view1"), ("table2", "view1"), ]
        # num_nodes = 3
        # В нашем случае граф - ориентированный

        # Создаем необходимые объекты
        self.nodes = {}
        self.edges = []
        self.num_nodes = 0

        # Иницилизируем фреймворк UdaExec
        self.udaExec = teradata.UdaExec (appConfigFile=UdaExec_file_name)        

    def __get_node_id(self):

        self.num_nodes += 1
        return self.num_nodes-1

    def __add_nodes(self, object_name, obj_type="NA"):

        if object_name not in self.nodes:
            self.nodes[object_name] = {"id": self.__get_node_id(), "type": obj_type, "dependency": []}
        else:
            self.nodes[object_name]["type"]=obj_type

    def __add_edge(self, source_object, target_object):

        if source_object not in self.nodes:
            self.__add_nodes(source_object)

        if target_object not in self.nodes:
            self.__add_nodes(target_object)

        self.edges.append((source_object, target_object))


    def get_objects(self, server_name, schema_name_list):

        # Создаем коннект к server_name (cursor - получение списка объектов для каждой схемы, session - получение DDL кода)
        with self.udaExec.connect(server_name) as session:
            with session.cursor() as cursor:
                # Проходим по заданному спику схем 
                for schema_name in schema_name_list:

                    # ---------- Таблицы -----------
                    # Получаем список таблиц
                    for object_name in cursor.execute(SQL_OBJECTS_LIST.format(schema_name, "'O','T'")):
                        # Т.к. таблицы не зависят от других объектов, то просто заносим их названия в словарь nodes
                        target_object = graph_object_name(server_name.lower(), schema_name.lower(), object_name[0].lower())
                        self.__add_nodes(target_object, obj_type="T")

                    # ---------- Представления -----------
                    # Представления могут зависеть от таблиц и других представлений, соотвественно необходимо найти эти зависимости.
                    # Будем искать объекты следующие за ключевым словом "FROM"

                    # Получаем список представлений
                    for object_name in cursor.execute(SQL_OBJECTS_LIST.format(schema_name, "'V'")):
                        # Добавляем новый узел
                        target_object = graph_object_name(server_name.lower(), schema_name.lower(), object_name[0].lower())
                        self.__add_nodes(target_object, obj_type="V")

                        # Получаем DDL код
                        SQL_show = "SHOW VIEW {0}.{1}".format(schema_name, object_name[0])
                        ddl_raw_code = ""
                        for result_row in session.execute(SQL_show, ignoreErrors=[3624, 5535]):
                            ddl_raw_code += result_row[0]

                        ddl_code = self.re_sql_clean_compile.sub(" ", ddl_raw_code.lower())
                        # Сохраняем информацию о найденных зависимостях
                        for source_object in [graph_object_name(server_name.lower(), "default", s)
                                              if len(s.split(".")) < 2 else graph_object_name(server_name.lower(), *s.split("."))
                                              for s in set(self.re_sql_source_compile.findall(ddl_code))]:
                            self.__add_edge(source_object, target_object)

                    # ---------- Процедуры -----------
                    # Добавляем информацию о процедурах. Они могут иметь как входящие так и исходящие связи.
                    # Будем искать объекты следующие за ключевыми фразами FROM и INSERT INTO, UPDATE, DELETE, CALL. Причем необходимо исключить из рассмотрения
                    # временные таблицы (для teradata это volatile table).
                    # Предполагается, что процедуры ссылаются на объекты находящиеся на том же сервере.
                    for object_name in cursor.execute(SQL_OBJECTS_LIST.format(schema_name, "'P'")):
                        # Добавляем новый узел
                        procedure_name = graph_object_name(server_name.lower(), schema_name.lower(), object_name[0].lower())
                        self.__add_nodes(procedure_name, obj_type="P")

                        # Получаем DDL код
                        SQL_show = "SHOW PROCEDURE {0}.{1}".format(schema_name, object_name[0])
                        ddl_raw_code = ""
                        for result_row in session.execute(SQL_show, ignoreErrors=[3624, 5535]):
                            ddl_raw_code += result_row[0]

                        ddl_code = self.re_sql_clean_compile.sub(" ", ddl_raw_code.lower())
                        # Предварительно получаем список временных таблиц созданных во время работы процедуры
                        volatile_objects = set(self.re_sql_volatile_compile.findall(ddl_code))
                        # Список объектов следующих за ключевым словом FROM, подлежщие удалению из списка источников
                        from_ignore = set(self.re_sql_from_ignore_compile.findall(ddl_code))
                        # Получаем список объектов следующих после ключевых слов from, delete, insert, call за исключением временных объектов:
                        source_objects = set(self.re_sql_source_compile.findall(ddl_code)) - volatile_objects - from_ignore
                        target_objects = set(self.re_sql_target_compile.findall(ddl_code)) - volatile_objects

                        # Сохраняем информацию о найденных зависимостях (ключевое слово from)
                        if source_objects:
                            for source_object in [graph_object_name(server_name.lower(), "default", s)
                                                  if len(s.split(".")) < 2 else graph_object_name(server_name.lower(), *s.split("."))
                                                  for s in source_objects]:
                                self.__add_edge(source_object, procedure_name)
                        # Сохраняем информацию о найденных зависимостях (ключевое слово insert, call, delete, update)
                        if target_objects:
                            for target_object in [graph_object_name(server_name.lower(), "default", s)
                                                  if len(s.split(".")) < 2 else graph_object_name(server_name.lower(), *s.split("."))
                                                  for s in target_objects]:
                                self.__add_edge(procedure_name, target_object)

        # Находим для каждого объекта список зависимых от него объектов (пока без ограничения глубины)
        for source_object in self.nodes:
            s = [source_object, ]
            discovered = []
            while s:
                v = s.pop()
                if v not in discovered:
                    discovered.append(v)
                    for target_object in [target for (source, target) in self.edges if source == v]:
                        s.append(target_object)
                        self.nodes[source_object]["dependency"].append(target_object)

        # Находим для каждого объекта список объектов от которых он зависит (пока без ограничения глубины)
        for target_object in self.nodes:
            s = [target_object, ]
            discovered = []
            while s:
                v = s.pop()
                if v not in discovered:
                    discovered.append(v)
                    for source_object in [source for (source, target) in self.edges if target == v]:
                        s.append(source_object)
                        self.nodes[target_object]["dependency"].append(source_object)


    def export_to_gml(self, file_name):
        # Экспорт данных в формате Graph Modeling Language (GML)
        with open(file_name, "w") as gml_file:
            gml_file.write("graph\n[\n\t")
            gml_file.write("\n\t".join(["""node\n\t[\n\t\tid {0}\n\t\tlabel "{1}"\n\t]""".
                                         format(value["id"], key) for (key, value) in self.nodes.items()]))
            gml_file.write("\n\t")
            gml_file.write("\n\t".join(["""edge\n\t[\n\t\tsource {0}\n\t\ttarget {1}\n\t]""".
                                       format(self.nodes[source_object]["id"], self.nodes[target_object]["id"])
                                        for (source_object, target_object) in self.edges]))
            gml_file.write("\n]")

    def export_to_json(self, file_name):
        # Экспорт данных в формате JSON (D3.js)
        with open(file_name, "w") as json_file:
            json_file.write("""{\n"nodes":[\n\t""")
            json_file.write(",\n\t".join(["""{{"name": "{0}", "group": {1}, "id": {2}, "dependency": [{3}]}}""".
                                         format(key, OBJECT_TYPE_ID_DICT[value["type"]], value["id"], ",".join('"{0}"'.format(dependency) for dependency in value["dependency"]))
                                          for (key, value) in sorted(self.nodes.items(), key=lambda x: x[1]['id'])]))

            json_file.write("""\n\t],\n"links":[\n\t""")
            json_file.write(",\n\t".join(["""{{"source":{0},"target":{1},"value":1}}""".
                                         format(self.nodes[source_object]["id"], self.nodes[target_object]["id"])
                                          for (source_object, target_object) in self.edges]))
            json_file.write("\n\t]\n}")

    def export_to_db(self, server_name="magnit2", schema_name="magic", table_name="D_DFMR_OBJ_DEPENDENCY"):
        # Экспорт списка зависимостей между объектами в БД server_name
        # Предполагается, что таблица имеет следующий вид:
##        CREATE MULTISET TABLE 
##        (
##              SOURCE_SERVER	    VARCHAR(128)
##        ,	SOURCE_DB	    VARCHAR(128)
##        ,	SOURCE_OBJ	    VARCHAR(128)
##        ,	SOURCE_OBJ_KIND	    CHAR(2)
##        ,	TARGET_SERVER	    VARCHAR(128)
##        ,	TARGET_DB	    VARCHAR(128)
##        ,	TARGET_OBJ	    VARCHAR(128)
##        ,	TARGET_OBJ_KIND	    CHAR(2)
##        )
##        PRIMARY INDEX (SOURCE_OBJ, TARGET_OBJ)
        # Формируем список зависимостей между объектами
        if self.edges:
            obj_dependency = [self.re_obj_name_compile.search(source_object).groups() +
                              (self.nodes[source_object]["type"],) +
                              self.re_obj_name_compile.search(target_object).groups() +
                              (self.nodes[target_object]["type"],)
                          for (source_object, target_object) in self.edges]
        else:
            obj_dependency = []

        with self.udaExec.connect(server_name) as session:
            # Delete all rows
            session.execute("DELETE FROM {0}.{1};".format(schema_name, table_name))
            # Insert rows
            if obj_dependency:
                session.executemany("""INSERT INTO {0}.{1}
                                    (
                                        SOURCE_SERVER
                                    ,   SOURCE_DB
                                    ,   SOURCE_OBJ
                                    ,   SOURCE_OBJ_KIND
                                    ,   TARGET_SERVER
                                    ,	TARGET_DB
                                    ,	TARGET_OBJ
                                    ,	TARGET_OBJ_KIND
                                    )
                                    VALUES (?,?,?,?,?,?,?,?)""".format(schema_name, table_name), obj_dependency, batch=True)

if __name__ == "__main__":

    td_graph = Td_Graph("td_graph_UdaExec.ini")
    td_graph.get_objects("db1", ["schema1",])
    td_graph.export_to_json("td_graph.json")
    td_graph.export_to_db("db1", "schema1", "D_OBJ_DEPENDENCY")
