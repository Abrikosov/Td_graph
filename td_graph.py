import os
import re

# Sorry, but comments only in Russian.

# Цель создания приложения: получение графа зависимостей между объектами БД,
# т.е. представление А зависит от таблиц Б и В, а процедура П зависит от таблицы Т и представления А.
# Анализ вед    ется на основе файлов с DDL кодом. Предполагается, что к моменту запуска данного приложения
# файлы уже собраны и располагаются в соотв. подкаталогах в папке "./DDL" (модуль по сбору DDL находится в разработке).
# Ожидается, что имена файлов имеют следующий вид: <server_name>__<schema_name>__<object_name>.sql.
# На данный момент, приложение ориентированно только на диалект teradata.

# TODO: 1. Добавить обработку функций
# TODO: 2. Добавить импорт списка связей в БД

DDL_TABLES_PATH = "DDL/Tables"
DDL_VIEWS_PATH = "DDL/Views"
DDL_PROCEDURES_PATH = "DDL/Procedures"

OBJECT_TYPE_ID_DICT = {"T": 1, "V": 2, "P": 3, "NA": 4}

class Td_Graph(object):

    def __init__(self):

        # Скомпилируем необходимы для работы регулярные выражения.
        # Нам будет необходимо удалять комментарии из кода.
        self.re_sql_comment_compile = re.compile(r"""
                                                \/\* [\s\S]*? \*\/  # Многострочный комментарий
                                            |   --.*                # Комментарий в одну строку
                                        """, re.VERBOSE)

        # Следующая конструкция понадобиться для выделения объекта следующего за FROM
        self.re_sql_from_compile = re.compile(r"(?<!\bDELETE)\s+FROM\s+(\S+)\b", re.IGNORECASE)

        # Следующая конструкция понадобиться для получения списка временных таблиц созданных в процессе работы процедуры
        self.re_sql_volatile_compile = re.compile(r"\bVOLAT\w*\s+TABLE\s+(\S+)\b", re.IGNORECASE)

        # Следующая конструкция понадобиться для получения списка таблиц в которые идет вставка данных
        self.re_sql_insert_compile = re.compile(r"\bINSERT\s+INTO\s+(\S+)\b", re.IGNORECASE)

        # Следующая конструкция понадобиться для выделения объекта следующего за DELETE
        self.re_sql_delete_compile = re.compile(r"\bDELETE\s+FROM\s+(\S+)\b", re.IGNORECASE)

        # Следующая конструкция понадобиться для выделения объекта следующего за UPDATE
        self.re_sql_update_compile = re.compile(r"\bUPDATE\s+(\S+)\b", re.IGNORECASE)

        # В ряде случаев необходимо определять имя сервера на котором располагается конкретный объект.
        # Это удобно делать анализируя имя файла.
        self.re_file_name_compile = re.compile(r"""
                                        (?P<server_name>[\S]+)__(?P<schema_name>[\S]+)__(?P<object_name>[\S]+).sql
                                            """, re.VERBOSE)

        # Следующая конструкция понадобиться для выделения объекта следующего за CALL
        self.re_sql_call_compile = re.compile(r"\bCALL\s+(\S+)\b", re.IGNORECASE)

        # Граф объектов БД будем представлять в виде двух списков: списка вершин и списка связей.
        # nodes = {"table1": {"id":1, "type": "T"}, "view1": {"id":2, "type": "V"}, "table2": {"id":3, "type": "T"}}
        # edges = [("table1", "view1"), ("table2", "view1"), ]
        # num_nodes = 3
        # В нашем случае граф - ориентированный

        # Создаем необходимые объекты
        self.nodes = {}
        self.edges = []
        self.num_nodes = 0

    def __get_node_id(self):

        self.num_nodes += 1
        return self.num_nodes-1

    def __add_nodes(self, object_name, obj_type="NA"):

        self.nodes[object_name] = {"id": self.__get_node_id(), "type": obj_type}

    def __add_edge(self, source_object, target_object):

        if source_object not in self.nodes:
            self.__add_nodes(source_object)

        if target_object not in self.nodes:
            self.__add_nodes(target_object)

        self.edges.append((source_object, target_object))


    def get_tables(self):

        # Т.к. таблицы не зависят от других объектов, то просто заносим их названия в словарь nodes
        # Получем список всех файлов находящихся в директории DDL_TABLES_PATH и имеющих расшиерение .sql
        for ddl_file_name in filter(lambda x: x.endswith('.sql'), os.listdir(path=DDL_TABLES_PATH)):
            server_name, schema_name, object_name = self.re_file_name_compile.search(ddl_file_name).groups()
            target_object = server_name + "__" + schema_name + "__" + object_name
            self.__add_nodes(target_object, obj_type="T")

    def get_views(self):

        # Добавляем информацию о представлениях. Они в свою очередь могут зависеть от таблиц и других представлений,
        # соотвественно необходимо найти эти зависимости. Будем искать объекты следующие за ключевым словом FROM.
        # Предполагается, что представления ссылаются на объекты находящиеся на том же сервере.
        # Получем список всех файлов находящихся в директории DDL_VIEWS_PATH и имеющих расшиерение .sql
        # Проходим по всем найденным файлам.
        for ddl_file_name in filter(lambda x: x.endswith('.sql'), os.listdir(path=DDL_VIEWS_PATH)):
            # Определяем наименование сервера, далее мы его будет прибавлять к названиям найденных смежных объектов.
            server_name, schema_name, object_name = self.re_file_name_compile.search(ddl_file_name).groups()
            target_object = server_name + "__" + schema_name + "__" + object_name
            self.__add_nodes(target_object, obj_type="V")

            with open(DDL_VIEWS_PATH + "/" + ddl_file_name, "r") as ddl_file:
                # Считываем код из файла и удаляем комментарии (заменяем на один пробел)
                ddl_code = self.re_sql_comment_compile.sub(" ", ddl_file.read())
                # Сохраняем информацию о найденных зависимостях
                for source_object in [server_name + "__" + s.replace(".", "__").lower() for s in set(self.re_sql_from_compile.findall(ddl_code))]:
                    self.__add_edge(source_object, target_object)

    def get_procedures(self):

        # Добавляем информацию о процедурах. Они могут иметь как входящие так и исходящие связи.
        # Будем искать объекты следующие за ключевыми фразами FROM и INSERT INTO. Причем необходимо исключить из рассмотрения
        # временные таблицы (для teradata это volatile table).
        # Предполагается, что процедуры ссылаются на объекты находящиеся на том же сервере.

        # Получем список всех файлов находящихся в директории DDL_PROCEDURES_PATH и имеющих расшиерение .sql
        # Проходим по всем найденным файлам.
        for ddl_file_name in filter(lambda x: x.endswith('.sql'), os.listdir(path=DDL_PROCEDURES_PATH)):
            # Определяем наименование сервера, далее мы его будет прибавлять к названиям найденных смежных объектов.
            server_name, schema_name, object_name = self.re_file_name_compile.search(ddl_file_name).groups()
            procedure_name = server_name + "__" + schema_name + "__" + object_name
            self.__add_nodes(procedure_name, obj_type="P")

            with open(DDL_PROCEDURES_PATH + "/" + ddl_file_name, "r") as ddl_file:
                # Считываем код из файла и удаляем комментарии (заменяем на один пробел)
                ddl_code = self.re_sql_comment_compile.sub(" ", ddl_file.read())
                # Предварительно получаем список временных таблиц созданных во время работы процедуры
                volatile_objects = set(self.re_sql_volatile_compile.findall(ddl_code))
                # Получаем список объектов следующих после ключевых слов from, delete, insert, call за исключением временных объектов:
                source_objects = set(self.re_sql_from_compile.findall(ddl_code)) - volatile_objects
                target_objects = (
                                    (
                                        set(self.re_sql_delete_compile.findall(ddl_code)) |
                                        set(self.re_sql_insert_compile.findall(ddl_code)) |
                                        set(self.re_sql_update_compile.findall(ddl_code))
                                    ) - volatile_objects
                                 ) | set(self.re_sql_call_compile.findall(ddl_code))

                # Сохраняем информацию о найденных зависимостях (ключевое слово from)
                for source_object in [server_name + "__" + s.replace(".", "__").lower() for s in source_objects]:
                    self.__add_edge(source_object, procedure_name)
                # Сохраняем информацию о найденных зависимостях (ключевое слово insert, call, delete, update)
                for target_object in [server_name + "__" + s.replace(".", "__").lower() for s in target_objects]:
                    self.__add_edge(procedure_name, target_object)

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
            json_file.write(",\n\t".join(["""{{"name": "{0}", "group": {1}}}""".
                                         format(key, OBJECT_TYPE_ID_DICT[value["type"]])
                                          for (key, value) in sorted(self.nodes.items(), key=lambda x: x[1]['id'])]))
            json_file.write("""\n\t],\n"links":[\n\t""")
            json_file.write(",\n\t".join(["""{{"source":{0},"target":{1},"value":1}}""".
                                         format(self.nodes[source_object]["id"], self.nodes[target_object]["id"])
                                          for (source_object, target_object) in self.edges]))
            json_file.write("\n\t]\n}")

if __name__ == "__main__":

    td_graph = Td_Graph()
    td_graph.get_tables()
    td_graph.get_views()
    td_graph.get_procedures()
    td_graph.export_to_json("td_graph.json")
