from collections import defaultdict, namedtuple
import re
import inspect

BOOLEAN_FIELDS = (
    "search.regex", "searchable", "orderable", "regex"
)

DataColumn = namedtuple("DataColumn", ("name", "model_name", "filter"))

class DataTablesError(ValueError):
    pass

class DataTable(object):
    def __init__(self, params, model, query, columns):
        self.params = params
        self.model = model
        self.query = query
        self.data = {}
        self.columns = []
        self.columns_dict = {}
        self.search_func = lambda qs, s: qs
        self.column_search_func = lambda mc, qs, s: qs

        for col in columns:
            if isinstance(col, DataColumn):
                self.columns.append(col)
            elif isinstance(col, tuple):
                if len(col) == 3:
                    name, model_name, filter_func = col
                elif len(col) == 2:
                    name, model_name = col if not callable(col[1]) else (col[0], col[0], col[1])
                else:
                    raise ValueError("Columns must be a tuple of 2 to 3 elements")
                d = DataColumn(name=name, model_name=model_name, filter=filter_func)
                self.columns.append(d)
            else:
                name, model_name = col, col
                d = DataColumn(name=name, model_name=model_name, filter=None)
                self.columns.append(d)

            self.columns_dict[d.name] = d

        self.join_related_models()

    def join_related_models(self):
        for column in (col for col in self.columns if "." in col.model_name):
            self.query = self.query.join(column.model_name.split(".")[0], aliased=True)

    def query_into_dict(self, key_start):
        returner = defaultdict(dict)
        pattern = re.compile(f"{key_start}(?:\[(\d+)\])?\[(\w+)\](?:\[(\w+)\])?")
        
        for param, value in self.params.items():
            match = pattern.match(param)
            if match:
                column_id, key, optional_subkey = match.groups()
                if column_id is None:
                    returner[key] = self.coerce_value(key, value)
                elif optional_subkey is None:
                    returner[int(column_id)][key] = self.coerce_value(key, value)
                else:
                    subdict = returner[int(column_id)].setdefault(key, {})
                    subdict[optional_subkey] = self.coerce_value(f"{key}.{optional_subkey}", value)
                    
        return dict(returner)

    @staticmethod
    def coerce_value(key, value):
        if key in BOOLEAN_FIELDS:
            return value == "true"
        try:
            return int(value)
        except ValueError:
            return value

    def get_integer_param(self, param_name):
        try:
            return int(self.params[param_name])
        except (KeyError, ValueError):
            raise DataTablesError(f"Parameter {param_name} is missing or invalid")

    def add_data(self, **kwargs):
        self.data.update(kwargs)

    def json(self):
        try:
            return self._json()
        except DataTablesError as e:
            return {"error": str(e)}

    def get_column(self, column):
        column_path = column.model_name.split(".")
        model_column = getattr(self.model, column_path[0])
        for path_part in column_path[1:]:
            model_column = getattr(model_column.property.mapper.entity, path_part)
        return model_column

    def searchable(self, func):
        self.search_func = func

    def searchable_column(self, func):
        self.column_search_func = func

    def _json(self):
        draw = self.get_integer_param("draw")
        start = self.get_integer_param("start")
        length = self.get_integer_param("length")

        columns = self.query_into_dict("columns")
        ordering = self.query_into_dict("order")
        search = self.query_into_dict("search")

        query = self.query
        total_records = query.count()

        if callable(self.search_func) and search.get("value"):
            query = self.search_func(query, search["value"])

        for column_data in columns.values():
            search_value = column_data["search"]["value"]
            if column_data.get("searchable") and search_value and callable(self.column_search_func):
                column = self.columns_dict[column_data["data"]]
                model_column = self.get_column(column)
                query = self.column_search_func(model_column, query, str(search_value))

        for order in ordering.values():
            column_data = columns.get(order["column"])
            if column_data and column_data.get("orderable"):
                column = self.columns_dict[column_data["data"]]
                model_column = self.get_column(column)
                query = query.order_by(model_column.desc() if order["dir"] == "desc" else model_column.asc())

        filtered_records = query.count()

        if length > 0:
            query = query.slice(start, start + length)

        return {
            "draw": draw,
            "recordsTotal": total_records,
            "recordsFiltered": filtered_records,
            "data": [self.output_instance(instance) for instance in query.all()]
        }

    def output_instance(self, instance):
        returner = {key.name: self.get_value(key, instance) for key in self.columns}
        if self.data:
            returner["DT_RowData"] = {k: v(instance) for k, v in self.data.items()}
        return returner

    def get_value(self, key, instance):
        attr = key.model_name
        for sub in attr.split(".")[:-1]:
            instance = getattr(instance, sub)
        value = key.filter(instance) if key.filter else getattr(instance, attr.split(".")[-1])
        return value() if inspect.isroutine(value) else value
