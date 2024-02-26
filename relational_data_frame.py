import operator

from pandas import DataFrame, Series, NA, NaT

from query_parser import QueryParser


class RelationalDataFrame:
    # TODO:
    # Filtering:
    # - .dt accessor
    # - .str accessor
    # - .isna() method
    # - .notna() method
    # - .between() method
    # - .isin() method
    # - ~ operator
    #
    # Relations:
    # - relation type (one-to-one, one-to-many, many-to-one, many-to-many)
    # - relation direction (one-way, two-way)
    # - relation inheritance (e.g. if a new filtered dataframe is created, should the relation be inherited?)
    # - column name caching (e.g. if a column name is unique in all related dataframes, the name of the related dataframe can be omitted)
    # - duplications (forbid the same relation name and dataframes to be related multiple times)
    # - multilayer relations (e.g. if a dataframe is related to another dataframe, which is related to another dataframe)
    #   - how to handle circular relations?
    # - plot relations (e.g. to visualize the relations between the dataframes)

    def __init__(self, data=None, index=None, columns=None, dtype=None, copy=False):
        self.data = DataFrame(data, index, columns, dtype, copy)
        self._relations = {}

    def __getattr__(self, name):
        return getattr(self.data, name)

    @property
    def _constructor(self):
        return RelationalDataFrame

    def relate(self, name: str, dataframe: 'RelationalDataFrame', on_left: str, on_right: str):
        self._relations[name] = {
            "dataframe": dataframe,
            "own_column": on_left,
            "other_column": on_right
        }

    def rfilter(self, query: str) -> 'RelationalDataFrame':
        """
        The relational filter method allows you to filter a dataframe easy based on its own columns and
        the columns of related dataframes. The query string should be a valid pandas query string.

        A single query contains up to 3 parts:
        - The left part of the query is the column name of the current dataframe
        - The middle part of the query is the operator
        - The right part is the condition to be met
        e.g. "age > 30" filters the dataframe to only include rows where the age is greater than 30.

        Evenly acceptable is a query where the left part is starting with the name of the dataframe
        or the self keyword.
        e.g. "customers.age > 30" or "self.age > 30" filters the dataframe to only include rows where
        the age is greater than 30.

        The true power ot the relational filter method comes when you do not only filter based on the
        columns of the current dataframe but also based on the columns of related dataframes. To do
        so, you can use the name of the related dataframe as the left part of the query.
        e.g. "orders.amount > 1000" filters the dataframe to only include rows where the amount of
        the related orders dataframe is greater than 1000.

        In case the query contains a column name that is unique in all the related dataframes, the
        name of the related dataframe can be omitted.
        e.g. "amount > 1000" filters the dataframe to only include rows where the amount of the
        related orders dataframe is greater than 1000.

        In case a column name is not unique in all the related dataframes, the name of the related
        dataframe must be used.

        Of course, also a combination of multiple queries using the logical operators & and | is possible.
        e.g. "age > 30 & orders.amount > 1000" filters the dataframe to only include rows where the age
        is greater than 30 and the amount of the related orders dataframe is greater than 1000.

        Also the use of brackets is possible.
        e.g. "(age > 30 | age < 20) & orders.amount > 1000" filters the dataframe to only include rows
        where the age is greater than 30 or less than 20 and the amount of the related orders dataframe
        is greater than 1000.

        :param query: The query string to filter the dataframe
        :return: A new dataframe containing only the rows that meet the condition
        """
        # 1. Parse the query string
        single_queries = QueryParser.parse_expr(query)

        # 2. Solve each layer in turn
        solved_queries = self.solve_query_layer(single_queries)

        # 3. Combine the results by using the logical operators
        solved_queries = self.combine_conditions(solved_queries)

        # 4. Apply the boolean mask to the dataframe rows
        return self.data[solved_queries]

    def combine_conditions(self, condition_layer: list) -> Series:
        if any(isinstance(i, list) for i in condition_layer):
            # If there are any nested lists in this layer, recurse into them first
            for i, element in enumerate(condition_layer):
                if isinstance(element, list):  # If the element is a list
                    condition_layer[i] = self.combine_conditions(element)
        else:
            # If we're in a layer with no more nested lists, this is the most deep-nested list.
            # Perform the boolean operations here.
            result = condition_layer[0]  # Start with the first boolean series
            for i in range(1, len(condition_layer), 2):  # Iterate over the rest of the list with a step of 2
                if condition_layer[i] == '&':
                    result = result & condition_layer[i + 1]
                elif condition_layer[i] == '|':
                    result = result | condition_layer[i + 1]
                else:
                    raise ValueError(f"Unknown logical operator '{condition_layer[i]}'")
            return result

        # After we've handled any nested lists in this layer, we can perform the boolean operations
        result = condition_layer[0]  # Start with the first boolean series
        for i in range(1, len(condition_layer), 2):  # Iterate over the rest of the list with a step of 2
            if condition_layer[i] == '&':
                result = result & condition_layer[i + 1]
            elif condition_layer[i] == '|':
                result = result | condition_layer[i + 1]
            else:
                raise ValueError(f"Unknown logical operator '{condition_layer[i]}'")
        return result

    def solve_query_layer(self, layer: list):
        results = []
        if all(isinstance(i, str) for i in layer):
            # then it is a condition
            results.append(self.solve_condition(layer))
        else:
            # then it is a combination of conditions and logical operators
            for element in layer:
                if isinstance(element, list):
                    results.append(self.solve_query_layer(element))
                else:
                    # This is a logical operator
                    results.append(element)
        return results

    def solve_condition(self, condition: list[str, str, str]) -> Series:
        # 1. Get the right column
        # 2. Get the right value / dtype of the value
        # 3. Solve the condition by using the right operator

        # Define a dictionary mapping operator strings to functions
        ops = {
            '<': operator.lt,
            '<=': operator.le,
            '==': operator.eq,
            '!=': operator.ne,
            '>=': operator.ge,
            '>': operator.gt
        }
        series = self.get_the_data_column(condition[0].strip())
        current_operator = condition[1].strip()
        value = self.resolve_value_data_type(condition[2].strip())

        if isinstance(series, tuple):
            return self.solve_relation_condition(series[0], series[1], ops[current_operator], value)
        return ops[current_operator](series, value)

    def solve_relation_condition(self, relation: dict, column: str, ops, value: any) -> Series:
        other_df = relation["dataframe"].data
        other_rel_id = relation["other_column"]
        own_rel_id = relation["own_column"]
        relation_ids = other_df[ops(other_df[column], value)][other_rel_id].unique()
        return self.data[own_rel_id].isin(relation_ids)

    def get_the_data_column(self, column_name: str) -> Series | tuple[dict, str]:
        # Szenarios:
        #   - column
        #   - table.column
        #   - self.column
        if "." in column_name:
            table_name, column_name = column_name.split(".")

            if table_name == "self":
                return self.data[column_name]
            else:
                # currently this must be a relation column
                relation_entry = self._relations.get(table_name, None)
                if relation_entry is None:
                    raise ValueError(f"Could not find a relation with the name '{table_name}'")
                elif column_name not in relation_entry["dataframe"].data.columns:
                    raise ValueError(
                        f"Could not find a column with the name '{column_name}' in the relation '{table_name}'")
                else:
                    return relation_entry, column_name
        else:
            if column_name in self.data.columns:
                return self.data[column_name]
            else:
                # for relation in self._relations:
                #     if column_name in relation.data.columns:
                #         return relation.data[column_name]
                raise NotImplementedError("This is not implemented yet")

    @staticmethod
    def resolve_value_data_type(value: str) -> any:
        # Possible data types:
        #   - int
        #   - float
        #   - str
        #   - date / datetime
        #   - bool
        #   - None / NaN / NaT

        if (value[0] == "'" and value[-1] == "'") or (value[0] == '"' and value[-1] == '"'):
            return value[1:-1]
        elif value.lower() == "true":
            return True
        elif value.lower() == "false":
            return False
        elif value.lower() == "none":
            return None
        elif value.lower() == "nan":
            return NA
        elif value.lower() == "nat":
            return NaT
        elif value.isdigit():
            return int(value)
        elif value.replace(".", "", 1).isdigit():
            return float(value)
        else:
            raise ValueError(f"Could not resolve the data type of the value '{value}'")
