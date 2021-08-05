from typing import List, Tuple


def query(
    table_name: str,
    col_list: list = ['*'],
    distinct_col: list = None,
    count_col: list = None,
    where: List[Tuple[str, str, str or int, str]] or None = None,
    limit: int or None = None
) -> str:
    q = 'SELECT '

    for c in col_list:
        if ((distinct_col) and (c in distinct_col)) and ((count_col) and (c in count_col)):
            q += f'COUNT(DISTINCT({c})), '
        elif distinct_col and (c in distinct_col):
            q += f'DISTINCT({c}), '
        elif count_col and (c in count_col):
            q += f'COUNT({c}), '
        else:
            q += f'{c}, '

    # remove last comma
    q = q[:-2] + ' '

    q += f'FROM {table_name} '

    if where:

        q += 'WHERE '

        for wh in where:

            q += f"{wh[0]} {wh[1]} "

            if isinstance(wh[2], str):

                q += f"'{wh[2]}'"

            else:

                q += f"{wh[2]}"

            if len(wh) == 4:
                q += f' {wh[3].upper()} '

    if limit:

        q += f'LIMIT {limit}'

    q += ';'
    return q


{
    "table": str,
    "return_columns": [
        {
            "column_name": str,
            "func": str,
            "as": str
        },
        "func",
        {
            "column_name": str,
            "func": str,
            "as": str
        }
    ],
    "where": {
        "condition1": [
            {
                "column": str,
                "operator": str, # "=", "<>/!=", ">/>=", "</<=", "like/not like", "between"
                "condition": None, # int, float, str (for str or date operators), list (for between operator)
                "and/or": "and"
            },
            {
                "column": str,
                "operator": str,
                "condition": None,
                "and/or": None
            }
        ],
        "and/or1": "and",
        "condition2": [
            {
                "column": "language",
                "operator": "=",
                "condition": "French",
                "and/or": "or"
            },
            {
                "column": "language",
                "operator": "=",
                "condition": "Spanish",
                "and/or": None
            }
        ],
        'and/or2': "and",
        "condition3": [
            {
                "column": "gross",
                "operator": ">",
                "condition": 2000000,
                "and/or": None
            }
        ]
    },
    "limit": None
}

funcs_dict = {
    "distinct_count" : "DISTINCT(COUNT({}))",
    "distinct"       : "DISTINCT({})",
    "count"          : "COUNT({})",
    "max"            : "MAX({})",
    "min"            : "MIN({})",
    "avg"            : "AVG({})",
    "sum"            : "SUM({})",
}

def parse_query(query_dict: dict) -> str:

    q = """SELECT """

    # We check to see if there are any columns we want to return; if not,
    # we return all
    if not query_dict.get('return_columns'):
        query_dict['return_columns'] = ['*']

    for col in query_dict['return_columns']:

        if isinstance(col, dict):

            if col["func"]:

                q += funcs_dict[col["func"]].format(col["column_name"]) + """, """

            else:
                q += f"""{col}, """
            
            if col["as"]:

                q += f"""AS {col["as"]}"""

    # remove last comma
    q = q[:-2] + """ """
    q += f"""FROM {query_dict["table"]} """

    if query_dict.get("where"):

        q += """WHERE """

        for key in query_dict["where"]:

            if 'condition' in key:

                q += '('

                for cond in query_dict['where'][key]:

                    if cond['operator'] == 'between':

                        q += f"""{cond['column']} {cond['operator'].upper()} {cond['condition'][0]} AND {cond['condition'][1]}"""

                    elif cond['operator'] == 'in':

                        q += f"""{cond['column']} {cond['operator'].upper()} ("""
                        if isinstance(cond['condition'][0], str):
                            q += """, """.join([f"'{i}'" for i in cond['condition']])
                        else:
                            q += """, """.join([str(i) for i in cond['condition']])
                        q += ')'
                    
                    elif cond['operator'] == 'is':

                        q += f"""{cond['column']} {cond['operator'].upper()} {cond['condition'].upper()}"""

                    elif cond['operator'] in ['like', 'not like']:

                        q += f"""{cond['column']} {cond['operator'].upper()} '{cond['condition']}'"""

                    else:

                        q += f"""{cond['column']} {cond['operator']} """

                        if isinstance(cond['condition'], str):

                            q += f"""'{cond['condition']}'"""

                        elif isinstance(cond['condition'], (int, float)):

                            q += f"""{cond['condition']}"""

                    if cond["and/or"]:
                        q += f""" {cond["and/or"].upper()} """

                q += """)"""

            if 'and/or' in key:

                q += f""" {query_dict['where'][key].upper()} """

    if query_dict.get('limit'):

        q += f"""LIMIT {query_dict["limit"]}"""

    q += """;"""
    return q
