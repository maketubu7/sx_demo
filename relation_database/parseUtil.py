#encoding=utf-8

import pandas as pd


def get_table_schema_index():
    properties = r'table_schema_indices.csv'
    df = pd.read_csv(properties,index_col=None,sep='\t')
    df['index'] = df['index'].astype(int)
    table_indices = {}
    table_schemas = {}

    for k,v in df.groupby('tablename'):
        table_indices[k] = list(v['index'])
        table_schemas[k] = list(v['column'])
    return table_indices, table_schemas

table_indices, table_schemas = get_table_schema_index()


if __name__ == '__main__':
    print(get_table_schema_index())
