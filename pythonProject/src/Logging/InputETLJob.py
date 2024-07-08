import pandas as pd
import csv

data = [
    ["", "89898999", '56', "AA34", 'Y', 126333],
    ['250', "129898999", '156', "AA345", 'Y', 122333],
    ['256', "898989991", '256', "AA342", 'N', 121233]
]
valid_df = []
in_valid_df = []

with open('../Schemas/AuthorizedItemOutbound.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['OpsiteNumber', 'CustomerNumber', 'ItemNumber',
                     'PriceListNumber', 'DeletedEnd', 'CreatedTime'])
    writer.writerows(data)

dtypes = {"OpsiteNumber": str, "CustomerNumber": str, "ItemNumber": str,
          'PriceListNumber': str, 'DeletedEnd': str, 'Createdtime': int}

df_csv = pd.read_csv('../Schemas/AuthorizedItemOutbound.csv', dtype=dtypes)
df_csv = pd.DataFrame(df_csv)

df_json = pd.read_json('../Schemas/AuthorizedItemOutbound.json')
df_json = pd.DataFrame(df_json)

mapping_path = "../Schemas/AuthorizedItemOutbound.csv"
split_paths = mapping_path.split("/")
prefix_file_name = split_paths[len(split_paths) - 1].split(".")[0]
df_mapping = pd.read_json("../Schemas/Input_file_db_mapping.json")


def Input_column_name_validation(input_file, json_file):
    is_valid = True
    for column in input_file.columns:
        if column not in json_file.columns:
            is_valid = False
            return is_valid
    return is_valid


var = df_mapping[prefix_file_name]["unique_columns"]


def datatype_Column_validation(json_file, df_csv):
    for index, rows in df_csv.iterrows():
        is_valid = True
        for column, schema in json_file.items():
            actual_dt_type = schema['type']
            actual_value = rows[column]
            is_nullable = schema.get("nullable", True)

            if pd.isna(actual_value) and not is_nullable:
                is_valid = False
            if actual_dt_type == "String" and not isinstance(actual_value, str):
                is_valid = False
            if actual_dt_type == "Integer" and not isinstance(actual_value, int):
                is_valid = False

        if is_valid:
            valid_df.append(rows)
        else:
            in_valid_df.append(rows)

    return pd.DataFrame(valid_df), pd.DataFrame(in_valid_df)


df_valid = pd.DataFrame()
df_invalid = pd.DataFrame()

if Input_column_name_validation(df_csv, df_json):
    try:
        if var:
            duplicates_subset = df_csv[df_csv.duplicated(subset=var)]
            if duplicates_subset:
                for index, row in duplicates_subset.iterrows():
                    in_valid_df.append(row)
                df_invalid = pd.DataFrame(in_valid_df)
            else:
                df_valid, df_invalid = datatype_Column_validation(df_json, df_csv)
        else:
            df_valid, df_invalid = datatype_Column_validation(df_json, df_csv)

    except Exception as e:
        print(e)

else:
    print("Invalid records")


def column_mapping_for_db_schema(mapping, file, prefixOf_file_name):
    file.rename(columns=mapping[prefixOf_file_name]["columns"], inplace=True)


column_mapping_for_db_schema(df_mapping, df_valid, prefix_file_name)
column_mapping_for_db_schema(df_mapping, df_invalid, prefix_file_name)

print("Valid records")
print(df_valid.to_string(), "\n")

print("inValid records")
print(df_invalid.to_string())
