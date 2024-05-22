# This code will give the filenames present in the s3 folder in a csv file
# Input arguments required are
# Credentials, input bucket name, prefix for exact path of the folder
import boto3
import pandas as pd

for profile in boto3.session.Session().available_profiles:
    print(profile)
session = boto3.Session(profile_name="254280563083_AWSAdministratorAccess")
s3 = session.client("s3")
credentials = session.get_credentials()
credentials = credentials.get_frozen_credentials()

print(credentials)

lst_obj = s3.list_objects_v2(Bucket="imcd-bi-bedrock", MaxKeys=1000, Prefix="TDS/")

while lst_obj["IsTruncated"]:
    lst_obj = s3.list_objects_v2(
        Bucket="imcd-bi-bedrock",
        MaxKeys=1000,
        StartAfter=lst_obj["Contents"][-1]["Key"],
        Prefix="TDS/",
        Delimiter="TDS/",
    )

    contents = lst_obj.get("Contents")
    char_re = [
        "{",
        "}",
        ":",
        """Key""",
        "Owner",
        "ID",
        "ETag",
        "LastModified",
        "StorageClass",
        "Size",
        '"',
        "[",
        "]",
    ]
    str_param = str(contents)
    for i in char_re:
        print(i)
        str_param = str_param.replace(
            str(i),
            "",
        )
    str_param = (
        str_param.replace(" ", "")
        .replace("''", "")
        .replace("datetime.", "'datetime.")
        .replace("),", ")',")
        .strip()
    )
    str_param = str_param.replace(",'", "','").replace("',", "','").replace("''", "'")
    my_list = [str_param]
    list_ob = str_param.split(",")
    print(list_ob)
    df = pd.DataFrame({"col": list_ob})
    df = df[df["col"].str.contains("TDS/documentContent")]
    df.to_csv(
        r"C:\Users\NL112GPA\OneDrive - IMCD Group\Desktop\TDS\missing_list.csv",
        index=False,
        mode="a",
        header=False,
    )
