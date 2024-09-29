import pandas as pd

from src.utils.decorators import duration
from src.stage.stage1_helpers import simple_process_item, clean_phone_number, clean_bithdate

TOTALWORDS_LIMIT = 6


@duration
def stage1_run(data: tuple):
    df_1_raw, df_2_raw, df_3_raw = data

    # TO-DO figure out how to get column names from ch
    df_1 = pd.DataFrame(df_1_raw[0])
    df_1.columns = [x[0] for x in df_1_raw[1][-1]]

    df_2 = pd.DataFrame(df_2_raw[0])
    df_2.columns = [x[0] for x in df_2_raw[1][-1]]

    df_3 = pd.DataFrame(df_3_raw[0])
    df_3.columns = [x[0] for x in df_3_raw[1][-1]]

    df_1 = df_1.drop(['sex', 'email'], axis=1)
    df_3 = df_3.drop(['sex', 'email'], axis=1)

    df_2['full_name'] = df_2['last_name'] + ' ' + df_2['first_name'] + ' ' + df_2['middle_name']

    df_3['address'] = ''
    df_3['phone'] = ''
    df_3['full_name_reversed'] = df_3['name'].str.split(' ').str[-1] + ' ' + df_3['name'].str.split(' ').str[0] \
        if len(df_3['name'].str.split(' ')) == 2 \
        else df_3['name'].str.replace('  ', ' ').str.split(' ').str[1] + ' ' + \
             df_3['name'].str.replace('  ', ' ').str.split(' ').str[0]
    df_3 = df_3.rename(columns={'full_name_reversed': 'full_name'})

    df_1['source'] = 1
    df_2['source'] = 2
    df_3['source'] = 3

    target_cols = ['uid', 'birthdate', 'phone', 'address', 'full_name', 'source']

    df_overall = pd.concat([df_1[target_cols], df_2[target_cols], df_3[target_cols]], axis='rows')

    df_overall['full_name'] = df_overall['full_name'].astype('str')
    df_overall['full_name'] = df_overall['full_name'].apply(
        lambda x: simple_process_item(x, ['нет', 'отсутствует', 'углы', 'угли', 'оглы', 'огли']))

    print(
        df_overall.shape
    )
    mask = (df_overall['full_name'].str.len() > 0)
    df_overall = df_overall.loc[mask]
    print(
        df_overall.shape
    )

    df_overall['phone'] = df_overall['phone'].apply(clean_phone_number)

    df_overall['birthdate'] = df_overall['birthdate'].apply(clean_bithdate)

    df_overall['address'] = df_overall['address'].astype('str')
    df_overall['address'] = df_overall['address'].apply(lambda x: simple_process_item(x, ['квартира', 'строение', 'шоссе',
                                                                                          'улица', 'булица', 'село',
                                                                                          'город', 'поселок', 'деревня']))

    df_overall['sign'] = df_overall['full_name'] + ' ' + df_overall['phone'] + ' ' + df_overall['birthdate'] + ' ' + \
                         df_overall['address']


    df_overall['totalwords'] = df_overall['sign'].str.split(' ').str.len()

    df_overall = df_overall[df_overall.totalwords > TOTALWORDS_LIMIT]
    print(
        df_overall.shape
    )


    selected_df_overall = df_overall.sort_values(by=['full_name', 'phone', 'birthdate'], ascending=True)

    print(
        selected_df_overall[['full_name', 'phone', 'birthdate']].head(20)
    )

    selected_df_overall = selected_df_overall[['uid', 'sign', 'source']]

    print(
        selected_df_overall[['sign']].head(20)
    )
    return selected_df_overall
