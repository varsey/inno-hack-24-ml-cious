import pandas as pd

from src.utils.decorators import duration
from src.stage.stage1_helpers import simple_process_item, clean_phone_number, clean_bithdate
from src.utils.logger import DicLogger, LOGGING_CONFIG

log = DicLogger(LOGGING_CONFIG).log

TOTALWORDS_LIMIT = 6
FNAME_STOPWORDS = ['нет', 'отсутствует', 'углы', 'угли', 'оглы', 'огли']
ADDR_STOPWORDS = ['квартира', 'строение', 'шоссе', 'улица', 'булица', 'село', 'город', 'поселок', 'деревня']
TARGET_COLS = ['uid', 'birthdate', 'phone', 'address', 'full_name', 'source']

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
    # Put last name to first place
    df_3['full_name_reversed'] = df_3['name'].str.split(' ').str[-1] + ' ' + df_3['name'].str.split(' ').str[0] \
        if len(df_3['name'].str.split(' ')) == 2 \
        else df_3['name'].str.replace('  ', ' ').str.split(' ').str[1] + ' ' + \
             df_3['name'].str.replace('  ', ' ').str.split(' ').str[0]
    df_3 = df_3.rename(columns={'full_name_reversed': 'full_name'})

    df_1['source'] = 1
    df_2['source'] = 2
    df_3['source'] = 3

    df_overall = pd.concat([df_1[TARGET_COLS], df_2[TARGET_COLS], df_3[TARGET_COLS]], axis='rows')

    df_overall['full_name'] = df_overall['full_name'].astype('str')
    df_overall['full_name'] = df_overall['full_name'].apply(
        lambda x: simple_process_item(x, FNAME_STOPWORDS))

    # Filter out empty fnames
    log.info(f'{df_overall.shape}')
    mask = (df_overall['full_name'].str.len() > 0)
    df_overall = df_overall.loc[mask]
    log.info(f'{df_overall.shape}')

    df_overall['phone'] = df_overall['phone'].apply(clean_phone_number)
    df_overall['birthdate'] = df_overall['birthdate'].apply(clean_bithdate)

    df_overall['address'] = df_overall['address'].astype('str')
    df_overall['address'] = df_overall['address'].apply(lambda x: simple_process_item(x, ADDR_STOPWORDS))

    df_overall['sign'] = df_overall['full_name'] + ' ' + df_overall['phone'] + ' ' + df_overall['birthdate'] + ' ' + \
                         df_overall['address']

    # Filter out short sentences
    df_overall['totalwords'] = df_overall['sign'].str.split(' ').str.len()
    df_overall = df_overall[df_overall.totalwords > TOTALWORDS_LIMIT]
    log.info(f'{df_overall.shape}')

    selected_df_overall = df_overall.sort_values(by=['full_name', 'phone', 'birthdate'], ascending=True)

    print(
        selected_df_overall[['full_name', 'phone', 'birthdate']].head(20)
    )

    return selected_df_overall[['uid', 'sign', 'source']]
