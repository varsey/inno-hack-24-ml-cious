import uuid

import pandas as pd

from src.utils.decorators import duration
from src.stage.stage2_helpers import get_tfidf_artifacts, find_similar_pairs, create_index_ivf, create_index_hnsw

THRESHOLD = 0.6
N_SYSTEMS = 3


def fill_row_by_pair(selected_df_overall: pd.DataFrame, n, indx, idx, indices) -> list:
    row = [[], [], []]

    first_indx = selected_df_overall['source'][indx:indx + n - 1].iloc[indices[0][0]]
    first_uid = selected_df_overall['uid'][indx:indx + n - 1].iloc[indices[0][0]]

    second_indx = selected_df_overall['source'][indx:indx + n - 1].iloc[idx]
    second_uid = selected_df_overall['uid'][indx:indx + n - 1].iloc[idx]

    for num in range(N_SYSTEMS):
        if num + 1 == first_indx:
            row[num].append(first_uid)
        if num + 1 == second_indx:
            row[num].append(second_uid)
    return row

@duration
def stage2_run(selected_df_overall: pd.DataFrame):
    result = pd.DataFrame(columns=[1, 2, 3])

    N = 100
    for indx in range(0, len(selected_df_overall), N):
        if indx / N % 1000 == 0:
            print(f'working with batch {indx / N} out of {len(selected_df_overall) / N}')
        documents = selected_df_overall['sign'][indx:indx + N - 1].to_list()
        tfidf_matrix, dimension = get_tfidf_artifacts(documents)

        index = create_index_hnsw(tfidf_matrix, dimension)

        for query_vector in tfidf_matrix:
            distances, indices = find_similar_pairs(index, query_vector, k=2)
            for i, (dist, idx) in enumerate(zip(distances[0], indices[0])):
                if dist < THRESHOLD and dist != 0 and idx != indices[0][0]:
                    result.loc[uuid.uuid4()] = fill_row_by_pair(selected_df_overall, N, indx, idx, indices)

    print('Done')

    print(result.shape)

    print(result.sample(5))
