import faiss
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer


data = pd.read_parquet('df_overall_cleaned.parquet')

print('data loaded')
documents = data['sign'].sample(10_000).to_list()

vectorizer = TfidfVectorizer()
tfidf_matrix = vectorizer.fit_transform(documents).toarray().astype('float32')


dimension = tfidf_matrix.shape[1]

# SETUP IVF
nlist = 2
quantizer = faiss.IndexFlatL2(dimension)
index_ivf = faiss.IndexIVFFlat(quantizer, dimension, nlist, faiss.METRIC_L2)
index_ivf.train(tfidf_matrix)
index_ivf.add(tfidf_matrix)

def find_similar_pairs_ivf(query_vector, k=2):
    distances, indices = index_ivf.search(query_vector.reshape(1, -1), k)
    return distances, indices

# Run 1000 first raws to evaluate speed
for query_vector in tfidf_matrix[:1000]:
    distances_ivf, indices_ivf = find_similar_pairs_ivf(query_vector, k=3)
    for i, (dist, idx) in enumerate(zip(distances_ivf[0], indices_ivf[0])):
        if dist < 1.3:
            print(f"Rank {i+1}: Document {idx} (Distance: {dist:.4f}) -> '{documents[idx]}'")
    print()
