import faiss

# faiss.omp_set_num_threads(-1)
from sklearn.feature_extraction.text import TfidfVectorizer


def get_tfidf_artifacts(documents):
    vectorizer = TfidfVectorizer(lowercase=False)
    tfidf_matrix = vectorizer.fit_transform(documents).toarray().astype('float32')
    dimension = tfidf_matrix.shape[1]
    return tfidf_matrix, dimension


def create_index_hnsw(tfidf_matrix, dimension):
    index_hnsw = faiss.IndexHNSWFlat(dimension, 32)
    index_hnsw.train(tfidf_matrix)
    index_hnsw.add(tfidf_matrix)
    return index_hnsw


def create_index_ivf(tfidf_matrix, dimension):
    nlist = 2
    quantizer = faiss.IndexFlatL2(dimension)
    index_ivf = faiss.IndexIVFFlat(quantizer, dimension, nlist, faiss.METRIC_L2)
    index_ivf.train(tfidf_matrix[:dimension])
    index_ivf.add(tfidf_matrix)
    return index_ivf


def find_similar_pairs(index, query_vector, k=2):
    distances, indices = index.search(query_vector.reshape(1, -1), k)
    return distances, indices

