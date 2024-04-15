from pymongo import MongoClient
from pymongo.client_options import ServerApi
import time


uri = "mongodb+srv://smd:231s4151s@cluster0.9rfkzg0.mongodb.net/?retryWrites=true&w=majority"
client = MongoClient(uri, server_api=ServerApi('1'))

# Save into collections
db = client['recommender_system']

def write_data(coll_name, df_):
    st = time.time()
    collection = db[coll_name]
    initial_count = collection.count_documents({})
    to_upload = df_.to_dict('records')
    collection.insert_many(to_upload)
    end_count = collection.count_documents({})
    elapsed_ = time.strftime("%Hh%Mm%Ss", time.gmtime(time.time() - st))
    print(f"{end_count - initial_count} documents inserted into {coll_name}\nData upload completed in {elapsed_}")
    