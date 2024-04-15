import hashlib, re
from pymongo import MongoClient, errors
from surprise import dump

def validate_email(email):
    pattern = r'^[a-zA-Z0-9_.+-]+@(?:gmail\.com|yahoo\.com|outlook\.com|proton\.me|hotmail\.com|icloud\.com)$'
    return re.match(pattern, email) is not None

def fetch_movies():
    client = MongoClient('mongodb+srv://smd:231s4151s@cluster0.9rfkzg0.mongodb.net/?retryWrites=true&w=majority')
    db = client['recommender_system']
    collection = db['movies']
    movies = list(collection.find({}, {'_id': 0, 'Movie_Id': 1, 'Name': 1}))
    return movies

def store_user_ratings(cust_id, Movie_Id, rating):
    try:
        client = MongoClient('mongodb+srv://smd:231s4151s@cluster0.9rfkzg0.mongodb.net/?retryWrites=true&w=majority')
        db = client['recommender_system']
        result = db.ratings.insert_one({
            'Cust_Id': cust_id,
            'Rating': rating,
            'Movie_Id': Movie_Id
            })
    except errors.ServerSelectionTimeoutError as err:
        print("Failed to connect to server:", err)
    except Exception as err:
        print("An error occurred:", err)

def fetch_user_ratings(cust_id):
    client = MongoClient('mongodb+srv://smd:231s4151s@cluster0.9rfkzg0.mongodb.net/?retryWrites=true&w=majority')
    db = client['recommender_system']
    ratings_collection = db.ratings  
    user_ratings = ratings_collection.find({'Cust_Id': cust_id})
    return list(user_ratings)

def train_svd(ratings):
    user_ratings = pd.DataFrame.from_dict(ratings, orient='index').reset_index()
    user_ratings.columns = ['user', 'item', 'rating']
    reader = Reader(rating_scale=(1, 5))
    data = Dataset.load_from_df(user_ratings, reader)
    algo = SVD()
    trainset = data.build_full_trainset()
    algo.fit(trainset)
    return algo

def predict_and_store_recommendations(user_id, algo):
    try:
        client = MongoClient('mongodb+srv://smd:231s4151s@cluster0.9rfkzg0.mongodb.net/?retryWrites=true&w=majority')
        db = client['recommender_system']
        movies = list(db.movies.find())
        predictions = [algo.predict(str(user_id), str(movie['_id']), verbose=False) for movie in movies]
        for pred in predictions:
            db.predictions.update_one(
                {'user_id': user_id, 'Movie_Id': pred.iid},
                {'$set': {'predicted_rating': pred.est}},
                upsert=True
            )
    except errors.ServerSelectionTimeoutError as err:
        print("Failed to connect to server:", err)
    except Exception as err:
        print("An error occurred:", err)

def get_user_ratings(user_id):
    try:
        client = MongoClient('mongodb+srv://smd:231s4151s@cluster0.9rfkzg0.mongodb.net/?retryWrites=true&w=majority')
        db = client['recommender_system']
        user_ratings = db.ratings.find({'user_id': user_id})
        ratings = {str(rating['Movie_Id']): rating['rating'] for rating in user_ratings}
        return ratings
    except errors.ServerSelectionTimeoutError as err:
        print("Failed to connect to server:", err)
        return None
    except Exception as err:
        print("An error occurred:", err)
        return None
    
def get_recommendations(user_id):
    recommendations = predictions.objects(Cust_Id=user_id).order_by('-Predicted_Rating')
    print("Query:", predictions.objects(Cust_Id=user_id).order_by('-Predicted_Rating').explain())
    recommended_movies = []
    for rec in recommendations:
        movie = movies.objects(Movie_Id=rec.Movie_Id).first()
        if movie:
            movie.Predicted_Rating = rec.Predicted_Rating
            recommended_movies.append(movie)
    
    return recommended_movies

def generate_otp():
    return ''.join(secrets.choice(string.digits) for _ in range(6))# Generate a 6-digit OTP
client = MongoClient('mongodb+srv://smd:231s4151s@cluster0.9rfkzg0.mongodb.net/?retryWrites=true&w=majority')


def save_otp(email, otp):
    db = client['recommender_system']
    otp_records = db['otp_records']
    expiration_time = time.time() + OTP_EXPIRATION_TIME
    otp_record = {
        'email': email,
        'otp': otp,
        'expiration_time': expiration_time
    }
    db.otp_records.insert_one(otp_record)

def verify_otp(email, otp):
    db = client['recommender_system']
    otp_records = db['otp_records']
    current_time = time.time()
    otp_record = otp_records.find_one({'email': email})
    print(f"Retrieved OTP record: {otp_record}")
    
    if otp_record:
        print(f"Stored OTP: {otp_record['otp']}")
        print(f"Expiration time: {otp_record['expiration_time']}")
    
    if otp_record and otp_record['otp'] == otp and otp_record['expiration_time'] > current_time:
        otp_records.delete_one({'email': email})
        return True
    return False
