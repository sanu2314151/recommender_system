from flask import Flask, render_template, request, redirect, url_for, flash, session, get_flashed_messages 
from flask_wtf import FlaskForm
from wtforms import StringField, PasswordField, SubmitField, BooleanField
from wtforms.validators import DataRequired, Email, Length, EqualTo
from application import app, db
from application.models import movies, users, ratings, predictions 
import hashlib, pandas as pd 
from pymongo import MongoClient, errors, time, secrets, string, re
from surprise import Dataset, Reader, SVD, dump
from surprise.model_selection import train_test_split
from pymongo.errors import PyMongoError
from flask_mail import Mail, Message
from flask_login import login_user
from definations import (
    validate_email,
    fetch_movies,
    store_user_ratings,
    fetch_user_ratings,
    train_svd,
    predict_and_store_recommendations,
    generate_otp,
    save_otp,
    verify_otp
)
df_mov_titles = pd.read_csv('C:\\Users\\Sam\\Documents\\GitHub\\recommender_system\\data/movie_titles_v2.csv',
                             sep='|',
                             header=0,
                             usecols=[0, 1, 2, 3],
                             encoding="ISO-8859-1",
                             dtype={'Year': 'str', 'Name': 'str', 'Link': 'str'})
df_mov_titles.set_index('Movie_Id', inplace=True)

# Load ratings data
df_ratings_XS = pd.read_csv('C:/Users/Sam/Documents/GitHub/recommender_system/data/df_ratings_XS.zip', sep='|', header=0)
print('Ratings shape: {}'.format(df_ratings_XS.shape))

# Registration Form
class RegistrationForm(FlaskForm):
    email = StringField('Email', validators=[DataRequired(), Email()])
    password = PasswordField('Password', validators=[DataRequired(), Length(min=6, max=15)])
    confirm_password = PasswordField('Confirm Password', validators=[DataRequired(), EqualTo('password')])
    first_name = StringField('First Name', validators=[DataRequired()])
    last_name = StringField('Last Name', validators=[DataRequired()])
    submit = SubmitField('Register')

# Login Form 
class LoginForm(FlaskForm):
    email = StringField("Email", validators=[DataRequired(), Email()])
    password = PasswordField("Password", validators=[DataRequired(), Length(min=6, max=15)])
    remember_me = BooleanField("Remember me")
    submit = SubmitField("Login")
@app.route("/")
def index():
    return render_template("index.html", navindex=True)

@app.route("/catalog")
def catalog():
    moviecat_ = movies.objects.all()
    return render_template("catalog.html", navcatalog=True, moviecat=moviecat_)

@app.route("/reviews")
def reviews():
    if not session.get('Cust_Id'):
        flash(f"Please login to access reviews", "danger")
        return redirect(url_for('login')) 
    custid = session.get('Cust_Id')
    ratings_ = list(ratings.objects.aggregate(* [  # MongoDB aggregation pipeline
        {'$match': {'Cust_Id':custid} },
        {'$lookup': {'from':'movies', 'localField':'Movie_Id', 'foreignField':'Movie_Id', 'as':'Movie_Info'} },
        {'$unwind': '$Movie_Info'},
        {'$group': {'_id': '$Movie_Id', 'Rating': {'$first': '$Rating'}, 'Movie_Info': {'$first': '$Movie_Info'}}},
        {'$project': {'Cust_Id':1, 'Rating':1, 'Movie_Id':1, 'Movie_Info.Name':1, 'Movie_Info.Link':1 } },
        {'$sort': { 'Rating':-1, 'Movie_Info.Name':1}}
    ]))
    return render_template("reviews.html", navreviews=True, ratings=ratings_)

@app.route("/recommend")
def recommend():
    if not session.get('Cust_Id'):
        flash(f"Please login to access recommendations", "danger")
        return redirect(url_for('login'))  # If not logged in, redirect user to login page
    custid = session.get('Cust_Id')
    preds_ = list(predictions.objects.aggregate(* [ # MongoDB aggregation pipeline
        {'$match': {'Cust_Id':custid} },
        {'$match': {'Predicted_Rating': {'$gte':3.0} } },
        {'$lookup': {'from':'movies', 'localField':'Movie_Id', 'foreignField':'Movie_Id', 'as':'Movie_Info'} },
        {'$unwind': '$Movie_Info'},
        {'$group': {'_id': '$Movie_Id', 'Predicted_Rating': {'$first': '$Predicted_Rating'}, 'Movie_Info': {'$first': '$Movie_Info'}}},
        {'$project': {'Cust_Id':1, 'Predicted_Rating':{'$round': ['$Predicted_Rating',2]}, 'Movie_Id':1, 'Movie_Info.Name':1, 'Movie_Info.Link':1 } },
        {'$sort': { 'Predicted_Rating':-1, 'Movie_Info.Name':1}}
    ]))
    
    return render_template("recommend.html", navrecommend=True, predictions=preds_)

@app.route("/login", methods=["GET", "POST"])
def login():
    if session.get('Cust_Id'):
        return redirect(url_for('index'))  # If already logged in, redirect to home

    form = LoginForm()
    if form.validate_on_submit():
        femail = request.form.get("email")
        fpassword = request.form.get("password")
        user = users.objects(Email=femail).first()
        if user and user.Password == fpassword:  # Direct password comparison
            flash(f"Welcome back, {user.First_Name}", "success")
            session['Cust_Id'] = user.Cust_Id
            session['First_Name'] = user.First_Name
            session['Last_Name'] = user.Last_Name
            return redirect("/")
        else:
            flash("Sorry, login failed", "danger")
    return render_template("login.html", form=form, navlogin=True)
    
@app.route("/logout")
def logout():
    session['Cust_Id'] = False
    session.pop('First_Name', None)
    session.pop('Last_Name', None)
    return redirect("/")

@app.route('/register', methods=['GET', 'POST'])
def register():
    form = RegistrationForm()
    if form.validate_on_submit():
        email = form.email.data
        password = form.password.data
        first_name = form.first_name.data
        last_name = form.last_name.data
        if not validate_email(email):
            flash('Invalid email address', 'danger')
            return render_template('register.html', form=form, navregister=True)

        existing_user = users.objects(Email=email).first()
        if existing_user:
            flash('A user with this email already exists.', 'danger')
            return redirect(url_for('register'))
        def generate_unique_cust_id(email):
            hashed_email = hashlib.sha256(email.encode()).hexdigest()
            cust_id = abs(int(hashed_email, 16)) % 2147483647  # Modulus by 2^31 - 1 to fit within IntField range
            return cust_id
        cust_id = generate_unique_cust_id(email)
        new_user = users(Cust_Id=cust_id, Email=email, Password=password, First_Name=first_name, Last_Name=last_name)
        new_user.save()
        flash('Registration successful!', 'success')
        return redirect(url_for('login'))
    return render_template('register.html', form=form, navregister=True)

@app.route('/rate_movies', methods=['GET', 'POST'])
def rate_movies():
    if 'Cust_Id' not in session:
        return redirect(url_for('login'))
    client = MongoClient('mongodb+srv://smd:231s4151s@cluster0.9rfkzg0.mongodb.net/?retryWrites=true&w=majority')
    db = client['recommender_system']
    movies_collection = db.movies
    movies = list(movies_collection.find({}, {'_id': 1, 'Movie_Id': 1, 'Name': 1, 'Thumbnail': 1, 'ShortDescription': 1}).limit(200))
    
    if request.method == 'POST':
        Movie_Id = request.form.get('Movie_Id')
        rating = request.form.get('rating')
        user_id = session['Cust_Id']
        store_user_ratings(user_id, Movie_Id, rating)
        if not get_flashed_messages():
            flash("Rating stored successfully.", "success")
        return redirect(url_for('rate_movies'))

    return render_template('rate_movies.html', movies=movies)

reader = Reader(rating_scale=(1, 5))
data = Dataset.load_from_df(df_ratings_XS[['Cust_Id', 'Movie_Id', 'Rating']], reader)
@app.route('/recommend_movies')

def recommend_movies():
    if not session.get('Cust_Id'):
        flash('Please log in to see recommendations', 'danger')
        return redirect(url_for('login'))

    user_id = session['Cust_Id']
    user_ratings = fetch_user_ratings(user_id)
    if not user_ratings:
        flash("Please provide at least one rating to get recommendations.", "warning")
        return redirect(url_for('rate_movies'))

    try:
        _, svd = dump.load(file_name="svd_model")
        reader = Reader()
        new_user_data = Dataset.load_from_df(user_ratings, reader)
        trainset = data.build_full_trainset()
        trainset = trainset.append_user_data(new_user_data)
        svd.fit(trainset)
        new_user_predictions = []
        unrated_movies = [movie_id for movie_id in df_mov_titles.index if (user_id, movie_id) not in new_user_data.raw_ratings]
        for movie_id in unrated_movies:
            prediction = svd.predict(user_id, movie_id)
            new_user_predictions.append((movie_id, prediction.est))
        for movie_id, prediction in new_user_predictions:
            db.predictions.update_one(
                {'Cust_Id': user_id, 'Movie_Id': movie_id},
                {'$set': {'Predicted_Rating': prediction}},
                upsert=True
            )
        recommended_movies = get_recommendations(user_id)

        return render_template('recommend_movies.html', movies=recommended_movies)

    except Exception as e:
        print(f"An error occurred while generating recommendations: {e}")
        return "An error occurred while generating recommendations."

@app.route('/forgot-password', methods=['GET', 'POST'])
def forgot_password():
    if request.method == 'POST':
        email = request.form.get('email')
        user = users.objects(Email=email).first()
        if user:
            otp = generate_otp()
            save_otp(email, otp)
            send_otp_email(email, otp)

            flash('OTP has been sent to your email.', 'success')
            return redirect(url_for('verify_otp', email=email))
        else:
            flash('No user found with the provided email.', 'danger')
    return render_template('forgot-password.html')

client = MongoClient('mongodb+srv://smd:231s4151s@cluster0.9rfkzg0.mongodb.net/?retryWrites=true&w=majority')

OTP_EXPIRATION_TIME = 300 # Set the OTP expiration time (in seconds)  
app.config.update(
    MAIL_SERVER='smtp.gmail.com',
    MAIL_PORT=587,
    MAIL_USE_TLS=True,
    MAIL_USERNAME='sdhal@gmail.com',
    MAIL_PASSWORD='ppsv stsc cwzq rhav'
)
mail = Mail(app)

def send_otp_email(email, otp):
    msg = Message(
        subject='Your OTP for password reset',
        sender='sdhal@gmail.com',
        recipients=[email]
    )
    msg.body = f"Your OTP is: {otp}"
    mail.send(msg)
from flask_login import login_user

@app.route('/reset-password/<email>', methods=['GET', 'POST'])
def reset_password(email):
    if request.method == 'POST':
        new_password = request.form.get('new_password')
        confirm_password = request.form.get('confirm_password')

        if new_password != confirm_password:
            flash('Passwords do not match.', 'danger')
            return render_template('reset-password.html', email=email)

        user = users.objects(Email=email).first()
        user.update(set__Password=new_password)
        flash('Password reset successfully.', 'success')
        return redirect(url_for('login'))

    return render_template('reset-password.html', email=email)

@app.route('/verify-otp/<email>', methods=['GET', 'POST'])
def verify_otp(email):
    if request.method == 'POST':
        otp = request.form.get('otp')
        if verify_otp(email, otp):
            return redirect(url_for('reset_password', email=email))
        else:
            flash('Invalid OTP.', 'danger')
    return render_template('verify-otp.html', email=email)

if __name__ == '__main__':
    app.run(debug=True)  