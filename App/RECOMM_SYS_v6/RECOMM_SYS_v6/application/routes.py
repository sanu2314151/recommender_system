from application import app, db
from flask import render_template, url_for, request, redirect, flash, session
from application.models import movies, users, ratings, predictions
from application.forms import LoginForm

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
        return redirect(url_for('login')) # If not logged in, redirect user to login page
    custid = session.get('Cust_Id')
    ratings_ = list(ratings.objects.aggregate(* [  # MongoDB aggregation pipeline
        {'$match': {'Cust_Id':custid} },
        {'$lookup': {'from':'movies', 'localField':'Movie_Id', 'foreignField':'Movie_Id', 'as':'Movie_Info'} },
        {'$unwind': '$Movie_Info'},
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
        # Get form data
        femail = request.form.get("email")
        fpassword = request.form.get("password")

        # Compare data against db
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