from application import db

class movies(db.Document):
    Movie_Id        =   db.IntField(unique = True)
    Year            =   db.IntField()
    Name            =   db.StringField(max_length = 255)
    Link            =   db.StringField(max_length = 255)

class users(db.Document):
    Cust_Id     =   db.IntField(unique = True)
    First_Name  =   db.StringField(max_length = 200)
    Last_Name   =   db.StringField(max_length = 200)
    Email       =   db.StringField(max_length = 100, unique=True)
    Password    =   db.StringField( )

class ratings(db.Document):
    Cust_Id     =   db.IntField()
    Rating      =   db.IntField()
    Movie_Id    =   db.IntField()

class predictions(db.Document):
    Movie_Id            =   db.IntField()
    Cust_Id             =   db.IntField()
    Predicted_Rating    =   db.FloatField()