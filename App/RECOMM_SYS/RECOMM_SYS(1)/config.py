import os

class Config(object):
    SECRET_KEY = os.environ.get('SECRET KEY') or b'\xef;\xcc\xd8\xe6\xa0;\x97\xa4T\xb9\xdc2\xb6\x95\xf7'

    MONGODB_SETTINGS = {'db':'recommender_system'}
