import sqlite3

def db():
   return sqlite3.connect('classr.db')

def cursor():
   return db().cursor()

