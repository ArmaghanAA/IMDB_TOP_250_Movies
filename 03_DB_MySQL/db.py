import mysql.connector
import sqlalchemy as db
from sqlalchemy.orm import declarative_base, Mapped, mapped_column
from sqlalchemy.orm import sessionmaker
import csv
import pandas as pd


password_DB = input('Enter the MySQL password:\n')
connection_string = f'mysql+mysqlconnector://root:{password_DB}@localhost'
engine = db.create_engine(connection_string)

#---------------- create new DB-----------------

DB_name = 'imdb_db'
with engine.connect() as conn:
    conn.execute(db.text(f"DROP DATABASE IF EXISTS {DB_name}"))
    conn.execute(db.text(f"CREATE DATABASE {DB_name}"))
    conn.execute(db.text(f"USE {DB_name}"))

connection_string = f'mysql+mysqlconnector://root:{password_DB}@localhost/{DB_name}'
engine = db.create_engine(connection_string)
Session=sessionmaker(bind=engine)
#-------------------create Tables-----------------------
Base=declarative_base()

#---movie Table-----
class movies(Base):
    __tablename__ = 'movie'
    id : Mapped [str] = mapped_column(db.String(8),primary_key=True)
    title : Mapped [str] = mapped_column(db.String(127),nullable=True)
    year : Mapped [int] = mapped_column(db.Integer,nullable=True)
    runtime : Mapped [int] = mapped_column(db.Integer,nullable=True)
    parental_guide : Mapped [str] = mapped_column(db.String(10),nullable=True)
    gross_us_canada : Mapped [int] = mapped_column(db.Integer,nullable=True)
    GENRES : Mapped ["genres"] = db.orm.relationship(back_populates="MOVIES1")
    CREWS2 : Mapped ["crews"] = db.orm.relationship(back_populates="MOVIES2")
    CASTS2 : Mapped ["casts"] = db.orm.relationship(back_populates="MOVIES3")


#-------person Table---------
class persons(Base):
    __tablename__ = 'person'
    id: Mapped [str] = mapped_column(db.String(10), primary_key=True,nullable=True)
    name: Mapped [str] = mapped_column(db.String(32),nullable=True)
    CASTS1: Mapped ["casts"] = db.orm.relationship(back_populates="PERSONS1")
    CREWS1 : Mapped ["crews"] = db.orm.relationship(back_populates="PERSONS2")
#-----------cast Table-----------
class casts(Base):
    __tablename__ = 'cast'
    id: Mapped [int] = mapped_column(db.Integer,primary_key=True,autoincrement=True,nullable=True)
    movie_id : Mapped [str] = mapped_column(db.String(8),db.ForeignKey('movie.id'),nullable=True)
    person_id : Mapped [str] = mapped_column(db.String(8),db.ForeignKey('person.id'),nullable=True)
    MOVIES3 : Mapped["movies"] = db.orm.relationship(back_populates="CASTS2")
    PERSONS1 : Mapped ["persons"] = db.orm.relationship(back_populates="CASTS1")
#--------crew Table-------------
class crews(Base):
    __tablename__ = 'crew'
    id: Mapped [int] = mapped_column(db.Integer,primary_key=True,autoincrement=True,nullable=True)
    movie_id: Mapped [str] = mapped_column(db.String(8),db.ForeignKey('movie.id'),nullable=True)
    person_id: Mapped [str] = mapped_column(db.String(8),db.ForeignKey('person.id'),nullable=True)
    role: Mapped [str] = mapped_column(db.String(8),nullable=True)
    PERSONS2: Mapped ["persons"] = db.orm.relationship(back_populates="CREWS1")
    MOVIES2 : Mapped ["movies"] = db.orm.relationship(back_populates="CREWS2")
#------genre Table-------------
class genres(Base):
    __tablename__ = 'genre'
    id: Mapped [int] = mapped_column(db.Integer,primary_key=True,autoincrement=True,nullable=True)
    movie_id: Mapped [str] = mapped_column(db.String(8),db.ForeignKey('movie.id'),nullable=True)
    genre: Mapped [str] = mapped_column(db.String(16),nullable=True)
    MOVIES1 : Mapped ["movies"] = db.orm.relationship(back_populates="GENRES")

Base.metadata.create_all(engine)
#-----------------------INSERT DATA------------------------
session=Session()

movie_df = pd.read_csv('movie.csv')
movie_df['parental_guide'].fillna('Unrated',inplace=True)
movie_df['parental_guide'].replace('','Unrated',inplace=True)
movie_df['parental_guide'].replace('Not Rated','Unrated',inplace=True)

person_df = pd.read_csv('person.csv')
cast_df = pd.read_csv('cast.csv')
crew_df = pd.read_csv('crew.csv')
genre_df = pd.read_csv('genre.csv')

with engine.connect() as conn:
    movie_df.to_sql('movie',conn,if_exists='append',index=False)
    person_df.to_sql('person',conn,if_exists='append',index=False)
    cast_df.to_sql('cast',conn,if_exists='append',index=False)
    crew_df.to_sql('crew',conn,if_exists='append',index=False)
    genre_df.to_sql('genre',conn,if_exists='append',index=False)


session.commit()

session.close()