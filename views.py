import random
from flask import Flask, jsonify, request, render_template, redirect, url_for
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, insert, exc, Column, Integer, String, ForeignKey, DateTime, Text, Boolean
from sqlalchemy.sql import func
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import logging
from logging.handlers import RotatingFileHandler
import subprocess
import sys
import os

# Ensure libraries are installed
required_libraries = [
    'psycopg2-binary',
    'sqlalchemy'
]

def install_libraries(libraries):
    for lib in libraries:
        subprocess.check_call([sys.executable, "-m", "pip", "install", lib])

install_libraries(required_libraries)

# Function to execute bash commands
def execute_bash_command(command):
    result = subprocess.run(command, shell=True, capture_output=True, text=True)
    if result.returncode != 0:
        raise Exception(f"Command failed: {command}\n{result.stderr}")
    return result.stdout

# Ensure PostgreSQL is installed and create a new user and database if they do not exist
def setup_postgresql():
    try:
        # Check if PostgreSQL is installed
        execute_bash_command("psql -V")
    except Exception:
        # Install PostgreSQL
        install_commands = """
        sudo apt update && \
        sudo apt install -y curl ca-certificates && \
        sudo install -d /usr/share/postgresql-common/pgdg && \
        sudo curl -o /usr/share/postgresql-common/pgdg/apt.postgresql.org.asc --fail https://www.postgresql.org/media/keys/ACCC4CF8.asc && \
        sudo sh -c 'echo "deb [signed-by=/usr/share/postgresql-common/pgdg/apt.postgresql.org.asc] https://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list' && \
        sudo apt update && \
        sudo apt -y install postgresql
        """
        execute_bash_command(install_commands)

    # Ensure the PostgreSQL service is started
    execute_bash_command("sudo service postgresql start")

    # Create a new PostgreSQL user and database if they don't exist
    create_user_db_commands = f"""
    sudo -i -u postgres psql -c "DO \\
    \$do\$ \\
    BEGIN \\
       IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = '{username}') THEN \\
           CREATE ROLE {username} WITH LOGIN PASSWORD '{password}'; \\
       END IF; \\
    END \\
    \$do$;"
    
    sudo -i -u postgres psql -c "DO \\
    \$do\$ \\
    BEGIN \\
       IF NOT EXISTS (SELECT FROM pg_database WHERE datname = '{database}') THEN \\
           CREATE DATABASE {database} OWNER {username}; \\
       END IF; \\
    END \\
    \$do$;"
    
    sudo -i -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE {database} TO {username};"
    """
    execute_bash_command(create_user_db_commands)

setup_postgresql()

# Creating the Database Tables and Database Owner if not exist

app = Flask(__name__)

# Logging
handler = RotatingFileHandler('app.log', maxBytes=10000, backupCount=3)
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(handler)

# Database connection
host = "localhost"
port = "5432"
database = "karim_db"
username = "karim"
password = "Karim123*"
connection_string = f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}"
engine = create_engine(connection_string)
metadata = MetaData(bind=engine)

# Define tables based on UML model
users = Table('users', metadata,
    Column('user_id', Integer, primary_key=True),
    Column('username', String, nullable=False),
    Column('email', String, unique=True, nullable=True),
    Column('timestamp', DateTime, nullable=True),
    Column('is_active', Boolean, nullable=True)
)

content = Table('content', metadata,
    Column('content_id', Integer, primary_key=True),
    Column('text', Text, nullable=True),
    Column('geostamp', String, nullable=True),
    Column('timestamp', DateTime, nullable=True)
)

media = Table('media', metadata,
    Column('media_id', Integer, primary_key=True),
    Column('post_id', Integer, ForeignKey('post.post_id'), nullable=True),
    Column('url', String, nullable=True),
    Column('type', String, nullable=True),
    Column('timestamp', DateTime, nullable=True)
)

post = Table('post', metadata,
    Column('post_id', Integer, primary_key=True),
    Column('user_id', Integer, ForeignKey('users.user_id'), nullable=True),
    Column('content_id', Integer, ForeignKey('content.content_id'), nullable=True),
    Column('timestamp', DateTime, nullable=True),
    Column('media_type', Text, nullable=True),
    Column('media_url', String, nullable=True),
    Column('caption', String, nullable=True),
    Column('like_count', Integer, nullable=True),
    Column('comment_count', Integer, nullable=True)
)

comment = Table('comment', metadata,
    Column('comment_id', Integer, primary_key=True),
    Column('post_id', Integer, ForeignKey('post.post_id'), nullable=True),
    Column('user_id', Integer, ForeignKey('users.user_id'), nullable=True),
    Column('text', Text, nullable=True),
    Column('timestamp', DateTime, nullable=True)
)

likes = Table('likes', metadata,
    Column('like_id', Integer, primary_key=True),
    Column('post_id', Integer, ForeignKey('post.post_id'), nullable=True),
    Column('user_id', Integer, ForeignKey('users.user_id'), nullable=True),
    Column('timestamp', DateTime, nullable=True)
)

tags = Table('tags', metadata,
    Column('tag_id', Integer, primary_key=True),
    Column('name', String, nullable=True)
)

# Experiment Setting

collection = Table('collection', metadata,
    Column('collection_id', Integer, primary_key=True),
    Column('harvesting_tech', Integer, nullable=True),
    Column('time_window', String, nullable=True),
    Column('geo_window', String, nullable=True),
    Column('timestamp', DateTime, nullable=True)
)

business_rule = Table('business_rule', metadata,
    Column('business_id', Integer, primary_key=True),
    Column('business_rule', String, nullable=True)
)

experiment_tag = Table('experiment_tag', metadata,
    Column('tag_id', Integer, primary_key=True),
    Column('tag_name', String, nullable=True),
    Column('content', String, nullable=True)
)

# Research Team

experiment = Table('experiment', metadata,
    Column('experiment_id', Integer, primary_key=True),
    Column('team_id', Integer, ForeignKey('teams.team_id'), nullable=True),
    Column('topic', String, nullable=True),
    Column('period', DateTime, nullable=True)
)

teams = Table('teams', metadata,
    Column('team_id', Integer, primary_key=True),
    Column('name', String, nullable=True)
)

scientist = Table('scientist', metadata,
    Column('scientist_id', Integer, primary_key=True),
    Column('name', String, nullable=True),
    Column('team_id', Integer, ForeignKey('teams.team_id'), nullable=True)
)

research_question = Table('research_question', metadata,
    Column('rq_id', Integer, primary_key=True),
    Column('research_question', String, nullable=True)
)

# Create tables if they don't exist
metadata.create_all(engine)

# Create a Session
Session = sessionmaker(bind=engine)
session = Session()

@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        try:
            rq = request.form['rq']
            team_id = int(request.form['team_id'])
            topic = request.form['topic']
            period = datetime.strptime(request.form['period'], '%Y-%m-%d')

            while True:
                try:
                    # Retrieve the current maximum experiment_id
                    max_id = session.query(func.max(experiment.c.experiment_id)).scalar()
                    new_id = max_id + 1 if max_id is not None else 1

                    new_experiment = experiment.insert().values(experiment_id=new_id, team_id=team_id, topic=topic, period=period)
                    session.execute(new_experiment)
                    session.commit()
                    logger.info("New experiment added successfully.")
                    return render_template('success.html')
                except exc.IntegrityError:
                    # If there's a unique constraint violation, retry with a new ID
                    session.rollback()
                    continue
        except Exception as e:
            session.rollback()
            logger.error(f"Error adding experiment: {e}")
            return render_template('error.html', error_message=str(e))
    else:
        # Query the available teams
        team_list = session.query(teams).all()
        return render_template('add_experiment.html', teams=team_list)

@app.route('/add_team', methods=['GET', 'POST'])
def add_team():
    if request.method == 'POST':
        try:
            name = request.form['name']

            max_team_id = session.query(func.max(teams.c.team_id)).scalar()
            new_team_id = max_team_id + 1 if max_team_id is not None else 1

            new_team = teams.insert().values(team_id=new_team_id, name=name)
            session.execute(new_team)
            session.commit()
            logger.info("New team added successfully.")
            return redirect(url_for('index'))
        except Exception as e:
            session.rollback()
            logger.error(f"Error adding team: {e}")
            return render_template('error.html', error_message=str(e))
    return render_template('add_team.html')

@app.route('/add_csv', methods=['GET', 'POST'])
def add_csv():
    if request.method == 'POST':
        try:
            file = request.files.get('csv_file')
            if not file:
                raise ValueError("No file uploaded")

            # Read the CSV file
            try:
                df = pd.read_csv(file)
            except pd.errors.ParserError as e:
                logger.error(f"Error reading CSV file: {e}")
                return render_template('error.html', error_message="Error reading CSV file. Please check the file format.")

            # Process the rows
            for index, row in df.iterrows():
                try:
                    # Extract values from row
                    team_id = row.get('team_id', None)
                    topic = row.get('Ecological relation/Other strandings', 'Unknown Topic')
                    period = pd.to_datetime(row.get('Sightning date', None), format='%m/%d/%Y', errors='coerce')
                    geostamp = row.get('Geolocation', None)

                    # Use default or random values if necessary
                    if team_id is None:
                        # Randomly pick a team_id from the existing teams
                        team_ids = [t.team_id for t in session.query(teams).all()]
                        team_id = random.choice(team_ids) if team_ids else None
                    
                    # Handle missing or invalid period
                    if pd.isna(period):
                        period = datetime.now()

                    # Generate new IDs
                    max_exp_id = session.query(func.max(experiment.c.experiment_id)).scalar()
                    new_experiment_id = max_exp_id + 1 if max_exp_id is not None else 1

                    max_media_id = session.query(func.max(media.c.media_id)).scalar()
                    new_media_id = max_media_id + 1 if max_media_id is not None else 1

                    # Insert into experiment table
                    new_experiment = {
                        'experiment_id': new_experiment_id,
                        'team_id': team_id,
                        'topic': topic,
                        'period': period
                    }
                    session.execute(experiment.insert().values(new_experiment))

                    # Insert into content table
                    new_content = {
                        'content_id': new_experiment_id,
                        'text': topic,
                        'geostamp': geostamp,
                        'timestamp': period
                    }
                    session.execute(content.insert().values(new_content))

                    # Insert into post table
                    new_post = {
                        'post_id': new_experiment_id,
                        'user_id': None,  # Assuming no user_id data available
                        'content_id': new_experiment_id,
                        'timestamp': period,
                        'media_type': None,  # If determined from URLs
                        'media_url': None,   # If determined from URLs
                        'caption': topic,
                        'like_count': None,  # If like_count data available
                        'comment_count': None  # If comment_count data available
                    }
                    session.execute(post.insert().values(new_post))

                    # Insert into media table
                    media_urls = [row.get(f'URL {i}', None) for i in range(1, 6)]
                    for url in media_urls:
                        if pd.notna(url):
                            new_media = {
                                'media_id': new_media_id,
                                'post_id': new_experiment_id,
                                'url': url,
                                'type': 'unknown',  # Adjust based on URL or additional info if available
                                'timestamp': period
                            }
                            session.execute(media.insert().values(new_media))
                            new_media_id += 1  # Increment media_id for next record

                    # Insert into collection table
                    new_collection = {
                        'collection_id': new_experiment_id,
                        'harvesting_tech': None,  # No corresponding data
                        'time_window': None,  # No corresponding data
                        'geo_window': geostamp,
                        'timestamp': period
                    }
                    session.execute(collection.insert().values(new_collection))

                    # Insert into experiment_tag table
                    new_experiment_tag = {
                        'tag_id': new_experiment_id,
                        'tag_name': None,  # No tag data
                        'content': topic
                    }
                    session.execute(experiment_tag.insert().values(new_experiment_tag))

                    # Commit all inserts
                    session.commit()
                except Exception as row_error:
                    logger.error(f"Error adding row {index}: {row_error}")
                    session.rollback()
                    return render_template('error.html', error_message=str(row_error))

            logger.info("New CSV added successfully.")
            return render_template('success.html')
        except Exception as e:
            session.rollback()
            logger.error(f"Error processing CSV: {e}")
            return render_template('error.html', error_message=str(e))
    return render_template('add_csv.html')

@app.route('/tables', methods=['GET'])
def get_tables():
    tables = metadata.tables.keys()
    return jsonify({"tables": list(tables)})

@app.route('/query', methods=['GET', 'POST'])
def query():
    tables = list(metadata.tables.keys())
    logger.info(f"Tables: {list(tables)}")
    table_data = []
    columns = []

    if request.method == 'POST':
        table_name = request.form.get('table')
        if table_name and table_name in metadata.tables:
            table = metadata.tables[table_name]
            result = session.execute(table.select()).fetchall()
            table_data = [dict(row) for row in result]
            columns = table.columns.keys()
    
    return render_template('query.html', tables=tables, table_data=table_data, columns=columns)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000, debug=True)
