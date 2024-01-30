import pandas as pd
from pymongo import MongoClient
import numpy as np

# Function to insert data into MongoDB
def insert_into_mongodb(collection, data):
    # Update with your MongoDB connection details
    cluster_uri = "mongodb://localhost:27017"
    db_name = "ddsproject2"

    # Construct the connection string
    connection_string = f"{cluster_uri}/{db_name}"

    # Connect to MongoDB
    client = MongoClient(connection_string)

    # Specify the database
    db = client[db_name]

    # Insert data into the specified collection
    db[collection].insert_many(data)

# Read from CSV
country_data = pd.read_csv('countries.csv')
players_data = pd.read_csv('players.csv')
match_results_data = pd.read_csv('Match_results.csv')
player_assists_goals_data = pd.read_csv('Player_Assists_Goals.csv')
player_cards_data = pd.read_csv('Player_Cards.csv')
world_cup_history_data = pd.read_csv('Worldcup_History.csv')

countries_collection = {'countries': []}
stadiums_collection = {'stadiums': []}

for index, country_row in country_data.iterrows():
    # Initialize country document
    country_doc = {
        'cname': country_row['Country_Name'],
        'capital': country_row['Capital'],
        'population': float(country_row['Population']),
        'manager': country_row['Manager'],
        'players': [],
        'world_cup_history': []
    }

    # Filter players data for the current country
    players_data_for_country = players_data[players_data["Country"] == country_row['Country_Name']]
    print(players_data_for_country)
    
    # Process players data for the current country
    for _, player_row in players_data_for_country.iterrows():
        player_cards_info = player_cards_data[player_cards_data['Player_id'] == player_row['Player_id']]
        player_assists_goals_info = player_assists_goals_data[player_assists_goals_data['Player_id'] == player_row['Player_id']]

        player_info = {
                    'lname': player_row['Lname'],
                    'name': player_row['Name'],
                    'dob': player_row['DOB'],
                    'height': player_row['Height'],
                    'is_captain': player_row['Is_captain'],
                    'position': player_row['Position'],
                    'no_yellow_cards': int(player_cards_info['No_of_Yellow_cards'].values[0]) if not player_cards_info.empty else 0,
                    'no_red_cards': int(player_cards_info['No_of_Red_cards'].values[0]) if not player_cards_info.empty else 0,
                    'no_goals': int(player_assists_goals_info['Goals'].values[0]) if not player_assists_goals_info.empty else 0,
                    'no_assists': int(player_assists_goals_info['Assists'].values[0]) if not player_assists_goals_info.empty else 0,
                }
            
        country_doc['players'].append(player_info)

        # Process world cup history data for the current country
        world_cup_history_info = world_cup_history_data[world_cup_history_data['Winner'] == country_row['Country_Name']]
        #print(world_cup_history_info)
        for _, history_row in world_cup_history_info.iterrows():
            history_info = {
                'year': history_row['Year'],
                'host': history_row['Host']
            }
            #print(history_info)
            country_doc['world_cup_history'].append(history_info)

    countries_collection['countries'].append(country_doc)

    # Process stadium data for the current country
    for _, match_row in match_results_data[match_results_data['Team1'].eq(country_row['Country_Name'])].iterrows():
        stadium_doc = {
                'stadium': match_row['Stadium'],
                'city': match_row['Host_city'],
                'matches': [
                    {
                        'team1': match_row['Team1'],
                        'team2': match_row['Team2'],
                        'team1Score': match_row['Team1_score'],
                        'team2Score': match_row['Team2_score'],
                        'date': match_row['Date']
                    }
                ]
            }
        
        # Append the stadium document to the list in collection2_data
        stadiums_collection['stadiums'].append(stadium_doc)

insert_into_mongodb('countries', countries_collection['countries'])
insert_into_mongodb('stadiums', stadiums_collection['stadiums'])

