import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import boto3
import uuid
import os
from decimal import Decimal

def lambda_handler(event, context):
    
    def get_playlist_name(playlist_id):
        playlist_info = sp.playlist(playlist_id)
        return playlist_info['name']
        
    # Obtén las credenciales de Spotify desde las variables de entorno
    client_id = os.environ['spotify_client_id'] 
    client_secret =  os.environ['spotify_client_secret'] 
    # Crea un cliente Spotify API
    client_credentials_manager = SpotifyClientCredentials(
        client_id=client_id,
        client_secret=client_secret,
    )
    sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
        
    # Configura la conexión con DynamoDB
    dynamodb = boto3.resource('dynamodb')
    table_name = 'spotify_data'
    table = dynamodb.Table(table_name)

    scan = table.scan(
        ProjectionExpression='#k',
        ExpressionAttributeNames={
            '#k': 'id'
        }
    )
    # Borrar datos antiguos
    with table.batch_writer() as batch:
        for each in scan['Items']:
            batch.delete_item(Key=each)
            
    playlist_ids = [
        '37i9dQZEVXbNFJfN1Vw8d9',  # España
        '37i9dQZEVXbIQnj7RRhdSX',  # Italia
        '37i9dQZEVXbJiZcmkrIHGU',  # Alemania
        '37i9dQZEVXbIPWwFssbupI',  # Francia
        '37i9dQZEVXbLnolsZ8PSNw',  # Reino Unido
        '37i9dQZEVXbLRQDuF5jeBp',  # Estados Unidos
        '37i9dQZEVXbMMy2roB9myp',  # Argentina
        '37i9dQZEVXbMXbN3EUUhlg',  # Brasil
        '37i9dQZEVXbO3qyFxbkOE1',  # México
        '37i9dQZEVXbKj23U1GF4IR'   # Canadá
    ]
    # Crear una lista para almacenar los datos de las canciones
    canciones_data = []
    
    for playlist_id in playlist_ids:
        # Obtiene las primeras 10 canciones del top 50 de cada país en cada iteración.
        results = sp.playlist_tracks(playlist_id, limit=10)
    
        for track in results['items']:
            track_name = track['track']['name']
            artist_name = track['track']['artists'][0]['name']
            track_id = track['track']['id']
            playlist = get_playlist_name(playlist_id)
            # Obtén el análisis de audio de la canción
            analysis = sp.audio_analysis(track_id)
            features = sp.audio_features([track_id])[0]
            unique_id = str(uuid.uuid4())
            tempo = Decimal(str(analysis['track']['tempo']))
            dance = Decimal(str(features['danceability']))
            acoustic = Decimal(str(features['acousticness']))
            energy = Decimal(str(features['energy']))
            valence = Decimal(str(features['valence']))
            
            cancion_data = {
                'id': unique_id,
                'cancion': track_name,
                'Artista': artist_name,
                'Playlist': playlist,
                'Tempo': tempo,
                'Danceability': dance,
                'Acousticness': acoustic,
                'Energy': energy,
                'Valence': valence
            }
            canciones_data.append(cancion_data)
    
        # Insertar los elementos en DynamoDB
        for cancion_data in canciones_data:
            table.put_item(Item=cancion_data)

    return {
        'statusCode': 200,
        'body': 'Canciones agregadas a DynamoDB exitosamente'
    }

