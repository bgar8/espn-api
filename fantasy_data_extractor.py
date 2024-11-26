import os
from espn_api.football import League
from datetime import datetime
import sys
from google.oauth2 import service_account
from googleapiclient.discovery import build

# Get credentials from environment variables
ESPN_S2 = os.getenv('ESPN_S2')
SWID = os.getenv('SWID')

SCOPES = [
    'https://www.googleapis.com/auth/drive.file',
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive.metadata.readonly',
    'https://www.googleapis.com/auth/drive'
]

class FantasyDataExtractor:
    def __init__(self, league_id, start_year, end_year=None, espn_s2=None, swid=None, folder_id=None):
        self.league_id = league_id
        self.start_year = start_year
        self.end_year = end_year or datetime.now().year
        self.espn_s2 = espn_s2 or ESPN_S2  # Use environment variable if not provided
        self.swid = swid or SWID  # Use environment variable if not provided
        self.folder_id = folder_id or os.getenv('GOOGLE_FOLDER_ID')  # Get folder ID from environment
        self.google_creds = self.get_google_creds()

    def get_google_creds(self):
        """Get credentials from service account"""
        try:
            # Print the current working directory to debug
            print(f"Current directory: {os.getcwd()}")
            print(f"Checking if file exists: {os.path.exists('stella_service_account.json')}")
            
            creds = service_account.Credentials.from_service_account_file(
                'stella_service_account.json',
                scopes=SCOPES
            )
            print(f"Successfully loaded credentials for: {creds.service_account_email}")
            return creds
        except Exception as e:
            print(f"Error loading service account credentials: {str(e)}")
            print(f"Full error details: {repr(e)}")  # More detailed error info
            sys.exit(1)

    def fetch_league_history(self):
        """Fetch historical league data"""
        all_data = {
            'teams': [],
            'matchups': []
        }
        
        current_year = datetime.now().year
        
        for year in range(self.start_year, self.end_year + 1):
            try:
                print(f"Fetching data for year {year}...")
                
                if year >= 2018:
                    if not (self.espn_s2 and self.swid):
                        print(f"Skipping {year} - ESPN credentials required for years 2018+")
                        continue
                    league = League(league_id=self.league_id, year=year, espn_s2=self.espn_s2, swid=self.swid)
                else:
                    league = League(league_id=self.league_id, year=year)
                
                for team in league.teams:
                    # Get owner information first
                    owner_name, owner_id = self.get_owner_name(team)
                    
                    # Debug standings information
                    print(f"\nDEBUG - Year {year}, Team: {team.team_name}")
                    print(f"Regular Standing: {team.standing}")
                    print(f"Final Standing: {getattr(team, 'final_standing', team.standing)}")
                    
                    # Calculate statistics first
                    games_played = team.wins + team.losses + getattr(team, 'ties', 0)
                    regular_season_games = min(13, games_played)
                    
                    # Base team data
                    team_data = {
                        'year': year,
                        'team_id': getattr(team, 'team_id', 0),
                        'team_name': team.team_name,
                        'owner_name': owner_name,
                        'owner_id': owner_id,
                        
                        # Record Stats
                        'wins': team.wins,
                        'losses': team.losses,
                        'ties': getattr(team, 'ties', 0),
                        'win_percentage': round((team.wins / games_played) * 100, 1) if games_played > 0 else 0,
                        
                        # Points Stats
                        'points_for': round(float(team.points_for), 1),
                        'points_against': round(float(team.points_against), 1),
                        'avg_points_per_game': round(float(team.points_for) / games_played, 2) if games_played > 0 else 0,
                        'avg_points_differential_per_game': round((team.points_for - team.points_against) / games_played, 2) if games_played > 0 else 0,
                        'regular_standing': team.standing,
                        
                        # Transaction Stats
                        'acquisitions': getattr(team, 'acquisitions', 0),
                        'acquisition_budget_spent': getattr(team, 'acquisition_budget_spent', 0),
                        'drops': getattr(team, 'drops', 0),
                        'trades': getattr(team, 'trades', 0),
                        
                        # Additional Stats
                        'playoff_pct': getattr(team, 'playoff_pct', 0)
                    }
                    
                    # Handle championship fields
                    if year == current_year:
                        team_data.update({
                            'final_standing': 0,
                            'made_playoffs': 0,
                            'championship_appearance': 0,
                            'champion': 0
                        })
                    else:
                        final_standing = getattr(team, 'final_standing', team.standing)
                        
                        # Debug championship logic
                        print(f"Setting championship fields:")
                        print(f"Final Standing: {final_standing}")
                        print(f"Made Playoffs: {1 if team.standing <= 6 else 0}")
                        print(f"Championship Appearance: {1 if final_standing <= 2 else 0}")
                        print(f"Champion: {1 if final_standing == 1 else 0}")
                        
                        team_data.update({
                            'final_standing': final_standing,
                            'made_playoffs': 1 if team.standing <= 6 else 0,
                            'championship_appearance': 1 if final_standing <= 2 else 0,
                            'champion': 1 if final_standing == 1 else 0
                        })
                    
                    all_data['teams'].append(team_data)
                
                # Only fetch matchup data for 2019 and later
                if year >= 2019:
                    current_week = getattr(league, 'current_week', 0) or 17
                    for week in range(1, current_week + 1):
                        try:
                            box_scores = league.box_scores(week)
                            for game in box_scores:
                                if game.home_team and game.away_team:
                                    matchup_data = {
                                        'year': year,
                                        'week': week,
                                        'home_team': game.home_team.team_name,
                                        'home_score': getattr(game, 'home_score', 0),
                                        'away_team': game.away_team.team_name,
                                        'away_score': getattr(game, 'away_score', 0)
                                    }
                                    all_data['matchups'].append(matchup_data)
                        except Exception as e:
                            print(f"Error getting matchup data for year {year} week {week}: {str(e)}")
                            continue
                
            except Exception as e:
                print(f"Error fetching data for year {year}: {str(e)}")
                continue
        
        return all_data

    def export_to_google_sheets(self, data):
        """Export data to Google Sheets with incremental updates"""
        try:
            if not self.folder_id:
                raise ValueError("Google Drive folder ID is required but not provided")
            
            print(f"Attempting to access folder with ID: {self.folder_id}")
            service = build('sheets', 'v4', credentials=self.google_creds)
            drive_service = build('drive', 'v3', credentials=self.google_creds)
            
            for key, values in data.items():
                if not values:
                    print(f"No data to export for {key}")
                    continue

                file_name = f'Fantasy_Football_{key}_{self.start_year}_{self.end_year}'
                
                # Prepare data for sheet
                if values:  # Check if we have data
                    headers = list(values[0].keys())
                    sheet_values = [headers]  # Initialize sheet_values with headers
                    for item in values:
                        sheet_values.append([str(item.get(header, '')) for header in headers])
                else:
                    print(f"No data to export for {key}")
                    continue

                # Create or update spreadsheet
                results = drive_service.files().list(
                    q=f"name='{file_name}' and '{self.folder_id}' in parents",
                    spaces='drive',
                    fields='files(id, name)'
                ).execute()

                if results.get('files'):
                    spreadsheet_id = results['files'][0]['id']
                    print(f"Updating existing spreadsheet: {file_name}")
                else:
                    spreadsheet = {
                        'properties': {
                            'title': file_name
                        },
                        'sheets': [{
                            'properties': {
                                'title': 'master_data'
                            }
                        }]
                    }
                    spreadsheet = service.spreadsheets().create(body=spreadsheet).execute()
                    spreadsheet_id = spreadsheet['spreadsheetId']

                    # Move to specified folder
                    file = drive_service.files().get(fileId=spreadsheet_id,
                                                   fields='parents').execute()
                    previous_parents = ",".join(file.get('parents', []))
                    drive_service.files().update(fileId=spreadsheet_id,
                                               addParents=self.folder_id,
                                               removeParents=previous_parents,
                                               fields='id, parents').execute()
                    print(f"Created new spreadsheet: {file_name}")

                # Update sheet content - specifically targeting 'master_data' sheet
                service.spreadsheets().values().clear(
                    spreadsheetId=spreadsheet_id,
                    range='master_data!A1:Z'  # Changed to target master_data sheet
                ).execute()

                body = {
                    'values': sheet_values
                }

                service.spreadsheets().values().update(
                    spreadsheetId=spreadsheet_id,
                    range='master_data!A1',  # Changed to target master_data sheet
                    valueInputOption='RAW',
                    body=body
                ).execute()

                print(f"Successfully exported {len(sheet_values)-1} {key} records to Google Sheets")

        except Exception as e:
            print(f"Error exporting data to Google Sheets: {str(e)}")
            raise

    def test_folder_access(self):
        """Test if service account can access the folder"""
        try:
            drive_service = build('drive', 'v3', credentials=self.google_creds)
            
            print(f"\nTesting folder access:")
            print(f"Service Account Email: {self.google_creds.service_account_email}")
            print(f"Folder ID: {self.folder_id}")
            
            # First, verify the folder exists and is accessible
            try:
                folder = drive_service.files().get(
                    fileId=self.folder_id,
                    fields='id, name, mimeType'
                ).execute()
                print(f"Found folder: {folder.get('name')} ({folder.get('mimeType')})")
            except Exception as e:
                print(f"Could not directly access folder: {str(e)}")
                return False
            
            # Try to create a test file in the folder
            try:
                file_metadata = {
                    'name': 'test_access.txt',
                    'parents': [self.folder_id],
                    'mimeType': 'text/plain'
                }
                file = drive_service.files().create(
                    body=file_metadata,
                    fields='id, name'
                ).execute()
                print(f"Successfully created test file: {file.get('name')}")
                
                # Clean up test file
                drive_service.files().delete(fileId=file.get('id')).execute()
                print("Successfully cleaned up test file")
                
                return True
                
            except Exception as e:
                print(f"Could not create file in folder: {str(e)}")
                return False
                
        except Exception as e:
            print(f"Folder access test failed: {str(e)}")
            return False

    def get_owner_name(self, team):
        """Extract owner name and ID from team object"""
        try:
            team_year = getattr(team, 'year', 'Unknown')
            team_name = getattr(team, 'team_name', 'Unknown Team')
            
            # Check if team object is valid
            if not team:
                print(f"Warning - Year {team_year}: Invalid team object for {team_name}")
                return "Unknown", ""

            # Check for owners list
            if not hasattr(team, 'owners'):
                print(f"Warning - Year {team_year}: No owners attribute for team '{team_name}'")
                return "Unknown", ""
            
            if not team.owners:
                print(f"Warning - Year {team_year}: Empty owners list for team '{team_name}'")
                return "Unknown", ""
            
            if not isinstance(team.owners, list):
                print(f"Warning - Year {team_year}: Owners is not a list for team '{team_name}' (type: {type(team.owners)})")
                return "Unknown", ""
            
            if len(team.owners) == 0:
                print(f"Warning - Year {team_year}: No owners found for team '{team_name}'")
                return "Unknown", ""

            # Get first owner
            owner = team.owners[0]
            
            if not isinstance(owner, dict):
                print(f"Warning - Year {team_year}: Owner data is not a dictionary for team '{team_name}' (type: {type(owner)})")
                return "Unknown", ""

            # Extract owner information
            first_name = owner.get('firstName', '')
            last_name = owner.get('lastName', '')
            owner_id = owner.get('id', '').strip('{}')

            if not first_name or not last_name:
                print(f"Warning - Year {team_year}: Incomplete owner name for team '{team_name}' (first: '{first_name}', last: '{last_name}')")
                return "Unknown", ""

            # Successfully found owner information
            print(f"Success - Year {team_year}: Found owner {first_name} {last_name} for team '{team_name}'")
            return f"{first_name} {last_name}", owner_id

        except Exception as e:
            print(f"Error - Year {getattr(team, 'year', 'Unknown')}: Failed to get owner for team '{getattr(team, 'team_name', 'Unknown Team')}': {str(e)}")
            return "Unknown", ""

def main():
    try:
        LEAGUE_ID = 771239
        START_YEAR = 2012
        END_YEAR = datetime.now().year
        FOLDER_ID = "1HWoIwON6vuIAlXp_dI3GFzU-Hvm4a0LU"
        
        # ESPN credentials - hardcoded for testing
        ESPN_S2 = "AEB1%2B6x6E551uaX42CV3xt0GVZHGWsvfhbp1oy3tn6HJbJOJ0oVjfB35BbOP%2BUhI5rkVN75a4%2FJ0z%2F6VjJgDTYZH0%2FnUGpIBp5%2FGeEgyIgPYODdYnVrEXz43TIr3FqW7U02V54vlSV6DhjPtE9Ue6GRY6A1sTgatsmEpkxKhhncbkO1pj9OiUNq8%2BPya%2F%2BEXseR5PB7lvPapp7aWUqWsYEm9boLOGI6JK1oHswrZzW7iDJpc%2FMiQVSHaSwHJKZQGvyLYtWmNUy7Tidlt5F275%2BF3tXT6L9qrOLXVWimGfZXq9YinT%2BwjEtJUIK57rBRovYs%3D"
        SWID = "{ABB8EBF7-D356-4FC4-B8EB-F7D3561FC43B}"
        
        print(f"Starting data extraction for league {LEAGUE_ID} from year {START_YEAR} to {END_YEAR}")
        
        extractor = FantasyDataExtractor(
            league_id=LEAGUE_ID,
            start_year=START_YEAR,
            end_year=END_YEAR,
            espn_s2=ESPN_S2,  # Pass ESPN credentials directly
            swid=SWID,
            folder_id=FOLDER_ID
        )
        
        data = extractor.fetch_league_history()
        extractor.export_to_google_sheets(data)
        print("Data extraction and export completed successfully!")

    except Exception as e:
        print(f"Fatal error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 