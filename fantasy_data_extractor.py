from espn_api.football import League
from datetime import datetime
import os
import sys
import csv
from google.oauth2 import service_account
from googleapiclient.discovery import build

ESPN_S2 = "AEB1%2B6x6E551uaX42CV3xt0GVZHGWsvfhbp1oy3tn6HJbJOJ0oVjfB35BbOP%2BUhI5rkVN75a4%2FJ0z%2F6VjJgDTYZH0%2FnUGpIBp5%2FGeEgyIgPYODdYnVrEXz43TIr3FqW7U02V54vlSV6DhjPtE9Ue6GRY6A1sTgatsmEpkxKhhncbkO1pj9OiUNq8%2BPya%2F%2BEXseR5PB7lvPapp7aWUqWsYEm9boLOGI6JK1oHswrZzW7iDJpc%2FMiQVSHaSwHJKZQGvyLYtWmNUy7Tidlt5F275%2BF3tXT6L9qrOLXVWimGfZXq9YinT%2BwjEtJUIK57rBRovYs%3D"
SWID = "{ABB8EBF7-D356-4FC4-B8EB-F7D3561FC43B}"

SCOPES = ['https://www.googleapis.com/auth/drive.file',
          'https://www.googleapis.com/auth/spreadsheets']

class FantasyDataExtractor:
    def __init__(self, league_id, start_year, end_year=None, espn_s2=None, swid=None, folder_id=None):
        self.league_id = league_id
        self.start_year = start_year
        self.end_year = end_year or datetime.now().year
        self.output_dir = 'fantasy_data'
        self.espn_s2 = espn_s2
        self.swid = swid
        self.folder_id = folder_id
        self.google_creds = self.get_google_creds()
        
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

    def get_google_creds(self):
        """Get credentials from service account"""
        try:
            return service_account.Credentials.from_service_account_file(
                'stella_service_account.json',
                scopes=SCOPES
            )
        except Exception as e:
            print(f"Error loading service account credentials: {str(e)}")
            sys.exit(1)

    def create_or_update_sheet(self, data, sheet_name):
        """Create or update Google Sheet with data"""
        try:
            service = build('sheets', 'v4', credentials=self.google_creds)
            drive_service = build('drive', 'v3', credentials=self.google_creds)

            # Create new spreadsheet
            spreadsheet = {
                'properties': {
                    'title': f'Fantasy_Football_Data_{sheet_name}_{self.start_year}_{self.end_year}'
                }
            }
            
            spreadsheet = service.spreadsheets().create(body=spreadsheet).execute()
            spreadsheet_id = spreadsheet['spreadsheetId']

            # Move file to specified folder if folder_id is provided
            if self.folder_id:
                file = drive_service.files().get(fileId=spreadsheet_id,
                                               fields='parents').execute()
                previous_parents = ",".join(file.get('parents', []))
                
                drive_service.files().update(fileId=spreadsheet_id,
                                           addParents=self.folder_id,
                                           removeParents=previous_parents,
                                           fields='id, parents').execute()

            # Prepare data for sheet
            if data:
                headers = list(data[0].keys())
                values = [headers]
                for item in data:
                    values.append([str(item.get(header, '')) for header in headers])

                body = {
                    'values': values
                }

                service.spreadsheets().values().update(
                    spreadsheetId=spreadsheet_id,
                    range='A1',
                    valueInputOption='RAW',
                    body=body
                ).execute()

            print(f"Successfully created/updated sheet: {spreadsheet['properties']['title']}")
            return spreadsheet_id

        except Exception as e:
            print(f"Error creating/updating Google Sheet: {str(e)}")
            return None

    def export_to_google_sheets(self, data):
        """Export data to Google Sheets"""
        try:
            for key, values in data.items():
                if values:
                    sheet_id = self.create_or_update_sheet(values, key)
                    if sheet_id:
                        print(f"Successfully exported {len(values)} records to Google Sheet")
                else:
                    print(f"No data to export for {key}")
        except Exception as e:
            print(f"Error exporting data to Google Sheets: {str(e)}")

    def get_team_owner(self, team):
        """Safely get team owner name with multiple fallback options"""
        try:
            # Check for owners list first
            if hasattr(team, 'owners') and team.owners:
                owner = team.owners[0]  # Get first owner
                # Combine first and last name
                if 'firstName' in owner and 'lastName' in owner:
                    return f"{owner['firstName']} {owner['lastName']}"
                elif 'displayName' in owner:
                    return owner['displayName']
            
            # Fallback to other methods if owners list doesn't exist
            if hasattr(team, 'owner'):
                return team.owner
            elif hasattr(team, 'manager'):
                return team.manager
            
            return 'Unknown Owner'
        except Exception as e:
            print(f"Error getting owner for team {team.team_name}: {str(e)}")
            return 'Unknown Owner'

    def get_owner_id(self, team):
        """Get the owner's unique ID"""
        try:
            if hasattr(team, 'owners') and team.owners:
                owner = team.owners[0]
                return owner.get('id', '').strip('{}')  # Remove curly braces from GUID
            return None
        except Exception as e:
            print(f"Error getting owner ID for team {team.team_name}: {str(e)}")
            return None

    def fetch_league_history(self):
        all_data = {'teams': [], 'matchups': [], 'rosters': []}
        
        current_year = datetime.now().year
        
        for year in range(self.start_year, min(self.end_year, current_year) + 1):
            try:
                print(f"Fetching data for {year}...")
                if year >= 2018:
                    if not (self.espn_s2 and self.swid):
                        print(f"Skipping {year} - ESPN authentication required for years 2018 and later")
                        continue
                    league = League(league_id=self.league_id, year=year, espn_s2=self.espn_s2, swid=self.swid)
                else:
                    league = League(league_id=self.league_id, year=year)
                
                # Get team data
                for team in league.teams:
                    try:
                        team_data = {
                            'year': year,
                            'team_id': team.team_id,
                            'team_name': team.team_name,
                            'owner_name': self.get_team_owner(team),
                            'owner_id': self.get_owner_id(team),
                            'display_name': team.owners[0].get('displayName', '') if hasattr(team, 'owners') and team.owners else '',
                            'wins': getattr(team, 'wins', 0),
                            'losses': getattr(team, 'losses', 0),
                            'points_for': getattr(team, 'points_for', 0),
                            'points_against': getattr(team, 'points_against', 0),
                            'final_standing': getattr(team, 'final_standing', 0),
                            
                            'acquisitions': getattr(team, 'acquisitions', 0),
                            'acquisition_budget_spent': getattr(team, 'acquisition_budget_spent', 0),
                            'drops': getattr(team, 'drops', 0),
                            'trades': getattr(team, 'trades', 0),
                            'playoff_pct': getattr(team, 'playoff_pct', 0),
                            
                            'defensive_sacks': team.stats.get('defensiveSacks', 0),
                            'defensive_interceptions': team.stats.get('defensiveInterceptions', 0),
                            'defensive_fumbles': team.stats.get('defensiveFumbles', 0),
                            'defensive_safeties': team.stats.get('defensiveSafeties', 0),
                            'defensive_blocked_kicks': team.stats.get('defensiveBlockedKicks', 0),
                            'defensive_return_tds': (
                                team.stats.get('kickoffReturnTouchdowns', 0) +
                                team.stats.get('puntReturnTouchdowns', 0) +
                                team.stats.get('interceptionReturnTouchdowns', 0) +
                                team.stats.get('fumbleReturnTouchdowns', 0)
                            ),
                            
                            'passing_touchdowns': team.stats.get('passingTouchdowns', 0),
                            'passing_interceptions': team.stats.get('passingInterceptions', 0),
                            'passing_2pt': team.stats.get('passing2PtConversions', 0),
                            'rushing_yards': team.stats.get('rushingYards', 0),
                            'rushing_touchdowns': team.stats.get('rushingTouchdowns', 0),
                            'rushing_2pt': team.stats.get('rushing2PtConversions', 0),
                            'receiving_yards': team.stats.get('receivingYards', 0),
                            'receiving_touchdowns': team.stats.get('receivingTouchdowns', 0),
                            'receiving_2pt': team.stats.get('receiving2PtConversions', 0),
                            'receiving_receptions': team.stats.get('receivingReceptions', 0),
                            
                            'fg_made_under40': team.stats.get('madeFieldGoalsFromUnder40', 0),
                            'fg_made_40to49': team.stats.get('madeFieldGoalsFrom40To49', 0),
                            'fg_made_50plus': team.stats.get('madeFieldGoalsFrom50Plus', 0),
                            'fg_missed': team.stats.get('missedFieldGoals', 0),
                            'extra_points_made': team.stats.get('madeExtraPoints', 0),
                            
                            # Calculate total touchdowns
                            'total_touchdowns': (
                                team.stats.get('passingTouchdowns', 0) +
                                team.stats.get('rushingTouchdowns', 0) +
                                team.stats.get('receivingTouchdowns', 0) +
                                (  # defensive/return touchdowns
                                    team.stats.get('kickoffReturnTouchdowns', 0) +
                                    team.stats.get('puntReturnTouchdowns', 0) +
                                    team.stats.get('interceptionReturnTouchdowns', 0) +
                                    team.stats.get('fumbleReturnTouchdowns', 0)
                                )
                            ),
                            
                            # Calculate total yards
                            'total_yards': (
                                team.stats.get('rushingYards', 0) +
                                team.stats.get('receivingYards', 0)
                            ),
                            
                            # Calculate points per game
                            'points_per_game': round(
                                getattr(team, 'points_for', 0) / max(
                                    (getattr(team, 'wins', 0) + getattr(team, 'losses', 0)), 1
                                ), 2
                            ),
                            
                            # Calculate acquisition rate (per week)
                            'acquisition_rate': round(
                                getattr(team, 'acquisitions', 0) / max(
                                    (getattr(team, 'wins', 0) + getattr(team, 'losses', 0)), 1
                                ), 2
                            ),
                            
                            # Calculate trade rate (per season)
                            'trade_rate': getattr(team, 'trades', 0),
                            
                            # Additional calculated stats
                            'total_2pt_conversions': (
                                team.stats.get('passing2PtConversions', 0) +
                                team.stats.get('rushing2PtConversions', 0) +
                                team.stats.get('receiving2PtConversions', 0)
                            ),
                            
                            'total_field_goals': (
                                team.stats.get('madeFieldGoalsFromUnder40', 0) +
                                team.stats.get('madeFieldGoalsFrom40To49', 0) +
                                team.stats.get('madeFieldGoalsFrom50Plus', 0)
                            ),
                            
                            'field_goal_percentage': round(
                                (team.stats.get('madeFieldGoalsFromUnder40', 0) +
                                 team.stats.get('madeFieldGoalsFrom40To49', 0) +
                                 team.stats.get('madeFieldGoalsFrom50Plus', 0)) /
                                max((team.stats.get('missedFieldGoals', 0) +
                                     team.stats.get('madeFieldGoalsFromUnder40', 0) +
                                     team.stats.get('madeFieldGoalsFrom40To49', 0) +
                                     team.stats.get('madeFieldGoalsFrom50Plus', 0)), 1) * 100,
                                1
                            ),
                            
                            # Win percentage
                            'win_percentage': round(
                                getattr(team, 'wins', 0) /
                                max((getattr(team, 'wins', 0) + getattr(team, 'losses', 0)), 1) * 100,
                                1
                            ),
                            
                            # Points differential per game
                            'points_differential_per_game': round(
                                (getattr(team, 'points_for', 0) - getattr(team, 'points_against', 0)) /
                                max((getattr(team, 'wins', 0) + getattr(team, 'losses', 0)), 1),
                                2
                            )
                        }
                        all_data['teams'].append(team_data)
                        print(f"Successfully fetched data for team: {team_data['team_name']}")
                        print(f"Debug - Full team object for {team.team_name}:")
                        print(team.__dict__)
                    except Exception as e:
                        print(f"Error processing team in year {year}: {str(e)}")
                        continue

                # Get matchup data for 2019 and later
                if year >= 2019:
                    try:
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
                                        print(f"Successfully fetched matchup data for {year} week {week}")
                            except Exception as e:
                                print(f"Error fetching week {week} data for year {year}: {str(e)}")
                                continue
                    except Exception as e:
                        print(f"Error processing matchups for year {year}: {str(e)}")
                        continue
                        
            except Exception as e:
                print(f"Error fetching year {year}: {str(e)}")
                continue

        return all_data

def main():
    try:
        LEAGUE_ID = 771239
        START_YEAR = 2012
        END_YEAR = datetime.now().year
        FOLDER_ID = '1HWoIwON6vuIAlXp_dI3GFzU-Hvm4a0LU'  # Replace with your actual folder ID
        
        print(f"Starting data extraction for league {LEAGUE_ID} from year {START_YEAR} to {END_YEAR}")
        
        extractor = FantasyDataExtractor(
            league_id=LEAGUE_ID,
            start_year=START_YEAR,
            end_year=END_YEAR,
            espn_s2=ESPN_S2,
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
