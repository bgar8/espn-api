def update_fantasy_data(event, context):
    # Your existing code here
    extractor = FantasyDataExtractor(...)
    data = extractor.fetch_league_history()
    extractor.export_to_google_sheets(data) 