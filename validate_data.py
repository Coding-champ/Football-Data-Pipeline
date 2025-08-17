# Erstelle validate_data.py:
import sqlite3
import os
from datetime import datetime

def validate_pipeline_data():
    db_path = 'data/football_data.db'
    
    if not os.path.exists(db_path):
        print("❌ Database nicht gefunden!")
        print("   Workflow muss erst laufen und Daten sammeln")
        return
    
    print("🔍 Football Pipeline Data Validation")
    print("=" * 50)
    
    conn = sqlite3.connect(db_path)
    
    # Fixtures prüfen
    fixtures = conn.execute("SELECT COUNT(*) FROM fixtures").fetchone()[0]
    print(f"⚽ Fixtures: {fixtures}")
    
    # Odds History prüfen  
    odds = conn.execute("SELECT COUNT(*) FROM odds_history").fetchone()[0]
    print(f"📊 Odds Records: {odds}")
    
    # Teams prüfen
    teams = conn.execute("SELECT COUNT(*) FROM teams").fetchone()[0] 
    print(f"🏆 Teams: {teams}")
    
    # Leagues prüfen
    leagues = conn.execute("SELECT COUNT(*) FROM leagues").fetchone()[0]
    print(f"🎯 Leagues: {leagues}")
    
    # Neueste Daten
    try:
        latest = conn.execute("""
            SELECT MAX(collected_at) FROM odds_history
        """).fetchone()[0]
        if latest:
            print(f"📅 Latest Data: {latest}")
        else:
            print("📅 Latest Data: Keine Daten gefunden")
    except:
        print("📅 Latest Data: Tabelle leer")
    
    # Top 5 upcoming games
    print("\n🎯 Next Games:")
    try:
        upcoming = conn.execute("""
            SELECT f.kickoff_utc, ht.name, at.name, l.name
            FROM fixtures f
            JOIN teams ht ON f.home_team_id = ht.id
            JOIN teams at ON f.away_team_id = at.id
            JOIN leagues l ON f.league_id = l.id  
            WHERE f.kickoff_utc > datetime('now')
            ORDER BY f.kickoff_utc
            LIMIT 5
        """).fetchall()
        
        for game in upcoming:
            print(f"   {game[0]} | {game[3]} | {game[1]} vs {game[2]}")
    except Exception as e:
        print(f"   ⚠️ Fehler beim Laden: {e}")
    
    conn.close()
    
    # Erwartete Werte
    print("\n📊 Expected Values nach 24h:")
    print("   ⚽ Fixtures: 10-30")
    print("   📊 Odds: 30-100")  
    print("   🏆 Teams: 20-60")
    print("   🎯 Leagues: 5-10")

if __name__ == "__main__":
    validate_pipeline_data()