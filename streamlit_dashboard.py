"""
Football Data Pipeline - Streamlit Dashboard
Visualisierung für Teams, Spieler, Statistiken und Spiele
"""

import streamlit as st
import pandas as pd
import sqlite3
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
import numpy as np
from typing import Dict, List, Optional
import warnings
warnings.filterwarnings('ignore')

# Page config
st.set_page_config(
    page_title="⚽ Football Analytics Dashboard",
    page_icon="⚽",
    layout="wide",
    initial_sidebar_state="expanded"
)

class FootballDashboard:
    def __init__(self, database_path: str):
        self.database_path = database_path
        self.conn = None
        self.connect_db()
    
    def connect_db(self):
        """Connect to database"""
        try:
            self.conn = sqlite3.connect(self.database_path)
            self.conn.row_factory = sqlite3.Row
        except Exception as e:
            st.error(f"Database connection failed: {e}")
    
    def execute_query(self, query, params: tuple = None) -> pd.DataFrame:
        with sqlite3.connect(self.database_path) as conn:
            conn.row_factory = sqlite3.Row
            return pd.read_sql_query(query, conn, params=params)
    
    def get_leagues(self) -> pd.DataFrame:
        """Get available leagues"""
        return self.execute_query("""
            SELECT DISTINCT l.id, l.name, l.country, l.season,
                   COUNT(DISTINCT f.id) as total_games
            FROM leagues l
            LEFT JOIN fixtures f ON l.id = f.league_id
            GROUP BY l.id, l.name, l.country, l.season
            ORDER BY total_games DESC
        """)
    
    def get_teams(self, league_id: Optional[int] = None) -> pd.DataFrame:
        """Get teams, optionally filtered by league"""
        query = """
            SELECT DISTINCT t.*, l.name as league_name
            FROM teams t
            LEFT JOIN fixtures f ON (t.id = f.home_team_id OR t.id = f.away_team_id)
            LEFT JOIN leagues l ON f.league_id = l.id
        """
        params = None
        
        if league_id:
            query += " WHERE l.id = ?"
            params = (league_id,)
        
        query += " ORDER BY t.name"
        return self.execute_query(query, params)

# Initialize dashboard
@st.cache_resource
def init_dashboard():
    return FootballDashboard('data/football_data.db')

dashboard = init_dashboard()

# =============================================================================
# SIDEBAR - Navigation & Filters
# =============================================================================

st.sidebar.title("⚽ Football Analytics")
st.sidebar.markdown("---")

# Main navigation
page = st.sidebar.selectbox(
    "📊 Select View",
    ["🏠 Overview", "🎯 Live Games", "📈 Odds Analysis", "⚽ Team Analysis", 
     "👤 Player Stats", "🔍 Event Impact", "🎲 Betting Insights"]
)

st.sidebar.markdown("---")

# Global filters
leagues_df = dashboard.get_leagues()
selected_league = st.sidebar.selectbox(
    "🏆 Select League",
    options=[None] + leagues_df['name'].tolist(),
    format_func=lambda x: "All Leagues" if x is None else x
)

league_id = None
if selected_league:
    league_id = leagues_df[leagues_df['name'] == selected_league]['id'].iloc[0]

# Time range filter
time_range = st.sidebar.selectbox(
    "📅 Time Range",
    ["Last 7 days", "Last 30 days", "Current season", "All time"]
)

st.sidebar.markdown("---")
st.sidebar.info("💡 Tip: Select different views from the dropdown above")

# =============================================================================
# MAIN DASHBOARD CONTENT
# =============================================================================

if page == "🏠 Overview":
    st.title("🏠 Football Analytics Overview")
    
    # Key metrics row
    col1, col2, col3, col4 = st.columns(4)
    
    # Total games
    total_games = dashboard.execute_query("SELECT COUNT(*) as count FROM fixtures").iloc[0]['count']
    col1.metric("🎯 Total Games", f"{total_games:,}")
    
    # Active leagues
    active_leagues = dashboard.execute_query("SELECT COUNT(DISTINCT league_id) as count FROM fixtures").iloc[0]['count']
    col2.metric("🏆 Active Leagues", f"{active_leagues}")
    
    # Total odds records
    total_odds = dashboard.execute_query("SELECT COUNT(*) as count FROM odds_history").iloc[0]['count']
    col3.metric("📊 Odds Records", f"{total_odds:,}")
    
    # Recent events
    recent_events = dashboard.execute_query("""
        SELECT COUNT(*) as count FROM team_events 
        WHERE detected_at >= datetime('now', '-7 days')
    """).iloc[0]['count']
    col4.metric("🚨 Recent Events", f"{recent_events}")
    
    st.markdown("---")
    
    # Charts row
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📈 Data Collection Trends")
        
        collection_trends = dashboard.execute_query("""
            SELECT DATE(collected_at) as date, 
                   collection_phase,
                   COUNT(*) as records
            FROM odds_history 
            WHERE collected_at >= datetime('now', '-30 days')
            GROUP BY DATE(collected_at), collection_phase
            ORDER BY date DESC
        """)
        
        if not collection_trends.empty:
            fig = px.bar(collection_trends, x='date', y='records', 
                        color='collection_phase',
                        title="Daily Data Collection by Phase")
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No recent data collection records found")
    
    with col2:
        st.subheader("🏆 League Activity")
        
        league_activity = dashboard.execute_query("""
            SELECT l.name, l.country, COUNT(f.id) as games
            FROM leagues l
            LEFT JOIN fixtures f ON l.id = f.league_id
            WHERE f.kickoff_utc >= datetime('now', '-30 days')
            GROUP BY l.id, l.name, l.country
            ORDER BY games DESC
            LIMIT 10
        """)
        
        if not league_activity.empty:
            fig = px.pie(league_activity, values='games', names='name',
                        title="Games by League (Last 30 Days)")
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No recent league activity found")

elif page == "🎯 Live Games":
    st.title("🎯 Live & Upcoming Games")
    
    # Time filter tabs
    tab1, tab2, tab3 = st.tabs(["⏰ Next 24h", "📅 This Week", "🔍 Custom Range"])
    
    with tab1:
        upcoming_games = dashboard.execute_query("""
            SELECT f.*, ht.name as home_team, at.name as away_team, l.name as league,
                   (julianday(f.kickoff_utc) - julianday('now')) * 24 as hours_until
            FROM fixtures f
            JOIN teams ht ON f.home_team_id = ht.id
            JOIN teams at ON f.away_team_id = at.id  
            JOIN leagues l ON f.league_id = l.id
            WHERE f.kickoff_utc BETWEEN datetime('now') AND datetime('now', '+24 hours')
            AND f.status = 'scheduled'
            ORDER BY f.kickoff_utc
        """)
        
        if not upcoming_games.empty:
            for _, game in upcoming_games.iterrows():
                with st.expander(f"🏆 {game['league']}: {game['home_team']} vs {game['away_team']}", expanded=True):
                    
                    col1, col2, col3 = st.columns([2, 1, 2])
                    
                    with col1:
                        st.markdown(f"**🏠 {game['home_team']}**")
                        # Get latest home team stats
                        home_stats = dashboard.execute_query("""
                            SELECT win_percentage, goals_for, goals_against, matches_played
                            FROM team_statistics 
                            WHERE team_id = ?
                            ORDER BY collection_date DESC LIMIT 1
                        """, (game['home_team_id'],))
                        
                        if not home_stats.empty:
                            stats = home_stats.iloc[0]
                            st.metric("Win Rate", f"{stats['win_percentage']:.1f}%")
                            st.metric("Goals", f"{stats['goals_for']}-{stats['goals_against']}")
                    
                    with col2:
                        st.markdown("**⚽ VS ⚽**")
                        kickoff_time = datetime.fromisoformat(game['kickoff_utc'].replace(' ', 'T'))
                        st.markdown(f"**⏰ {kickoff_time.strftime('%H:%M')}**")
                        st.markdown(f"📅 {kickoff_time.strftime('%d.%m.%Y')}")
                        
                        hours_until = game['hours_until']
                        if hours_until < 1:
                            st.markdown("🔥 **Starting soon!**")
                        else:
                            st.markdown(f"⏱️ In {hours_until:.1f}h")
                    
                    with col3:
                        st.markdown(f"**✈️ {game['away_team']}**")
                        # Get latest away team stats
                        away_stats = dashboard.execute_query("""
                            SELECT win_percentage, goals_for, goals_against, matches_played
                            FROM team_statistics 
                            WHERE team_id = ?
                            ORDER BY collection_date DESC LIMIT 1
                        """, (game['away_team_id'],))
                        
                        if not away_stats.empty:
                            stats = away_stats.iloc[0]
                            st.metric("Win Rate", f"{stats['win_percentage']:.1f}%")
                            st.metric("Goals", f"{stats['goals_for']}-{stats['goals_against']}")
                    
                    # Latest odds
                    latest_odds = dashboard.execute_query("""
                        SELECT home_odds, draw_odds, away_odds, bookmaker, collected_at
                        FROM odds_history 
                        WHERE fixture_id = ? AND market_type = 'h2h'
                        ORDER BY collected_at DESC LIMIT 1
                    """, (game['id'],))
                    
                    if not latest_odds.empty:
                        odds = latest_odds.iloc[0]
                        st.markdown("**🎲 Latest Odds**")
                        odds_col1, odds_col2, odds_col3, odds_col4 = st.columns(4)
                        odds_col1.metric("🏠 Home", f"{odds['home_odds']:.2f}")
                        odds_col2.metric("🤝 Draw", f"{odds['draw_odds']:.2f}")
                        odds_col3.metric("✈️ Away", f"{odds['away_odds']:.2f}")
                        odds_col4.markdown(f"*{odds['bookmaker']}*")
        else:
            st.info("No games in the next 24 hours")

elif page == "📈 Odds Analysis":
    st.title("📈 Odds Movement Analysis")
    
    # Team selector for odds analysis
    teams_df = dashboard.get_teams(league_id)
    selected_team = st.selectbox(
        "🏆 Select Team for Odds Analysis",
        options=teams_df['name'].tolist() if not teams_df.empty else []
    )
    
    if selected_team and not teams_df.empty:
        team_id = teams_df[teams_df['name'] == selected_team]['id'].iloc[0]
        
        # Get team's next game with odds data
        next_game = dashboard.execute_query("""
            SELECT f.*, ht.name as home_team, at.name as away_team, l.name as league
            FROM fixtures f
            JOIN teams ht ON f.home_team_id = ht.id
            JOIN teams at ON f.away_team_id = at.id
            JOIN leagues l ON f.league_id = l.id
            WHERE (f.home_team_id = ? OR f.away_team_id = ?)
            AND f.kickoff_utc > datetime('now')
            AND EXISTS (SELECT 1 FROM odds_history WHERE fixture_id = f.id)
            ORDER BY f.kickoff_utc LIMIT 1
        """, (team_id, team_id))
        
        if not next_game.empty:
            game = next_game.iloc[0]
            st.subheader(f"🎯 {game['home_team']} vs {game['away_team']}")
            st.caption(f"📅 {game['kickoff_utc']} • 🏆 {game['league']}")
            
            # Odds history
            odds_history = dashboard.execute_query("""
                SELECT collected_at, home_odds, draw_odds, away_odds, 
                       bookmaker, collection_phase
                FROM odds_history 
                WHERE fixture_id = ? AND market_type = 'h2h'
                ORDER BY collected_at
            """, (game['id'],))
            
            if not odds_history.empty:
                # Convert to datetime
                odds_history['collected_at'] = pd.to_datetime(odds_history['collected_at'])
                
                # Odds movement chart
                fig = go.Figure()
                
                # Group by bookmaker for cleaner visualization
                for bookmaker in odds_history['bookmaker'].unique():
                    bm_data = odds_history[odds_history['bookmaker'] == bookmaker]
                    
                    fig.add_trace(go.Scatter(
                        x=bm_data['collected_at'],
                        y=bm_data['home_odds'],
                        name=f"{game['home_team']} ({bookmaker})",
                        line=dict(width=2),
                        mode='lines+markers'
                    ))
                    
                    fig.add_trace(go.Scatter(
                        x=bm_data['collected_at'],
                        y=bm_data['away_odds'],
                        name=f"{game['away_team']} ({bookmaker})",
                        line=dict(width=2, dash='dash'),
                        mode='lines+markers'
                    ))
                
                fig.update_layout(
                    title="📊 Odds Movement Over Time",
                    xaxis_title="Time",
                    yaxis_title="Odds",
                    hovermode='x unified',
                    height=500
                )
                
                st.plotly_chart(fig, use_container_width=True)
                
                # Odds summary table
                st.subheader("📋 Current Odds Summary")
                latest_odds = odds_history.sort_values('collected_at').groupby('bookmaker').tail(1)
                
                summary_df = latest_odds[['bookmaker', 'home_odds', 'draw_odds', 'away_odds', 'collection_phase']].copy()
                summary_df.columns = ['Bookmaker', game['home_team'], 'Draw', game['away_team'], 'Data Phase']
                
                st.dataframe(summary_df, use_container_width=True)
                
                # Odds movement alerts
                st.subheader("🚨 Significant Movements")
                
                # Calculate percentage changes
                for bookmaker in odds_history['bookmaker'].unique():
                    bm_data = odds_history[odds_history['bookmaker'] == bookmaker].sort_values('collected_at')
                    if len(bm_data) >= 2:
                        first_home = bm_data.iloc[0]['home_odds']
                        last_home = bm_data.iloc[-1]['home_odds']
                        first_away = bm_data.iloc[0]['away_odds']
                        last_away = bm_data.iloc[-1]['away_odds']
                        
                        home_change = ((last_home - first_home) / first_home) * 100 if first_home else 0
                        away_change = ((last_away - first_away) / first_away) * 100 if first_away else 0
                        
                        if abs(home_change) > 5 or abs(away_change) > 5:
                            col1, col2 = st.columns(2)
                            with col1:
                                st.metric(
                                    f"{game['home_team']} ({bookmaker})",
                                    f"{last_home:.2f}",
                                    f"{home_change:+.1f}%"
                                )
                            with col2:
                                st.metric(
                                    f"{game['away_team']} ({bookmaker})",
                                    f"{last_away:.2f}", 
                                    f"{away_change:+.1f}%"
                                )
            else:
                st.info("No odds data available for this game")
        else:
            st.info("No upcoming games with odds data found for this team")

elif page == "⚽ Team Analysis":
    st.title("⚽ Team Performance Analysis")
    
    teams_df = dashboard.get_teams(league_id)
    if teams_df.empty:
        st.error("No teams found")
        st.stop()
    
    selected_team = st.selectbox(
        "🏆 Select Team",
        options=teams_df['name'].tolist()
    )
    
    if selected_team:
        team_id = teams_df[teams_df['name'] == selected_team]['id'].iloc[0]
        
        # Team overview
        col1, col2, col3 = st.columns(3)
        
        # Latest statistics
        latest_stats = dashboard.execute_query("""
            SELECT * FROM team_statistics 
            WHERE team_id = ?
            ORDER BY collection_date DESC LIMIT 1
        """, (team_id,))
        
        if not latest_stats.empty:
            stats = latest_stats.iloc[0]
            
            with col1:
                st.metric("🏆 Win Rate", f"{stats['win_percentage']:.1f}%")
                st.metric("⚽ Goals For", f"{stats['goals_for']}")
            
            with col2:
                st.metric("🎯 Matches Played", f"{stats['matches_played']}")
                st.metric("🥅 Goals Against", f"{stats['goals_against']}")
            
            with col3:
                if stats['goals_for'] > 0 and stats['goals_against'] > 0:
                    goal_ratio = stats['goals_for'] / stats['goals_against']
                    st.metric("📊 Goal Ratio", f"{goal_ratio:.2f}")
                st.metric("🎪 Goal Difference", f"{stats['goals_for'] - stats['goals_against']:+d}")
        
        # Performance over time
        st.subheader("📈 Performance Trends")
        
        performance_history = dashboard.execute_query("""
            SELECT collection_date, win_percentage, goals_for, goals_against, matches_played
            FROM team_statistics 
            WHERE team_id = ?
            ORDER BY collection_date
        """, (team_id,))
        
        if not performance_history.empty:
            fig = make_subplots(
                rows=2, cols=2,
                subplot_titles=('Win Percentage', 'Goals Scored', 'Goals Conceded', 'Matches Played'),
                specs=[[{"secondary_y": False}, {"secondary_y": False}],
                       [{"secondary_y": False}, {"secondary_y": False}]]
            )
            
            performance_history['collection_date'] = pd.to_datetime(performance_history['collection_date'])
            
            fig.add_trace(
                go.Scatter(x=performance_history['collection_date'], 
                          y=performance_history['win_percentage'],
                          name="Win %", line=dict(color='green')),
                row=1, col=1
            )
            
            fig.add_trace(
                go.Scatter(x=performance_history['collection_date'], 
                          y=performance_history['goals_for'],
                          name="Goals For", line=dict(color='blue')),
                row=1, col=2
            )
            
            fig.add_trace(
                go.Scatter(x=performance_history['collection_date'], 
                          y=performance_history['goals_against'],
                          name="Goals Against", line=dict(color='red')),
                row=2, col=1
            )
            
            fig.add_trace(
                go.Scatter(x=performance_history['collection_date'], 
                          y=performance_history['matches_played'],
                          name="Matches", line=dict(color='purple')),
                row=2, col=2
            )
            
            fig.update_layout(height=500, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)
        
        # Recent fixtures and upcoming games
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("📅 Recent Results")
            recent_fixtures = dashboard.execute_query("""
                SELECT f.*, ht.name as home_team, at.name as away_team,
                       CASE WHEN f.home_team_id = ? THEN 'home' ELSE 'away' END as venue
                FROM fixtures f
                JOIN teams ht ON f.home_team_id = ht.id
                JOIN teams at ON f.away_team_id = at.id
                WHERE (f.home_team_id = ? OR f.away_team_id = ?)
                AND f.kickoff_utc < datetime('now')
                AND f.home_score IS NOT NULL
                ORDER BY f.kickoff_utc DESC LIMIT 5
            """, (team_id, team_id, team_id))
            
            for _, fixture in recent_fixtures.iterrows():
                opponent = fixture['away_team'] if fixture['venue'] == 'home' else fixture['home_team']
                venue_icon = "🏠" if fixture['venue'] == 'home' else "✈️"
                
                if fixture['venue'] == 'home':
                    result = f"{fixture['home_score']}-{fixture['away_score']}"
                else:
                    result = f"{fixture['away_score']}-{fixture['home_score']}"
                
                st.text(f"{venue_icon} vs {opponent}: {result}")
        
        with col2:
            st.subheader("🔮 Upcoming Games")
            upcoming_fixtures = dashboard.execute_query("""
                SELECT f.*, ht.name as home_team, at.name as away_team,
                       CASE WHEN f.home_team_id = ? THEN 'home' ELSE 'away' END as venue
                FROM fixtures f
                JOIN teams ht ON f.home_team_id = ht.id
                JOIN teams at ON f.away_team_id = at.id
                WHERE (f.home_team_id = ? OR f.away_team_id = ?)
                AND f.kickoff_utc > datetime('now')
                ORDER BY f.kickoff_utc LIMIT 5
            """, (team_id, team_id, team_id))
            
            for _, fixture in upcoming_fixtures.iterrows():
                opponent = fixture['away_team'] if fixture['venue'] == 'home' else fixture['home_team']
                venue_icon = "🏠" if fixture['venue'] == 'home' else "✈️"
                kickoff = datetime.fromisoformat(fixture['kickoff_utc'].replace(' ', 'T'))
                
                st.text(f"{venue_icon} vs {opponent}")
                st.caption(f"📅 {kickoff.strftime('%d.%m.%Y %H:%M')}")

elif page == "🔍 Event Impact":
    st.title("🔍 Event Impact Analysis")
    st.markdown("Analyze how injuries, suspensions, and other events affect team performance and odds")
    
    # Event type filter
    event_types = ['injury', 'suspension', 'transfer', 'lineup_change']
    selected_event_type = st.selectbox("🎯 Event Type", event_types)
    
    # Recent events
    recent_events = dashboard.execute_query("""
        SELECT te.*, t.name as team_name, p.name as player_name
        FROM team_events te
        JOIN teams t ON te.team_id = t.id
        LEFT JOIN players p ON te.player_id = p.id
        WHERE te.event_type = ?
        AND te.detected_at >= datetime('now', '-30 days')
        ORDER BY te.detected_at DESC
        LIMIT 20
    """, (selected_event_type,))
    
    if not recent_events.empty:
        st.subheader(f"📊 Recent {selected_event_type.title()} Events")
        
        for _, event in recent_events.iterrows():
            with st.expander(f"{event['team_name']} - {event['player_name'] or 'Team Event'}", expanded=False):
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    st.text(f"📅 Date: {event['start_date']}")
                    st.text(f"⚠️ Severity: {event['severity'] or 'Unknown'}")
                
                with col2:
                    st.text(f"📝 Description:")
                    st.text(event['event_description'] or 'No details available')
                
                with col3:
                    if event['end_date']:
                        st.text(f"🔄 Expected Return: {event['end_date']}")
                
                # Find odds impact around this event
                event_date = event['start_date']
                odds_impact = dashboard.execute_query("""
                    SELECT oh.*, f.kickoff_utc, f.home_team_id, f.away_team_id,
                           ht.name as home_team, at.name as away_team
                    FROM odds_history oh
                    JOIN fixtures f ON oh.fixture_id = f.id
                    JOIN teams ht ON f.home_team_id = ht.id
                    JOIN teams at ON f.away_team_id = at.id
                    WHERE (f.home_team_id = ? OR f.away_team_id = ?)
                    AND date(f.kickoff_utc) BETWEEN date(?, '-3 days') AND date(?, '+7 days')
                    ORDER BY oh.collected_at
                """, (event['team_id'], event['team_id'], event_date, event_date))
                
                if not odds_impact.empty:
                    st.text("📈 Odds movement around this event:")
                    
                    # Simple before/after analysis
                    before_event = odds_impact[odds_impact['collected_at'] < event['detected_at']]
                    after_event = odds_impact[odds_impact['collected_at'] >= event['detected_at']]
                    
                    if not before_event.empty and not after_event.empty:
                        # Team's odds (home or away)
                        is_home_team = odds_impact.iloc[0]['home_team_id'] == event['team_id']
                        
                        if is_home_team:
                            before_odds = before_event['home_odds'].mean()
                            after_odds = after_event['home_odds'].mean()
                            team_position = "home"
                        else:
                            before_odds = before_event['away_odds'].mean()
                            after_odds = after_event['away_odds'].mean()
                            team_position = "away"
                        
                        if before_odds and after_odds:
                            change_pct = ((after_odds - before_odds) / before_odds) * 100
                            
                            impact_col1, impact_col2, impact_col3 = st.columns(3)
                            impact_col1.metric("Before Event", f"{before_odds:.2f}")
                            impact_col2.metric("After Event", f"{after_odds:.2f}")
                            impact_col3.metric("Change", f"{change_pct:+.1f}%")
                else:
                    st.text("No odds data available around this event")
    else:
        st.info(f"No recent {selected_event_type} events found")

# =============================================================================
# FOOTER & ADDITIONAL INFO
# =============================================================================

st.sidebar.markdown("---")
st.sidebar.markdown("""
### 📊 Dashboard Features:
- **Live Games**: Real-time game tracking
- **Odds Analysis**: Movement trends & alerts  
- **Team Analysis**: Performance metrics
- **Event Impact**: Injury/suspension effects
- **Betting Insights**: Value opportunities

### 🔄 Data Updates:
Dashboard automatically refreshes from the database.
Data is collected every 30 minutes via GitHub Actions.
""")

st.sidebar.success("✅ Dashboard Ready!")

# Add refresh button
if st.sidebar.button("🔄 Refresh Data"):
    st.cache_resource.clear()
    st.rerun()

# =============================================================================
# MAIN EXECUTION
# =============================================================================

if __name__ == "__main__":
    st.markdown("---")
    st.markdown("**⚽ Football Analytics Dashboard** | Powered by GitHub Actions + Streamlit")
    
    # Show last database update
    try:
        last_update = dashboard.execute_query("""
            SELECT MAX(collected_at) as last_update FROM odds_history
        """).iloc[0]['last_update']
        
        if last_update:
            st.caption(f"📅 Last data update: {last_update}")
    except:
        pass