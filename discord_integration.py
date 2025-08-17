"""
Discord Bot Integration für Football Data Pipeline
Umfassende Features für Notifications, Commands und Interaktionen
"""

try:
    import discord
    from discord.ext import commands, tasks
    DISCORD_AVAILABLE = True
except ImportError:
    # Discord bot features not available, but webhook still works
    DISCORD_AVAILABLE = False
    discord = None
    commands = None
    tasks = None

from datetime import datetime, timedelta
from typing import Dict, List
import sqlite3
try:
    import matplotlib.pyplot as plt
    import seaborn as sns
    PLOTTING_AVAILABLE = True
except ImportError:
    PLOTTING_AVAILABLE = False
import io
import os
import requests

if DISCORD_AVAILABLE:
    class FootballDiscordBot(commands.Bot):
        def __init__(self, database_path: str):
            intents = discord.Intents.default()
            intents.message_content = True
            super().__init__(command_prefix='!fb ', intents=intents)
            self.database_path = database_path
            self.notification_channels = {}
            self.user_subscriptions = {}

        @tasks.loop(minutes=30)
        async def check_upcoming_games(self):
            """Notify about games starting in next 2 hours"""
            try:
                conn = sqlite3.connect(self.database_path)
                conn.row_factory = sqlite3.Row
                upcoming = conn.execute("""
                    SELECT f.*, ht.name as home_team, at.name as away_team, l.name as league
                    FROM fixtures f
                    JOIN teams ht ON f.home_team_id = ht.id
                    JOIN teams at ON f.away_team_id = at.id
                    JOIN leagues l ON f.league_id = l.id
                    WHERE f.kickoff_utc BETWEEN datetime('now', '+90 minutes') 
                                            AND datetime('now', '+120 minutes')
                    AND f.kickoff_utc >= datetime('now')
                    ORDER BY f.kickoff_utc ASC
                """).fetchall()
                for game in upcoming:
                    await self.send_game_preview(game)
            except Exception as e:
                print(f"Error in check_upcoming_games: {e}")

        async def on_ready(self):
            print(f'🤖 {self.user} is connected to Discord!')
            await self.setup_scheduled_tasks()

        async def setup_scheduled_tasks(self):
            """Start background tasks"""
            self.check_upcoming_games.start()
            # self.odds_movement_alerts.start()
            # self.injury_notifications.start()

        async def send_injury_alert(self, event):
            """Alert on new injuries/suspensions"""
            severity_colors = {
                'minor': 0xffff00,
                'major': 0xff9900, 
                'season_ending': 0xff0000
            }
            embed = discord.Embed(
                title=f"🚑 {event['event_type'].title()} Alert",
                description=f"**{event['team_name']}**",
                color=severity_colors.get(event['severity'], 0x808080)
            )
            if event['player_name']:
                embed.add_field(name="👤 Player", value=event['player_name'], inline=True)
            embed.add_field(name="📝 Details", value=event['event_description'] or 'No details available', inline=False)
            embed.add_field(name="⚠️ Severity", value=event['severity'] or 'Unknown', inline=True)
            if event['end_date']:
                embed.add_field(name="📅 Expected Return", value=event['end_date'], inline=True)
            for guild_id, channels in self.notification_channels.items():
                channel_id = channels.get("injuries")
                if channel_id:
                    try:
                        channel = self.get_channel(channel_id)
                        if channel:
                            await channel.send(embed=embed)
                    except Exception as e:
                        print(f"Error sending injury alert: {e}")

        @commands.command(name='games')
        async def upcoming_games_command(self, ctx, hours: int = 24):
            """Show games in next X hours"""
            try:
                conn = sqlite3.connect(self.database_path)
                conn.row_factory = sqlite3.Row
                games = conn.execute("""
                    SELECT f.*, ht.name as home_team, at.name as away_team, l.name as league
                    FROM fixtures f
                    JOIN teams ht ON f.home_team_id = ht.id
                    JOIN teams at ON f.away_team_id = at.id
                    JOIN leagues l ON f.league_id = l.id  
                    WHERE f.kickoff_utc BETWEEN datetime('now') AND datetime('now', '+{} hours')
                    ORDER BY f.kickoff_utc
                    LIMIT 10
                """.format(hours)).fetchall()
                if not games:
                    await ctx.send(f"No games found in the next {hours} hours.")
                    return
                embed = discord.Embed(
                    title=f"⚽ Upcoming Games ({hours}h)",
                    color=0x0099ff
                )
                for game in games:
                    kickoff_timestamp = int(datetime.fromisoformat(game['kickoff_utc'].replace(' ', 'T')).timestamp())
                    embed.add_field(
                        name=f"{game['league']}", 
                        value=f"**{game['home_team']}** vs **{game['away_team']}**\n"
                                f"<t:{kickoff_timestamp}:R>",
                        inline=False
                    )
                await ctx.send(embed=embed)
            except Exception as e:
                await ctx.send(f"Error fetching games: {e}")

        @commands.command(name='odds')
        async def odds_command(self, ctx, *, team_name: str):
                    """Get latest odds for team's next game"""
                    try:
                        conn = sqlite3.connect(self.database_path)
                        conn.row_factory = sqlite3.Row
                        game = conn.execute("""
                            SELECT f.*, ht.name as home_team, at.name as away_team, l.name as league
                            FROM fixtures f
                            JOIN teams ht ON f.home_team_id = ht.id
                            JOIN teams at ON f.away_team_id = at.id
                            JOIN leagues l ON f.league_id = l.id
                            WHERE (ht.name LIKE ? OR at.name LIKE ?) 
                            AND f.kickoff_utc > datetime('now')
                            ORDER BY f.kickoff_utc
                            LIMIT 1
                        """, (f'%{team_name}%', f'%{team_name}%')).fetchone()
                        if not game:
                            await ctx.send(f"No upcoming games found for '{team_name}'")
                            return
                        odds = conn.execute("""
                            SELECT * FROM odds_history 
                            WHERE fixture_id = ? AND market_type = 'h2h'
                            ORDER BY collected_at DESC
                            LIMIT 3
                        """, (game['id'],)).fetchall()
                        embed = discord.Embed(
                            title=f"🎲 Odds: {game['home_team']} vs {game['away_team']}",
                            description=f"**{game['league']}**",
                            color=0x00ff00
                        )
                        kickoff_timestamp = int(datetime.fromisoformat(game['kickoff_utc'].replace(' ', 'T')).timestamp())
                        embed.add_field(name="⏰ Kickoff", value=f"<t:{kickoff_timestamp}:F>", inline=False)
                        for odd in odds:
                            embed.add_field(
                                name=f"📊 {odd['bookmaker']} ({odd['collection_phase']})",
                                value=f"**{game['home_team']}**: {odd['home_odds']}\n"
                                     f"**{game['away_team']}**: {odd['away_odds']}\n"
                                     f"Draw: {odd['draw_odds']}\n"
                                     f"Market: {odd['market_type']}"
                            )
                        await ctx.send(embed=embed)
                    except Exception as e:
                        await ctx.send(f"Error fetching odds: {e}")
                    inline=True
            
                    await ctx.send(embed=embed)
    
        @commands.command(name='trends')
        async def odds_trends_command(self, ctx, *, team_name: str):
            """Generate odds trend chart for team's next game"""
            try:
                conn = sqlite3.connect(self.database_path)
                conn.row_factory = sqlite3.Row
                
                # Find team and game
                game = conn.execute("""
                    SELECT f.*, ht.name as home_team, at.name as away_team
                    FROM fixtures f
                    JOIN teams ht ON f.home_team_id = ht.id
                    JOIN teams at ON f.away_team_id = at.id
                    WHERE (ht.name LIKE ? OR at.name LIKE ?)
                    AND f.kickoff_utc > datetime('now')
                    ORDER BY f.kickoff_utc LIMIT 1
                """, (f'%{team_name}%', f'%{team_name}%')).fetchone()
                
                if not game:
                    await ctx.send(f"No upcoming games found for '{team_name}'")
                    return
                
                # Get odds history
                odds_history = conn.execute("""
                    SELECT collected_at, home_odds, away_odds, bookmaker
                    FROM odds_history 
                    WHERE fixture_id = ? AND market_type = 'h2h' AND home_odds IS NOT NULL
                    ORDER BY collected_at
                """, (game['id'],)).fetchall()
                
                if len(odds_history) < 2:
                    await ctx.send("Not enough odds data for trend analysis")
                    return
                
                # Create trend chart
                chart_buffer = await self.create_odds_chart(odds_history, game)
                
                file = discord.File(chart_buffer, filename='odds_trend.png')
                embed = discord.Embed(
                    title=f"📈 Odds Trends: {game['home_team']} vs {game['away_team']}",
                    color=0x9900ff
                )
                embed.set_image(url="attachment://odds_trend.png")
                
                await ctx.send(embed=embed, file=file)
                
            except Exception as e:
                await ctx.send(f"Error creating trends chart: {e}")
        
        @commands.command(name='form')
        async def team_form_command(self, ctx, *, team_name: str):
        """Show team's recent form and statistics"""
        try:
            conn = sqlite3.connect(self.database_path)
            conn.row_factory = sqlite3.Row
            
            # Find team
            team = conn.execute("""
                SELECT * FROM teams WHERE name LIKE ? LIMIT 1
            """, (f'%{team_name}%',)).fetchone()
            
            if not team:
                await ctx.send(f"Team '{team_name}' not found")
                return
            
            # Get latest stats
            stats = conn.execute("""
                SELECT * FROM team_statistics 
                WHERE team_id = ?
                ORDER BY collection_date DESC LIMIT 1
            """, (team['id'],)).fetchone()
            
            # Get recent fixtures
            recent_fixtures = conn.execute("""
                SELECT f.*, ht.name as home_team, at.name as away_team,
                       CASE 
                         WHEN f.home_team_id = ? THEN 'home'
                         ELSE 'away' 
                       END as venue
                FROM fixtures f
                JOIN teams ht ON f.home_team_id = ht.id
                JOIN teams at ON f.away_team_id = at.id
                WHERE (f.home_team_id = ? OR f.away_team_id = ?)
                AND f.kickoff_utc <= datetime('now')
                AND f.status != 'scheduled'
                ORDER BY f.kickoff_utc DESC LIMIT 5
            """, (team['id'], team['id'], team['id'])).fetchall()
            
            embed = discord.Embed(
                title=f"📊 {team['name']} - Team Form",
                color=0x00aaff
            )
            
            if stats:
                embed.add_field(
                    name="📈 Season Stats",
                    value=f"**Played**: {stats['matches_played']}\n"
                          f"**Win Rate**: {stats['win_percentage']:.1f}%\n"
                          f"**Goals**: {stats['goals_for']}-{stats['goals_against']}",
                    inline=True
                )
            
            if recent_fixtures:
                form_string = ""
                for fixture in recent_fixtures:
                    venue_icon = "🏠" if fixture['venue'] == 'home' else "✈️"
                    opponent = fixture['away_team'] if fixture['venue'] == 'home' else fixture['home_team']
                    form_string += f"{venue_icon} vs {opponent}\n"
                
                embed.add_field(name="🏃 Recent Games", value=form_string, inline=True)
            
            await ctx.send(embed=embed)
            
        except Exception as e:
            await ctx.send(f"Error fetching team form: {e}")

# =============================================================================
# ADMIN COMMANDS
# =============================================================================

    @commands.command(name='setup')
    @commands.has_permissions(administrator=True)
    async def setup_notifications(self, ctx, alert_type: str = None):
        """Setup notification channel for this server and alert type
        Usage: !fb setup <alert_type> (odds, injuries, previews)
        """
        valid_types = {"odds", "injuries", "previews"}
        if not alert_type or alert_type not in valid_types:
            await ctx.send(f"Please specify alert type: {'/'.join(valid_types)}\nExample: !fb setup odds")
            return
        if ctx.guild.id not in self.notification_channels:
            self.notification_channels[ctx.guild.id] = {}
        self.notification_channels[ctx.guild.id][alert_type] = ctx.channel.id
        await ctx.send(f"✅ {alert_type.capitalize()} notifications will be sent to {ctx.channel.mention}")
    
    @commands.command(name='subscribe')
    async def subscribe_user(self, ctx, *preferences):
        """Subscribe to specific notifications (e.g. injuries, odds, games)"""
        valid_prefs = {'injuries', 'odds', 'games', 'all'}
        user_prefs = set(preferences) if preferences else {'all'}
        
        if not user_prefs.issubset(valid_prefs):
            await ctx.send(f"Valid preferences: {', '.join(valid_prefs)}")
            return
        
        self.user_subscriptions[ctx.author.id] = list(user_prefs)
        await ctx.send(f"✅ Subscribed to: {', '.join(user_prefs)}")

# =============================================================================
# UTILITY METHODS
# =============================================================================

    async def create_league_bar_chart(self, teams, league_name):
            import matplotlib.pyplot as plt
            import io
            names = [team['name'] for team in teams]
            win_rates = [team['win_percentage'] for team in teams]
            plt.figure(figsize=(8, 4))
            bars = plt.barh(names, win_rates, color='skyblue')
            plt.xlabel('Win Rate (%)')
            plt.title(f'Top Teams in {league_name}')
            plt.xlim(0, 100)
            for bar, rate in zip(bars, win_rates):
                plt.text(bar.get_width()+1, bar.get_y()+bar.get_height()/2, f'{rate:.1f}%', va='center')
            plt.tight_layout()
            buf = io.BytesIO()
            plt.savefig(buf, format='png')
            buf.seek(0)
            plt.close()
            return buf
    
    async def create_odds_chart(self, odds_history, game):
        """Create odds trend visualization"""
        plt.figure(figsize=(10, 6))
        
        timestamps = [datetime.fromisoformat(row['collected_at'].replace(' ', 'T')) for row in odds_history]
        home_odds = [float(row['home_odds']) if row['home_odds'] else None for row in odds_history]
        away_odds = [float(row['away_odds']) if row['away_odds'] else None for row in odds_history]
        
        plt.plot(timestamps, home_odds, label=f"{game['home_team']} (Home)", marker='o', linewidth=2)
        plt.plot(timestamps, away_odds, label=f"{game['away_team']} (Away)", marker='s', linewidth=2)
        
        plt.title(f"Odds Movement: {game['home_team']} vs {game['away_team']}", fontsize=14, fontweight='bold')
        plt.xlabel("Time")
        plt.ylabel("Odds")
        plt.legend()
        plt.grid(True, alpha=0.3)
        plt.xticks(rotation=45)
        plt.tight_layout()
        
        # Save to buffer
        buffer = io.BytesIO()
        plt.savefig(buffer, format='png', dpi=150, bbox_inches='tight')
        buffer.seek(0)
        plt.close()
        
        return buffer

# =============================================================================
# WEBHOOK INTEGRATION (für GitHub Actions)
# =============================================================================

def send_webhook_notification(webhook_url: str, title: str, description: str, color: int = 0x0099ff):
    """Send notification via Discord Webhook (für GitHub Actions)"""
    embed = {
        "title": title,
        "description": description,
        "color": color,
        "timestamp": datetime.now().isoformat(),
        "footer": {"text": "Football Data Pipeline"}
    }
    
    payload = {"embeds": [embed]}
    
    try:
        response = requests.post(webhook_url, json=payload)
        response.raise_for_status()
        print(f"✅ Discord notification sent: {title}")
    except Exception as e:
        print(f"❌ Discord webhook failed: {e}")

# Für GitHub Actions Integration
def github_actions_discord_notifications():
    """
    Add this to your workflow after data collection:
    """
    example_webhook_usage = '''
    # In deinem GitHub Actions workflow, nach der Datensammlung:
    
    - name: Discord Notification
      if: success()
      run: |
        python << EOF
        import os
        from discord_bot_features import send_webhook_notification
        
        webhook_url = os.getenv('DISCORD_WEBHOOK_URL')
        if webhook_url:
            send_webhook_notification(
                webhook_url,
                "🎯 Data Collection Complete", 
                f"Collected {collection_type} for {home_team} vs {away_team}\\n"
                f"Next game: <t:{kickoff_timestamp}:R>",
                0x00ff00
            )
        EOF
      env:
        DISCORD_WEBHOOK_URL: ${{ secrets.DISCORD_WEBHOOK_URL }}
    '''
    return example_webhook_usage

if __name__ == "__main__":
    # Bot setup example
    TOKEN = os.getenv('DISCORD_BOT_TOKEN')
    DATABASE_PATH = 'data/football_data.db'
    
    if TOKEN:
        bot = FootballDiscordBot(DATABASE_PATH)
        print("Discord bot features ready!")
        print("Set DISCORD_BOT_TOKEN environment variable to run bot")
        # bot.run(TOKEN)
    else:
        print("Discord integration examples generated!")
        print("Features include:")
        print("• Automated game previews")
        print("• Odds movement alerts") 
        print("• Injury/suspension notifications")
        print("• Interactive commands (!fb games, !fb odds, !fb trends)")
        print("• Trend visualizations")
        print("• Team form analysis")
        print("• GitHub Actions webhook integration")
        
        # Example of how to initialize the bot
        if __name__ == "__main__":
            # Bot setup example
            TOKEN = os.getenv('DISCORD_BOT_TOKEN')
            DATABASE_PATH = 'data/football_data.db'
            
            if TOKEN:
                bot = FootballDiscordBot(DATABASE_PATH)
                print("🤖 Starting Discord bot...")
                bot.run(TOKEN)
            else:
                print("⚠️ DISCORD_BOT_TOKEN not set - webhook only mode")
                print("Set DISCORD_WEBHOOK_URL for webhook notifications")
    
    @tasks.loop(minutes=15)
    async def odds_movement_alerts(self):
        """Alert on significant odds movements (>10% change)"""
        try:
            conn = sqlite3.connect(self.database_path)
            conn.row_factory = sqlite3.Row
            
            # Find recent significant odds changes
            movements = conn.execute("""
                WITH recent_odds AS (
                    SELECT *, 
                           LAG(home_odds) OVER (PARTITION BY fixture_id, market_type, bookmaker 
                                               ORDER BY collected_at) as prev_home_odds,
                           LAG(away_odds) OVER (PARTITION BY fixture_id, market_type, bookmaker 
                                               ORDER BY collected_at) as prev_away_odds
                    FROM odds_history 
                    WHERE collected_at >= datetime('now', '-1 hour')
                )
                SELECT r.*, f.kickoff_utc, ht.name as home_team, at.name as away_team
                FROM recent_odds r
                JOIN fixtures f ON r.fixture_id = f.id
                JOIN teams ht ON f.home_team_id = ht.id  
                JOIN teams at ON f.away_team_id = at.id
                WHERE prev_home_odds IS NOT NULL
                AND (ABS(home_odds - prev_home_odds) / prev_home_odds > 0.1
                     OR ABS(away_odds - prev_away_odds) / prev_away_odds > 0.1)
                AND f.kickoff_utc > datetime('now')
                ORDER BY r.collected_at DESC
                LIMIT 10
            """).fetchall()
            
            conn.close()
            return movements
            
        except Exception as e:
            print(f"Error getting odds movements: {e}")
            return []