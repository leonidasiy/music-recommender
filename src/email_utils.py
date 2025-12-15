"""
Email utilities for sending HTML recommendation reports via Gmail SMTP.
"""

import smtplib
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
from typing import List

from recommender import Recommendation

logger = logging.getLogger(__name__)


def generate_html_email(recommendations: List[Recommendation], stats: dict) -> str:
    """
    Generate HTML email content with recommendations.
    
    Args:
        recommendations: List of Recommendation objects
        stats: Dictionary with statistics (total_files, tracks_parsed, etc.)
        
    Returns:
        HTML string
    """
    current_date = datetime.now().strftime("%B %Y")
    
    html = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;
            line-height: 1.6;
            color: #333;
            max-width: 700px;
            margin: 0 auto;
            padding: 20px;
            background-color: #f5f5f5;
        }}
        .container {{
            background-color: white;
            border-radius: 12px;
            padding: 30px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }}
        h1 {{
            color: #1DB954;
            margin-bottom: 10px;
            font-size: 28px;
        }}
        .subtitle {{
            color: #666;
            margin-bottom: 30px;
            font-size: 14px;
        }}
        .stats {{
            background-color: #f8f9fa;
            border-radius: 8px;
            padding: 15px;
            margin-bottom: 25px;
            font-size: 13px;
            color: #666;
        }}
        .track {{
            border-bottom: 1px solid #eee;
            padding: 15px 0;
        }}
        .track:last-child {{
            border-bottom: none;
        }}
        .track-number {{
            display: inline-block;
            width: 30px;
            height: 30px;
            background: linear-gradient(135deg, #1DB954, #1ed760);
            color: white;
            border-radius: 50%;
            text-align: center;
            line-height: 30px;
            font-weight: bold;
            font-size: 14px;
            margin-right: 15px;
        }}
        .track-info {{
            display: inline-block;
            vertical-align: top;
            width: calc(100% - 60px);
        }}
        .track-title {{
            font-weight: 600;
            font-size: 16px;
            color: #222;
        }}
        .track-artist {{
            color: #666;
            font-size: 14px;
        }}
        .track-album {{
            color: #999;
            font-size: 12px;
            margin-top: 2px;
        }}
        .track-meta {{
            margin-top: 8px;
            font-size: 12px;
        }}
        .track-genres {{
            color: #888;
            font-style: italic;
        }}
        .links {{
            margin-top: 8px;
        }}
        .links a {{
            display: inline-block;
            padding: 6px 12px;
            margin-right: 8px;
            border-radius: 20px;
            text-decoration: none;
            font-size: 12px;
            font-weight: 500;
        }}
        .spotify-link {{
            background-color: #1DB954;
            color: white;
        }}
        .spotify-link:hover {{
            background-color: #1ed760;
        }}
        .youtube-link {{
            background-color: #FF0000;
            color: white;
        }}
        .youtube-link:hover {{
            background-color: #cc0000;
        }}
        .score {{
            color: #999;
            font-size: 11px;
            margin-top: 5px;
        }}
        .footer {{
            margin-top: 30px;
            padding-top: 20px;
            border-top: 1px solid #eee;
            font-size: 12px;
            color: #999;
            text-align: center;
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>🎵 Your Monthly Music Picks</h1>
        <p class="subtitle">{current_date} • Based on your music library</p>
        
        <div class="stats">
            📁 Analyzed <strong>{stats.get('total_files', 0)}</strong> files • 
            🎵 Found <strong>{stats.get('tracks_parsed', 0)}</strong> tracks • 
            🎯 Generated <strong>{len(recommendations)}</strong> recommendations
        </div>
"""
    
    for i, rec in enumerate(recommendations, 1):
        genres_str = ", ".join(rec.genres[:3]) if rec.genres else "—"
        
        html += f"""
        <div class="track">
            <span class="track-number">{i}</span>
            <div class="track-info">
                <div class="track-title">{_escape_html(rec.title)}</div>
                <div class="track-artist">{_escape_html(rec.artist)}</div>
                <div class="track-album">📀 {_escape_html(rec.album)}</div>
                <div class="track-meta">
                    <span class="track-genres">🏷️ {genres_str}</span>
                </div>
                <div class="links">
"""
        
        if rec.spotify_url:
            html += f'                    <a href="{rec.spotify_url}" class="spotify-link">▶ Spotify</a>\n'
            
        html += f'                    <a href="{rec.youtube_url}" class="youtube-link">▶ YouTube</a>\n'
        
        html += f"""
                </div>
                <div class="score">Match score: {rec.score:.2f} • Popularity: {rec.popularity}</div>
            </div>
        </div>
"""
    
    html += """
        <div class="footer">
            Generated by Music Recommender 🎧<br>
            Based on Spotify recommendations & your taste profile
        </div>
    </div>
</body>
</html>
"""
    
    return html


def _escape_html(text: str) -> str:
    """Escape HTML special characters."""
    return (text
        .replace('&', '&amp;')
        .replace('<', '&lt;')
        .replace('>', '&gt;')
        .replace('"', '&quot;')
        .replace("'", '&#39;'))


def generate_plain_text_email(recommendations: List[Recommendation], stats: dict) -> str:
    """Generate plain text version of the email."""
    current_date = datetime.now().strftime("%B %Y")
    
    text = f"""
🎵 YOUR MONTHLY MUSIC PICKS - {current_date}
{'=' * 50}

Library Stats:
• Files analyzed: {stats.get('total_files', 0)}
• Tracks parsed: {stats.get('tracks_parsed', 0)}
• Recommendations: {len(recommendations)}

{'=' * 50}
TOP RECOMMENDATIONS
{'=' * 50}

"""
    
    for i, rec in enumerate(recommendations, 1):
        genres_str = ", ".join(rec.genres[:3]) if rec.genres else "N/A"
        
        text += f"""
{i}. {rec.title}
   Artist: {rec.artist}
   Album: {rec.album}
   Genres: {genres_str}
   Popularity: {rec.popularity} | Score: {rec.score:.2f}
"""
        if rec.spotify_url:
            text += f"   Spotify: {rec.spotify_url}\n"
        text += f"   YouTube: {rec.youtube_url}\n"
        text += "-" * 40 + "\n"
    
    text += """
---
Generated by Music Recommender
"""
    
    return text


def send_email(
    recommendations: List[Recommendation],
    stats: dict,
    sender: str,
    to: str,
    smtp_user: str,
    smtp_password: str,
    smtp_server: str = "smtp.gmail.com",
    smtp_port: int = 587
) -> bool:
    """
    Send recommendations email via Gmail SMTP.
    
    Args:
        recommendations: List of Recommendation objects
        stats: Dictionary with statistics
        sender: Sender email address
        to: Recipient email address
        smtp_user: SMTP username
        smtp_password: SMTP password (App Password for Gmail)
        smtp_server: SMTP server hostname
        smtp_port: SMTP server port
        
    Returns:
        True if successful, False otherwise
    """
    try:
        current_date = datetime.now().strftime("%B %Y")
        subject = f"🎵 Your Monthly Music Recommendations - {current_date}"
        
        # Create message
        msg = MIMEMultipart('alternative')
        msg['Subject'] = subject
        msg['From'] = sender
        msg['To'] = to
        
        # Generate content
        text_content = generate_plain_text_email(recommendations, stats)
        html_content = generate_html_email(recommendations, stats)
        
        # Attach parts (plain text first, then HTML)
        part1 = MIMEText(text_content, 'plain', 'utf-8')
        part2 = MIMEText(html_content, 'html', 'utf-8')
        
        msg.attach(part1)
        msg.attach(part2)
        
        # Send email
        logger.info(f"Connecting to SMTP server {smtp_server}:{smtp_port}")
        
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(smtp_user, smtp_password)
            server.sendmail(sender, [to], msg.as_string())
            
        logger.info(f"Email sent successfully to {to}")
        return True
        
    except smtplib.SMTPAuthenticationError as e:
        logger.error(f"SMTP authentication failed: {e}")
        logger.error("Make sure you're using an App Password, not your regular password")
        return False
        
    except Exception as e:
        logger.error(f"Failed to send email: {e}")
        return False