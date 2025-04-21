from botasaurus_server.server import Server
from src.scrape_heading_task import scrape_heading_task

# Add the scraper to the server
Server.add_scraper(scrape_heading_task)

# Connect to PostgreSQL server
Server.set_database_url('postgresql://postgres.updemyybuhoicisjksiu:Dexter_#254@aws-0-eu-central-1.pooler.supabase.com:6543/postgres')