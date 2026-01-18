import os
import json
import logging
from datetime import datetime
from telethon import TelegramClient
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
api_id = os.getenv('TG_API_ID')
api_hash = os.getenv('TG_API_HASH')

# List of channels to scrape as per project document
channels = [
    'CheMed123',         # CheMed
    'lobelia4cosmetics',  # Lobelia Cosmetics
    'tikvahpharma',      # Tikvah Pharma
    # You can add more channel usernames here from et.tgstat.com/medicine
]

# Set up logging
logging.basicConfig(
    filename='logs/scraping.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

async def scrape_channel(client, channel_username):
    entity = await client.get_entity(channel_username)
    channel_name = entity.title
    
    # Create directory for images: data/raw/images/{channel_name}/
    image_dir = f'data/raw/images/{channel_username}'
    os.makedirs(image_dir, exist_ok=True)
    
    messages_data = []
    
    async for message in client.iter_messages(entity, limit=100):
        media_path = None
        if message.photo:
            # Task 1 Requirement: Store images with message_id name
            filename = f"{message.id}.jpg"
            media_path = await message.download_media(file=os.path.join(image_dir, filename))
        
        # Collect data fields required by Task 1
        data = {
            'message_id': message.id,
            'channel_name': channel_name,
            'channel_username': channel_username,
            'message_date': message.date.isoformat(),
            'message_text': message.text,
            'has_media': message.photo is not None,
            'image_path': media_path,
            'views': message.views,
            'forwards': message.forwards
        }
        messages_data.append(data)

    # Task 1 Requirement: Store raw data as JSON in partitioned structure
    # data/raw/telegram_messages/YYYY-MM-DD/channel_name.json
    date_str = datetime.now().strftime('%Y-%m-%d')
    json_dir = f'data/raw/telegram_messages/{date_str}'
    os.makedirs(json_dir, exist_ok=True)
    
    with open(f'{json_dir}/{channel_username}.json', 'w') as f:
        json.dump(messages_data, f, indent=4)
    
    logging.info(f"Successfully scraped {len(messages_data)} messages from {channel_username}")

async def main():
    async with TelegramClient('session_name', api_id, api_hash) as client:
        for channel in channels:
            try:
                print(f"Scraping {channel}...")
                await scrape_channel(client, channel)
            except Exception as e:
                logging.error(f"Error scraping {channel}: {e}")
                print(f"Error scraping {channel}: {e}")

if __name__ == '__main__':
    import asyncio
    asyncio.run(main())